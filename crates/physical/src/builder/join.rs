//! Physical stage builder for partitioned hash joins.

use std::sync::Arc;

use common::Result;
use common::identifiers::{PartitionId, QueryId, StageId, TaskId};
use common::protocol::{
    DataSink, DataSource, ExchangeInput, FieldSchema, JoinKeyMode, JoinKeySpec, JoinSource,
    MicroProgram, PhysicalJoinType, ShuffleTarget,
};
use error::VeinError;
use parser::ast::expr::BinaryOp;
use parser::ast::pipe_op::JoinType;
use plan::{
    LogicalExpr, LogicalPlan, PlanTraversal, ResolvedColumn, TableFormat, VeinDataType, VeinSchema,
};

use crate::builder::pipeline::{
    DefaultPipelineStageBuilder, PipelineStageBuilder, split_file_scan_source,
};
use crate::compiler::BytecodeCompiler;
use crate::distribution::{Distribution, HashPartitioning};
use crate::job::{StageEdge, StageInputPort};
use crate::stage::Stage;
use crate::task::Task;

/// Maximum exchange partitions used when catalog statistics call for more parallelism.
pub const MAX_JOIN_PARTITIONS: u16 = 32;
/// Conservative fanout when the catalog has no usable size statistics.
pub const DEFAULT_JOIN_PARTITIONS: u16 = 4;

const TARGET_ROWS_PER_PARTITION: u64 = 250_000;
const TARGET_BYTES_PER_PARTITION: u64 = 128 * 1024 * 1024;

/// Builds the physical stages for a logical join.
pub trait JoinStageBuilder: Send + Sync {
    /// Lowers a logical join into two repartition producers and one join stage.
    fn build(
        &self,
        query_id: QueryId,
        plan: &LogicalPlan,
        stages: &mut Vec<Stage>,
        edges: &mut Vec<StageEdge>,
    ) -> Result<StageId>;
}

/// Default partitioned hash-join stage builder.
#[derive(Clone)]
pub struct DefaultJoinStageBuilder {
    pipeline_builder: Arc<dyn PipelineStageBuilder>,
}

impl DefaultJoinStageBuilder {
    /// Creates a builder with the default linear child-pipeline compiler.
    pub fn new() -> Self {
        Self {
            pipeline_builder: Arc::new(DefaultPipelineStageBuilder::new()),
        }
    }

    /// Overrides the child pipeline builder.
    pub fn with_pipeline_builder(mut self, builder: Arc<dyn PipelineStageBuilder>) -> Self {
        self.pipeline_builder = builder;
        self
    }
}

impl Default for DefaultJoinStageBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl JoinStageBuilder for DefaultJoinStageBuilder {
    fn build(
        &self,
        query_id: QueryId,
        plan: &LogicalPlan,
        stages: &mut Vec<Stage>,
        edges: &mut Vec<StageEdge>,
    ) -> Result<StageId> {
        let join_plan = find_join_plan(plan).ok_or_else(|| {
            VeinError::planning(
                "join stage builder requires a join with only linear post-join operators",
                None,
            )
        })?;

        let LogicalPlan::Join {
            join_type,
            left,
            right,
            on,
            schema: join_schema,
            ..
        } = join_plan
        else {
            return Err(VeinError::planning(
                "join stage builder received a non-join plan",
                None,
            ));
        };

        if !matches!(join_type, JoinType::Inner) {
            return Err(VeinError::planning(
                "only INNER JOIN is currently supported by the physical join builder",
                None,
            ));
        }

        let key_pairs = extract_equi_keys(
            on.as_ref().ok_or_else(|| {
                VeinError::planning("INNER JOIN requires an equality predicate", None)
            })?,
            left.schema(),
            right.schema(),
        )?;

        if key_pairs.is_empty() {
            return Err(VeinError::planning(
                "INNER JOIN requires at least one equality key",
                None,
            ));
        }

        let address_native = key_pairs.iter().all(|(left_column, right_column)| {
            left_column.data_type == right_column.data_type
                && supports_address_key(left_column, left.schema())
                && supports_address_key(right_column, right.schema())
                && address_key_available(left.as_ref(), left_column)
                && address_key_available(right.as_ref(), right_column)
        });
        let key_mode = if address_native {
            JoinKeyMode::AddressNative
        } else {
            JoinKeyMode::ValueNative
        };
        let left_keys: Vec<JoinKeySpec> = key_pairs
            .iter()
            .map(|(left, _)| key_spec(left, key_mode))
            .collect();
        let right_keys: Vec<JoinKeySpec> = key_pairs
            .iter()
            .map(|(_, right)| key_spec(right, key_mode))
            .collect();

        let (left_source, left_program, left_schema) =
            self.pipeline_builder.compile_pipeline(left)?;
        let (right_source, right_program, right_schema) =
            self.pipeline_builder.compile_pipeline(right)?;

        let left_stage_id = StageId::new();
        let right_stage_id = StageId::new();
        let join_stage_id = StageId::new();
        let partition_count = choose_join_partition_count(left, right);
        tracing::debug!(
            target: "vein.optimizer",
            event = "join_partition_count_selected",
            partition_count,
            "selected adaptive join partition count"
        );
        let left_sources = split_file_scan_source(left_source)?;
        let right_sources = split_file_scan_source(right_source)?;
        let left_task_ids: Vec<_> = left_sources.iter().map(|_| TaskId::new()).collect();
        let right_task_ids: Vec<_> = right_sources.iter().map(|_| TaskId::new()).collect();
        let left_exchange_key = common::protocol::ExchangeKey {
            query_id,
            stage_id: left_stage_id,
        };
        let right_exchange_key = common::protocol::ExchangeKey {
            query_id,
            stage_id: right_stage_id,
        };
        let left_schema_fields = schema_fields(left_schema.clone());
        let right_schema_fields = schema_fields(right_schema.clone());
        let left_path = format!("shuffle/{query_id}/{join_stage_id}/left");
        let right_path = format!("shuffle/{query_id}/{join_stage_id}/right");

        let mut left_stage = Stage::new(left_stage_id);
        for ((partition, source), task_id) in left_sources.into_iter().zip(&left_task_ids) {
            left_stage.tasks.push(Task::new(
                *task_id,
                partition,
                source,
                left_program.clone(),
                DataSink::Shuffle(ShuffleTarget {
                    exchange_key: left_exchange_key,
                    vfs_path_prefix: left_path.clone(),
                    producer_task_id: *task_id,
                    attempt: 0,
                    expected_tasks: left_task_ids.clone(),
                    schema_fingerprint: schema_fingerprint(&left_schema_fields),
                    num_partitions: partition_count,
                    partition_keys: left_keys.clone(),
                    schema: left_schema_fields.clone(),
                }),
            ));
        }
        let mut right_stage = Stage::new(right_stage_id);
        for ((partition, source), task_id) in right_sources.into_iter().zip(&right_task_ids) {
            right_stage.tasks.push(Task::new(
                *task_id,
                partition,
                source,
                right_program.clone(),
                DataSink::Shuffle(ShuffleTarget {
                    exchange_key: right_exchange_key,
                    vfs_path_prefix: right_path.clone(),
                    producer_task_id: *task_id,
                    attempt: 0,
                    expected_tasks: right_task_ids.clone(),
                    schema_fingerprint: schema_fingerprint(&right_schema_fields),
                    num_partitions: partition_count,
                    partition_keys: right_keys.clone(),
                    schema: right_schema_fields.clone(),
                }),
            ));
        }

        stages.push(left_stage);
        stages.push(right_stage);

        let left_distribution =
            Distribution::Hash(HashPartitioning::new(left_keys.clone(), partition_count));
        let right_distribution =
            Distribution::Hash(HashPartitioning::new(right_keys.clone(), partition_count));

        let (join_program, output_schema) = compile_post_join_program(plan, join_schema)?;

        let output_schema = output_schema
            .fields
            .iter()
            .map(|field| FieldSchema {
                field_id: field.id.0 as i32,
                name: field.name.clone(),
                data_type: format!("{:?}", field.data_type),
            })
            .collect::<Vec<_>>();

        let mut join_stage = Stage::new(join_stage_id);
        for partition_id in 0..partition_count {
            let join_source = DataSource::Join(JoinSource {
                left: ExchangeInput {
                    exchange_key: left_exchange_key,
                    upstream_stage_id: left_stage_id,
                    path_prefix: left_path.clone(),
                    partition_id: partition_id as u32,
                    partition_count,
                },
                right: ExchangeInput {
                    exchange_key: right_exchange_key,
                    upstream_stage_id: right_stage_id,
                    path_prefix: right_path.clone(),
                    partition_id: partition_id as u32,
                    partition_count,
                },
                join_type: PhysicalJoinType::Inner,
                left_keys: left_keys.clone(),
                right_keys: right_keys.clone(),
                left_schema: left_schema_fields.clone(),
                right_schema: right_schema_fields.clone(),
            });
            join_stage = join_stage.with_task(Task::new(
                TaskId::new(),
                PartitionId::new(),
                join_source,
                join_program.clone(),
                DataSink::Client {
                    schema: output_schema.clone(),
                },
            ));
        }
        stages.push(join_stage);

        edges.push(StageEdge::routed(
            left_stage_id,
            join_stage_id,
            StageInputPort::Left,
            left_distribution,
        ));
        edges.push(StageEdge::routed(
            right_stage_id,
            join_stage_id,
            StageInputPort::Right,
            right_distribution,
        ));

        Ok(join_stage_id)
    }
}

fn choose_join_partition_count(left: &LogicalPlan, right: &LogicalPlan) -> u16 {
    let Some((max_rows, max_bytes)) = join_statistics(left, right) else {
        return DEFAULT_JOIN_PARTITIONS;
    };

    let row_partitions = ceil_div(max_rows, TARGET_ROWS_PER_PARTITION);
    let byte_partitions = ceil_div(max_bytes, TARGET_BYTES_PER_PARTITION);
    row_partitions
        .max(byte_partitions)
        .clamp(1, MAX_JOIN_PARTITIONS as u64) as u16
}

fn join_statistics(left: &LogicalPlan, right: &LogicalPlan) -> Option<(u64, u64)> {
    let left = source_statistics(left)?;
    let right = source_statistics(right)?;
    Some((left.0.max(right.0), left.1.max(right.1)))
}

fn source_statistics(plan: &LogicalPlan) -> Option<(u64, u64)> {
    match plan {
        LogicalPlan::Scan { table, .. } => {
            let stats = table.statistics.as_ref()?;
            match (stats.num_rows, stats.total_bytes) {
                (Some(rows), Some(bytes)) => Some((rows, bytes)),
                (Some(rows), None) => Some((rows, 0)),
                (None, Some(bytes)) => Some((0, bytes)),
                (None, None) => None,
            }
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input, .. }
        | LogicalPlan::SubqueryAlias { input, .. } => source_statistics(input),
        _ => None,
    }
}

fn ceil_div(value: u64, divisor: u64) -> u64 {
    value / divisor + u64::from(value % divisor != 0)
}

fn find_join_plan(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan {
        LogicalPlan::Join { .. } => Some(plan),
        LogicalPlan::Project { input, .. } | LogicalPlan::Filter { input, .. } => {
            find_join_plan(input)
        }
        _ => None,
    }
}

fn compile_post_join_program(
    plan: &LogicalPlan,
    join_schema: &VeinSchema,
) -> Result<(MicroProgram, VeinSchema)> {
    if join_schema.fields.len() > u8::MAX as usize {
        return Err(VeinError::planning(
            "join output exceeds the VM register capacity",
            None,
        ));
    }

    let mut compiler = BytecodeCompiler::new(join_schema.fields.len() as u8);
    let output_schema = compile_post_join_node(plan, join_schema, &mut compiler)?;
    Ok((compiler.finish(), output_schema))
}

fn compile_post_join_node(
    plan: &LogicalPlan,
    join_schema: &VeinSchema,
    compiler: &mut BytecodeCompiler,
) -> Result<VeinSchema> {
    match plan {
        LogicalPlan::Join { schema, .. } => Ok((**schema).clone()),
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => {
            let input_schema = compile_post_join_node(input, join_schema, compiler)?;
            compiler.compile_projection(exprs, &input_schema)?;
            Ok((**schema).clone())
        }
        LogicalPlan::Filter { input, predicate } => {
            let input_schema = compile_post_join_node(input, join_schema, compiler)?;
            compiler.compile_filter(predicate, &input_schema)?;
            Ok(input_schema)
        }
        _ => Err(VeinError::planning("unsupported operator above join", None)),
    }
}

fn schema_fields(schema: VeinSchema) -> Vec<FieldSchema> {
    schema
        .fields
        .into_iter()
        .map(|field| FieldSchema {
            field_id: field.id.0 as i32,
            name: field.name,
            data_type: format!("{:?}", field.data_type),
        })
        .collect()
}

fn schema_fingerprint(schema: &[FieldSchema]) -> String {
    schema
        .iter()
        .map(|field| format!("{}:{}:{}", field.field_id, field.name, field.data_type))
        .collect::<Vec<_>>()
        .join("|")
}

fn key_spec(column: &ResolvedColumn, mode: JoinKeyMode) -> JoinKeySpec {
    JoinKeySpec {
        field_id: column.id.0 as i32,
        column_name: column.name.clone(),
        mode,
    }
}

fn supports_address_key(column: &ResolvedColumn, schema: &VeinSchema) -> bool {
    schema.fields.iter().any(|field| field.id == column.id)
        && matches!(
            column.data_type,
            VeinDataType::Boolean
                | VeinDataType::Int8
                | VeinDataType::Int16
                | VeinDataType::Int32
                | VeinDataType::Int64
                | VeinDataType::UInt8
                | VeinDataType::UInt16
                | VeinDataType::UInt32
                | VeinDataType::UInt64
                | VeinDataType::Float32
                | VeinDataType::Float64
                | VeinDataType::Utf8
                | VeinDataType::LargeUtf8
                | VeinDataType::StringView
                | VeinDataType::Binary
                | VeinDataType::LargeBinary
                | VeinDataType::BinaryView
                | VeinDataType::Date32
                | VeinDataType::Timestamp(_)
        )
}

fn address_key_available(plan: &LogicalPlan, column: &ResolvedColumn) -> bool {
    match plan {
        LogicalPlan::Scan { table, schema, .. } => {
            matches!(table.format.as_ref(), Some(TableFormat::Exo))
                && supports_address_key(column, schema)
        }
        LogicalPlan::Filter { input, .. } | LogicalPlan::Write { input, .. } => {
            address_key_available(input, column)
        }
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => {
            let Some(expression) = schema
                .fields
                .iter()
                .zip(exprs.iter())
                .find_map(|(field, expression)| (field.id == column.id).then_some(expression))
            else {
                return false;
            };
            let LogicalExpr::Column(source_column) = expression else {
                return false;
            };
            address_key_available(input, source_column)
        }
        LogicalPlan::SubqueryAlias { input, .. } => address_key_available(input, column),
        _ => false,
    }
}

fn extract_equi_keys(
    predicate: &LogicalExpr,
    left_schema: &VeinSchema,
    right_schema: &VeinSchema,
) -> Result<Vec<(ResolvedColumn, ResolvedColumn)>> {
    let mut keys = Vec::new();
    collect_equi_keys(predicate, left_schema, right_schema, &mut keys)?;
    Ok(keys)
}

fn collect_equi_keys(
    predicate: &LogicalExpr,
    left_schema: &VeinSchema,
    right_schema: &VeinSchema,
    output: &mut Vec<(ResolvedColumn, ResolvedColumn)>,
) -> Result<()> {
    match predicate {
        LogicalExpr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
            ..
        } => {
            collect_equi_keys(left, left_schema, right_schema, output)?;
            collect_equi_keys(right, left_schema, right_schema, output)
        }
        LogicalExpr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
            ..
        } => {
            let (LogicalExpr::Column(left_column), LogicalExpr::Column(right_column)) =
                (left.as_ref(), right.as_ref())
            else {
                return Err(VeinError::planning(
                    "join equality must compare two columns",
                    None,
                ));
            };

            let left_is_left = left_schema
                .fields
                .iter()
                .any(|field| field.id == left_column.id);
            let right_is_right = right_schema
                .fields
                .iter()
                .any(|field| field.id == right_column.id);
            let left_is_right = right_schema
                .fields
                .iter()
                .any(|field| field.id == left_column.id);
            let right_is_left = left_schema
                .fields
                .iter()
                .any(|field| field.id == right_column.id);

            if left_is_left && right_is_right {
                output.push((left_column.clone(), right_column.clone()));
            } else if left_is_right && right_is_left {
                output.push((right_column.clone(), left_column.clone()));
            } else {
                return Err(VeinError::planning(
                    "join equality must reference one column from each input",
                    None,
                ));
            }
            Ok(())
        }
        _ => Err(VeinError::planning(
            "physical join currently supports only conjunctive equality predicates",
            None,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plan::{ColumnId, TableMetadata, TableStatistics};
    use std::sync::Arc;

    fn scan(column: ResolvedColumn, format: Option<TableFormat>) -> LogicalPlan {
        let schema = VeinSchema {
            fields: vec![column.clone()],
        };
        let mut table = TableMetadata::new("test", Arc::new(schema.clone()));
        table.format = format;
        LogicalPlan::Scan {
            table: Arc::new(table),
            projection: None,
            filters: vec![],
            schema: Box::new(schema),
        }
    }

    fn column(id: usize, data_type: VeinDataType) -> ResolvedColumn {
        ResolvedColumn::new(ColumnId(id), format!("c{id}"), None, data_type)
    }

    #[test]
    fn chooses_one_partition_for_small_catalog_statistics() {
        let stats = TableStatistics {
            num_rows: Some(1_000),
            total_bytes: Some(64 * 1024),
            ..Default::default()
        };
        let mut left = scan(column(1, VeinDataType::Int64), Some(TableFormat::Exo));
        let mut right = scan(column(2, VeinDataType::Int64), Some(TableFormat::Exo));
        if let LogicalPlan::Scan { table, .. } = &mut left {
            Arc::make_mut(table).statistics = Some(stats.clone());
        }
        if let LogicalPlan::Scan { table, .. } = &mut right {
            Arc::make_mut(table).statistics = Some(stats);
        }

        assert_eq!(choose_join_partition_count(&left, &right), 1);
    }

    #[test]
    fn uses_conservative_fanout_without_statistics() {
        let left = scan(column(1, VeinDataType::Int64), Some(TableFormat::Exo));
        let right = scan(column(2, VeinDataType::Int64), Some(TableFormat::Exo));

        assert_eq!(
            choose_join_partition_count(&left, &right),
            DEFAULT_JOIN_PARTITIONS
        );
    }

    #[test]
    fn uses_conservative_fanout_when_catalog_statistics_are_empty() {
        let mut left = scan(column(1, VeinDataType::Int64), Some(TableFormat::Exo));
        let mut right = scan(column(2, VeinDataType::Int64), Some(TableFormat::Exo));
        if let LogicalPlan::Scan { table, .. } = &mut left {
            Arc::make_mut(table).statistics = Some(TableStatistics::default());
        }
        if let LogicalPlan::Scan { table, .. } = &mut right {
            Arc::make_mut(table).statistics = Some(TableStatistics::default());
        }

        assert_eq!(
            choose_join_partition_count(&left, &right),
            DEFAULT_JOIN_PARTITIONS
        );
    }

    #[test]
    fn caps_large_statistics_at_maximum_partition_limit() {
        let stats = TableStatistics {
            num_rows: Some(u64::MAX),
            total_bytes: Some(u64::MAX),
            ..Default::default()
        };
        let mut left = scan(column(1, VeinDataType::Int64), Some(TableFormat::Exo));
        let mut right = scan(column(2, VeinDataType::Int64), Some(TableFormat::Exo));
        if let LogicalPlan::Scan { table, .. } = &mut left {
            Arc::make_mut(table).statistics = Some(stats.clone());
        }
        if let LogicalPlan::Scan { table, .. } = &mut right {
            Arc::make_mut(table).statistics = Some(stats);
        }

        assert_eq!(
            choose_join_partition_count(&left, &right),
            MAX_JOIN_PARTITIONS
        );
    }

    #[test]
    fn selects_address_native_only_for_exo_keys() {
        let left_column = column(1, VeinDataType::Int64);
        let right_column = column(2, VeinDataType::Int64);
        let left = scan(left_column.clone(), Some(TableFormat::Exo));
        let right = scan(right_column.clone(), Some(TableFormat::Exo));

        assert!(address_key_available(&left, &left_column));
        assert!(address_key_available(&right, &right_column));
        assert_eq!(
            key_spec(&left_column, JoinKeyMode::AddressNative).mode,
            JoinKeyMode::AddressNative
        );
    }

    #[test]
    fn falls_back_to_value_native_when_one_side_lacks_addresses() {
        let left_column = column(1, VeinDataType::Int64);
        let right_column = column(2, VeinDataType::Int64);
        let left = scan(left_column.clone(), Some(TableFormat::Exo));
        let right = scan(right_column.clone(), Some(TableFormat::Parquet));

        assert!(address_key_available(&left, &left_column));
        assert!(!address_key_available(&right, &right_column));
        assert_eq!(
            key_spec(&right_column, JoinKeyMode::ValueNative).mode,
            JoinKeyMode::ValueNative
        );
    }
}
