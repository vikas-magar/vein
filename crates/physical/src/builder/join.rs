//! Physical stage builder for partitioned hash joins.

use std::sync::Arc;

use common::identifiers::{PartitionId, QueryId, StageId, TaskId};
use common::protocol::{
    DataSink, DataSource, ExchangeInput, FieldSchema, JoinKeyMode, JoinKeySpec, JoinSource,
    MicroProgram, PhysicalJoinType, ShuffleTarget,
};
use common::Result;
use error::VeinError;
use parser::ast::expr::BinaryOp;
use parser::ast::pipe_op::JoinType;
use plan::{LogicalExpr, LogicalPlan, PlanTraversal, ResolvedColumn, VeinSchema};

use crate::builder::pipeline::{DefaultPipelineStageBuilder, PipelineStageBuilder};
use crate::compiler::BytecodeCompiler;
use crate::distribution::{Distribution, HashPartitioning};
use crate::job::{StageEdge, StageInputPort};
use crate::stage::Stage;
use crate::task::Task;

/// Number of exchange partitions used by the default join stage builder.
///
/// The reducer stage creates one partition-local task per exchange partition.
pub const DEFAULT_JOIN_PARTITIONS: u16 = 32;

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

        let left_keys: Vec<JoinKeySpec> = key_pairs
            .iter()
            .map(|(left, _)| key_spec(left))
            .collect();
        let right_keys: Vec<JoinKeySpec> = key_pairs
            .iter()
            .map(|(_, right)| key_spec(right))
            .collect();

        let (left_source, left_program, left_schema) =
            self.pipeline_builder.compile_pipeline(left)?;
        let (right_source, right_program, right_schema) =
            self.pipeline_builder.compile_pipeline(right)?;

        let left_stage_id = StageId::new();
        let right_stage_id = StageId::new();
        let join_stage_id = StageId::new();
        let partition_count = DEFAULT_JOIN_PARTITIONS;
        let left_task_id = TaskId::new();
        let right_task_id = TaskId::new();
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

        let left_task = Task::new(
            left_task_id,
            PartitionId::new(),
            left_source,
            left_program,
            DataSink::Shuffle(ShuffleTarget {
                exchange_key: left_exchange_key,
                vfs_path_prefix: left_path.clone(),
                producer_task_id: left_task_id,
                attempt: 0,
                expected_tasks: vec![left_task_id],
                schema_fingerprint: schema_fingerprint(&left_schema_fields),
                num_partitions: partition_count,
                partition_keys: left_keys.clone(),
                schema: left_schema_fields.clone(),
            }),
        );
        let right_task = Task::new(
            right_task_id,
            PartitionId::new(),
            right_source,
            right_program,
            DataSink::Shuffle(ShuffleTarget {
                exchange_key: right_exchange_key,
                vfs_path_prefix: right_path.clone(),
                producer_task_id: right_task_id,
                attempt: 0,
                expected_tasks: vec![right_task_id],
                schema_fingerprint: schema_fingerprint(&right_schema_fields),
                num_partitions: partition_count,
                partition_keys: right_keys.clone(),
                schema: right_schema_fields.clone(),
            }),
        );

        stages.push(Stage::new(left_stage_id).with_task(left_task));
        stages.push(Stage::new(right_stage_id).with_task(right_task));

        let left_distribution =
            Distribution::Hash(HashPartitioning::new(left_keys.clone(), partition_count));
        let right_distribution =
            Distribution::Hash(HashPartitioning::new(right_keys.clone(), partition_count));

        let (join_program, output_schema) =
            compile_post_join_program(plan, join_schema)?;

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
                requires_exact_verification: true,
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
        _ => Err(VeinError::planning(
            "unsupported operator above join",
            None,
        )),
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

fn key_spec(column: &ResolvedColumn) -> JoinKeySpec {
    JoinKeySpec {
        field_id: column.id.0 as i32,
        column_name: column.name.clone(),
        mode: JoinKeyMode::Auto,
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

            let left_is_left = left_schema.fields.iter().any(|field| field.id == left_column.id);
            let right_is_right = right_schema.fields.iter().any(|field| field.id == right_column.id);
            let left_is_right = right_schema.fields.iter().any(|field| field.id == left_column.id);
            let right_is_left = left_schema.fields.iter().any(|field| field.id == right_column.id);

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
