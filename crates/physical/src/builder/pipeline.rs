//! Linear pipeline builder abstraction: translates unbroken operator sequences into a single [`Stage`].

use crate::compiler::PipelineCompiler;
use crate::stage::Stage;
use crate::task::Task;
use common::Result;
use common::identifiers::{PartitionId, StageId, TaskId};
use common::protocol::{
    DataSink, DataSource, JoinKind, JoinOutput, JoinPlan, LimitSpec, MicroProgram,
    PhysicalInputPlan, SortPlan,
};
use error::VeinError;
use parser::ast::expr::BinaryOp;
use parser::ast::pipe_op::JoinType;
use plan::{LogicalExpr, LogicalPlan};
use plan::schema::VeinSchema;
use std::sync::Arc;

/// Trait for translating linear, non-blocking logical plans into a single physical [`Stage`].
pub trait PipelineStageBuilder: Send + Sync {
    /// Translates a linear logical plan into a single physical stage with `DataSink::Client`.
    fn build(
        &self,
        plan: &LogicalPlan,
    ) -> Result<Stage>;

    /// Compiles the linear pipeline into a leaf data source, a [`MicroProgram`], and an output schema.
    fn compile_pipeline(
        &self,
        plan: &LogicalPlan,
    ) -> Result<(DataSource, MicroProgram, VeinSchema)>;
}

/// Default linear pipeline stage builder.
#[derive(Debug, Default, Clone)]
pub struct DefaultPipelineStageBuilder;

impl DefaultPipelineStageBuilder {
    /// Creates a new default linear pipeline stage builder.
    pub fn new() -> Self {
        Self
    }

    /// Builds the two-input stage a join needs.
    ///
    /// Join inputs remain a recursive stream plan inside one task. The runtime
    /// builds each join's right input before probing its left, then forwards
    /// matched batches without materializing the full joined relation.
    fn build_join(
        &self,
        plan: &LogicalPlan,
        limit: Option<LimitSpec>,
        sort: Option<SortPlan>,
        sort_key_program: Option<Arc<MicroProgram>>,
    ) -> Result<Stage> {
        let (input_plan, schema) = self.compile_join_input_or_leaf(plan)?;

        let mut stage = Stage::new(StageId::new());
        stage.tasks.push(
            Task::new(
                TaskId::new(),
                PartitionId::from_raw(0),
                DataSource::Memory,
                Arc::new(MicroProgram::empty()),
                Arc::new(DataSink::Client {
                    schema: Arc::new(schema.clone()),
                }),
            )
                .with_input_plan(Arc::new(input_plan))
                .with_limit(limit)
                .with_sort(sort)
                .with_sort_key_program(sort_key_program, Some(schema.fields.len() as u32)),
        );
        Ok(stage)
    }

    fn compile_join_input(&self, plan: &LogicalPlan) -> Result<(PhysicalInputPlan, VeinSchema)> {
        let LogicalPlan::Join {
            join_type,
            left,
            right,
            on,
            using,
            schema,
            ..
        } = plan
        else {
            return self.compile_join_input_or_leaf(plan);
        };

        let (probe, probe_schema) = self.compile_join_input_or_leaf(left)?;
        let (build, build_schema) = self.compile_join_input_or_leaf(right)?;

        let mut probe_keys = Vec::new();
        let mut build_keys = Vec::new();
        if using.is_empty() {
            let (probe_key, build_key) = equality_keys(on.as_ref(), &probe_schema, &build_schema)?;
            probe_keys.push(probe_key);
            build_keys.push(build_key);
        } else {
            for column in using {
                probe_keys.push(position(&probe_schema, &column.name)?);
                build_keys.push(position(&build_schema, &column.name)?);
            }
        }

        let (outputs, output_schema) = available_join_outputs(schema, &probe_schema, &build_schema);
        if output_schema.fields.len() != schema.fields.len() {
            let unavailable = schema
                .fields
                .iter()
                .filter(|field| {
                    output_schema
                        .position_of_id(field.id)
                        .is_none()
                })
                .map(|field| format!("{}#{}", field.name, field.id.0))
                .collect::<Vec<_>>();
            tracing::debug!(
                target: "vein.physical",
                event = "physical_join_outputs_pruned",
                declared_output_count = schema.fields.len(),
                available_output_count = output_schema.fields.len(),
                unavailable_outputs = ?unavailable,
                probe_columns = ?probe_schema.fields.iter().map(|field| (&field.name, field.id.0)).collect::<Vec<_>>(),
                build_columns = ?build_schema.fields.iter().map(|field| (&field.name, field.id.0)).collect::<Vec<_>>(),
                "physical_join_outputs_pruned"
            );
        }
        let join = JoinPlan {
            join_type: join_kind(join_type)?,
            probe_keys,
            build_keys,
            outputs,
            probe_types: probe_schema.fields.iter().map(|field| field.data_type.clone()).collect(),
            build_types: build_schema.fields.iter().map(|field| field.data_type.clone()).collect(),
        };
        Ok((PhysicalInputPlan::Join {
            probe: Box::new(probe),
            build: Box::new(build),
            plan: join,
        }, output_schema))
    }

    fn contains_join(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Join { .. } => true,
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::SubqueryAlias { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Aggregate { input, .. } => Self::contains_join(input),
            _ => false,
        }
    }

    fn compile_join_input_or_leaf(
        &self,
        plan: &LogicalPlan,
    ) -> Result<(PhysicalInputPlan, VeinSchema)> {
        match plan {
            LogicalPlan::Join { .. } => self.compile_join_input(plan),
            LogicalPlan::Aggregate { input, .. } if Self::contains_join(input) => {
                let (child, child_schema) = self.compile_join_input_or_leaf(input)?;
                let (program, output_schema) =
                    PipelineCompiler::compile_join_transform(plan, &child_schema)?;
                let aggregate = PipelineCompiler::aggregate(plan)?.ok_or_else(|| {
                    VeinError::planning("aggregate input did not produce an aggregate plan", None)
                })?;
                Ok((
                    PhysicalInputPlan::Aggregate {
                        input: Box::new(PhysicalInputPlan::Transform {
                            input: Box::new(child),
                            program: Arc::new(program),
                        }),
                        plan: aggregate,
                    },
                    output_schema,
                ))
            }
            LogicalPlan::Aggregate { .. } => {
                let (source, program, output_schema) = self.compile_pipeline(plan)?;
                let aggregate = PipelineCompiler::aggregate(plan)?.ok_or_else(|| {
                    VeinError::planning("aggregate input did not produce an aggregate plan", None)
                })?;
                Ok((
                    PhysicalInputPlan::Aggregate {
                        input: Box::new(PhysicalInputPlan::Pipeline {
                            source,
                            program: Arc::new(program),
                        }),
                        plan: aggregate,
                    },
                    output_schema,
                ))
            }
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::SubqueryAlias { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            if Self::contains_join(input) =>
                {
                    let (child, child_schema) = self.compile_join_input_or_leaf(input)?;
                    let (program, output_schema) =
                        PipelineCompiler::compile_join_transform(plan, &child_schema)?;
                    Ok((
                        PhysicalInputPlan::Transform {
                            input: Box::new(child),
                            program: Arc::new(program),
                        },
                        output_schema,
                    ))
                }
            _ => {
                let (source, program, schema) = self.compile_pipeline(plan)?;
                Ok((
                    PhysicalInputPlan::Pipeline {
                        source,
                        program: Arc::new(program),
                    },
                    schema,
                ))
            }
        }
    }
}

/// The protocol's join flavour, refusing the ones this engine cannot run rather
/// than mapping them onto the nearest outer join.
fn join_kind(join_type: &JoinType) -> Result<JoinKind> {
    Ok(match join_type {
        JoinType::Inner => JoinKind::Inner,
        JoinType::Left => JoinKind::Left,
        JoinType::Right => JoinKind::Right,
        JoinType::Full => JoinKind::Full,
        other => {
            return Err(VeinError::planning(
                format!("the local engine does not plan {other:?} joins yet"),
                None,
            ));
        }
    })
}

/// The two key columns of an equality `ON`.
///
/// Resolved by column id, never by name: only the id distinguishes `a.id` from
/// `b.id` in the self-join `ON a.id = b.id`, and resolving both sides against the
/// same positions would compare the wrong columns and return wrong rows.
fn equality_keys(
    on: Option<&LogicalExpr>,
    probe: &VeinSchema,
    build: &VeinSchema,
) -> Result<(u32, u32)> {
    let Some(LogicalExpr::BinaryOp {
                 left,
                 right,
                 op: BinaryOp::Eq,
                 ..
             }) = on
    else {
        return Err(VeinError::planning(
            "a join needs a single equality ON predicate, or USING columns",
            None,
        ));
    };
    let (LogicalExpr::Column(left), LogicalExpr::Column(right)) = (left.as_ref(), right.as_ref())
    else {
        return Err(VeinError::planning(
            "join keys must be columns; expressions and constant keys are not supported yet",
            None,
        ));
    };
    let probe_key =
        probe
            .position_of_id(left.id)
            .ok_or_else(|| {
                VeinError::planning(format!("join key {} is not on the probe side", left.name), None)
            })? as u32;
    let build_key =
        build
            .position_of_id(right.id)
            .ok_or_else(|| {
                VeinError::planning(format!("join key {} is not on the build side", right.name), None)
            })? as u32;
    Ok((probe_key, build_key))
}

fn position(schema: &VeinSchema, name: &str) -> Result<u32> {
    schema
        .position_by_name(name)
        .map(|index| index as u32)
        .ok_or_else(|| {
            VeinError::planning(format!("join key column '{name}' is not in the input"), None)
        })
}

/// Builds a join output layout from fields that survived input pruning.
/// Probe-first preserves `USING` semantics; unavailable logical fields are
/// omitted because no downstream operator can reference them.
fn available_join_outputs(
    schema: &VeinSchema,
    probe: &VeinSchema,
    build: &VeinSchema,
) -> (Vec<JoinOutput>, VeinSchema) {
    let mut outputs = Vec::with_capacity(schema.fields.len());
    let mut fields = Vec::with_capacity(schema.fields.len());
    for field in &schema.fields {
        let probe_index = probe.position_of_id(field.id).or_else(|| {
            probe.fields.iter().position(|candidate| {
                candidate.name == field.name && candidate.qualifier == field.qualifier
            })
        });
        if let Some(index) = probe_index {
            outputs.push(JoinOutput::Probe(index as u32));
            fields.push(Arc::clone(field));
            continue;
        }
        let build_index = build.position_of_id(field.id).or_else(|| {
            build.fields.iter().position(|candidate| {
                candidate.name == field.name && candidate.qualifier == field.qualifier
            })
        });
        if let Some(index) = build_index {
            outputs.push(JoinOutput::Build(index as u32));
            fields.push(Arc::clone(field));
        }
    }
    (outputs, VeinSchema::new(fields))
}

/// The output layout: each field of the join's schema, probe side matched first.
///
/// Probe-first is what makes a `USING` key appear once — the binder leaves it on
/// the left — rather than twice.
#[allow(dead_code)]
fn join_outputs(
    exprs: Option<&[LogicalExpr]>,
    schema: &VeinSchema,
    probe: &VeinSchema,
    build: &VeinSchema,
) -> Result<Vec<JoinOutput>> {
    let Some(exprs) = exprs else {
        return schema
            .fields
            .iter()
            .map(|field| {
                if let Some(index) = probe.position_of_id(field.id) {
                    Ok(JoinOutput::Probe(index as u32))
                } else if let Some(index) = build.position_of_id(field.id) {
                    Ok(JoinOutput::Build(index as u32))
                } else if let Some(index) = side_of(probe, field) {
                    Ok(JoinOutput::Probe(index as u32))
                } else if let Some(index) = side_of(build, field) {
                    Ok(JoinOutput::Build(index as u32))
                } else {
                    Err(VeinError::planning(
                        format!("join output column {} comes from neither input", field.name),
                        None,
                    ))
                }
            })
            .collect();
    };

    // A projection above the join: its expressions name the source columns, so
    // they resolve by id — the only thing that separates a self-join's two
    // columns of the same name.
    exprs
        .iter()
        .map(|expr| {
            let LogicalExpr::Column(column) = expr else {
                return Err(VeinError::planning(
                    "a projection above a join must select plain columns: an expression \
                     needs a program after the join, which a task does not carry yet",
                    None,
                ));
            };
            if let Some(index) = probe.position_of_id(column.id) {
                Ok(JoinOutput::Probe(index as u32))
            } else if let Some(index) = build.position_of_id(column.id) {
                Ok(JoinOutput::Build(index as u32))
            } else {
                Err(VeinError::planning(
                    format!("join output column {} comes from neither input", column.name),
                    None,
                ))
            }
        })
        .collect()
}

/// The position of `field` in `schema`, matched by name and qualifier.
#[allow(dead_code)]
fn side_of(schema: &VeinSchema, field: &plan::schema::ResolvedColumnRef) -> Option<usize> {
    schema
        .fields
        .iter()
        .position(|candidate| {
            candidate.name == field.name && candidate.qualifier == field.qualifier
        })
}


pub(crate) fn split_file_scan_source(source: DataSource) -> Result<Vec<(PartitionId, DataSource)>> {
    let DataSource::File(scan) = source else {
        return Ok(vec![(PartitionId::from_raw(0), source)]);
    };

    if scan.files.len() <= 1 {
        return Ok(vec![(PartitionId::from_raw(0), DataSource::File(scan))]);
    }

    scan.files
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, file)| {
            let partition = u16::try_from(index)
                .map(PartitionId::from_raw)
                .map_err(|_| VeinError::planning("file scan has more than u16::MAX files", None))?;
            let mut file_scan = scan.clone();
            file_scan.files = vec![file];
            file_scan.task_id = TaskId::new();
            Ok((partition, DataSource::File(file_scan)))
        })
        .collect()
}

impl PipelineStageBuilder for DefaultPipelineStageBuilder {
    fn build(
        &self,
        plan: &LogicalPlan,
    ) -> Result<Stage> {
        let limit = PipelineCompiler::window(plan)?;
        let aggregate = PipelineCompiler::aggregate(plan)?;
        let sort = PipelineCompiler::sort(plan)?;
        let sort_key_program = PipelineCompiler::sort_key_program(plan)?.map(Arc::new);
        let post_aggregate_program = PipelineCompiler::post_aggregate_program(plan)?.map(Arc::new);
        if Self::contains_join(plan) {
            return self.build_join(plan, limit, sort, sort_key_program);
        }
        let (source, program, out_schema) = self.compile_pipeline(plan)?;
        let sort_output_columns = sort_key_program
            .as_ref()
            .map(|_| out_schema.fields.len() as u32);
        let sink = match plan {
            LogicalPlan::Write {
                target, options, ..
            } => {
                let key_values: Vec<common::protocol::KeyValue> = options
                    .iter()
                    .map(|(k, v)| common::protocol::KeyValue {
                        key: k.clone(),
                        value: v.clone(),
                    })
                    .collect();
                // The Arrow schema exists only to encode the WRITE target's
                // schema IPC, so a read never pays to build it.
                let arrow_schema = out_schema.to_arrow();
                let mut schema_bytes = Vec::new();
                {
                    let mut writer = arrow::ipc::writer::StreamWriter::try_new(
                        &mut schema_bytes,
                        &arrow_schema,
                    ).map_err(|error| VeinError::internal(format!("encode WRITE schema: {error}")))?;
                    writer.finish().map_err(|error| VeinError::internal(format!("finish WRITE schema: {error}")))?;
                }
                let target_obj = common::protocol::StorageTarget::new(
                    target.clone(),
                    key_values,
                    Arc::new(out_schema),
                ).with_schema_ipc(schema_bytes);
                DataSink::Storage(target_obj)
            }
            _ => DataSink::Client {
                schema: Arc::new(out_schema),
            },
        };
        let stage_id = StageId::new();
        let mut stage = Stage::new(stage_id);
        // A limit counts rows globally and an aggregate groups them globally, so
        // a plan with either stays one task: the per-file split below would apply
        // the window, or the grouping, once per file.
        let sources = if limit.is_some()
            || aggregate.is_some()
            || sort.is_some()
            || matches!(sink, DataSink::Storage(_))
        {
            vec![(PartitionId::from_raw(0), source)]
        } else {
            split_file_scan_source(source)?
        };
        let program = Arc::new(program);
        let sink = Arc::new(sink);
        for (partition, source) in sources {
            stage.tasks.push(
                Task::new(
                    TaskId::new(),
                    partition,
                    source,
                    Arc::clone(&program),
                    Arc::clone(&sink),
                )
                    .with_limit(limit)
                    .with_sort(sort.clone())
                    .with_sort_key_program(sort_key_program.clone(), sort_output_columns)
                    .with_aggregate(aggregate.clone())
                    .with_post_aggregate_program(post_aggregate_program.clone()),
            );
        }
        Ok(stage)
    }

    fn compile_pipeline(
        &self,
        plan: &LogicalPlan,
    ) -> Result<(DataSource, MicroProgram, VeinSchema)> {
        PipelineCompiler::compile(plan)
    }
}

#[cfg(test)]
mod tests {
    use super::{split_file_scan_source, DefaultPipelineStageBuilder};
    use common::identifiers::TaskId;
    use common::protocol::{DataSource, FileFormat, FileScanTask};
    use plan::schema::VeinSchema;
    use plan::{LogicalExpr, LogicalPlan};
    use std::sync::Arc;

    fn scan_plan() -> LogicalPlan {
        let schema = Arc::new(VeinSchema::empty());
        let table = Arc::new(
            plan::TableMetadata::new("empty", Arc::clone(&schema))
                .with_format(plan::TableFormat::Parquet),
        );
        LogicalPlan::Scan {
            table,
            projection: None,
            filters: Vec::new(),
            schema: Box::new((*schema).clone()),
        }
    }

    #[test]
    fn detects_join_under_aggregate_for_recursive_planning() {
        let join = LogicalPlan::Join {
            join_type: parser::ast::pipe_op::JoinType::Inner,
            left: Box::new(scan_plan()),
            right: Box::new(scan_plan()),
            on: None,
            using: Vec::new(),
            schema: Box::new(VeinSchema::empty()),
        };
        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(join),
            grouping_sets: vec![Vec::new()],
            aggs: Vec::<LogicalExpr>::new(),
            schema: Box::new(VeinSchema::empty()),
        };

        assert!(DefaultPipelineStageBuilder::contains_join(&aggregate));
    }

    #[test]
    fn splits_multi_file_scan_into_stable_single_file_units() {
        let source = DataSource::File(FileScanTask {
            files: vec!["a.parquet".to_string(), "b.parquet".to_string()],
            format: FileFormat::Parquet,
            byte_range: None,
            projected: Arc::new(VeinSchema::empty()),
            dictionary_columns: Vec::new(),
            pushdown: None,
            task_id: TaskId::new(),
            catalog_snapshot: None,
        });

        let units = split_file_scan_source(source).expect("file scan should split");

        assert_eq!(units.len(), 2);
        assert_eq!(units[0].0.into_raw(), 0);
        assert_eq!(units[1].0.into_raw(), 1);
        for ((_, source), expected_path) in units.iter().zip(["a.parquet", "b.parquet"]) {
            let DataSource::File(scan) = source else {
                panic!("split scan should remain a file source");
            };
            assert_eq!(scan.files, vec![expected_path]);
        }
    }
}
