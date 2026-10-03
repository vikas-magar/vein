//! Linear pipeline builder abstraction: translates unbroken operator sequences into a single [`Stage`].

use crate::compiler::PipelineCompiler;
use crate::stage::Stage;
use crate::task::Task;
use common::Result;
use common::identifiers::{PartitionId, StageId, TaskId};
use common::protocol::{
    DataSink, DataSource, JoinKind, JoinOutput, JoinPlan, LimitSpec, MicroProgram,
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
    /// The probe side compiles to an ordinary pipeline (source plus program);
    /// the build side compiles to a source the executor must read to completion
    /// before the probe starts. Both describe one [`Task::join`], because only
    /// the runtime can sequence them.
    fn build_join(
        &self,
        plan: &LogicalPlan,
        schema: &VeinSchema,
        outputs: Option<&[LogicalExpr]>,
        limit: Option<LimitSpec>,
    ) -> Result<Stage> {
        let LogicalPlan::Join {
            join_type,
            left,
            right,
            on,
            using,
            ..
        } = plan
        else {
            return Err(VeinError::internal("build_join called on a non-join plan"));
        };

        let kind = join_kind(join_type)?;
        let (probe_source, probe_program, probe_schema) = self.compile_pipeline(left)?;
        let (build_source, _build_program, build_schema) = self.compile_pipeline(right)?;

        let mut probe_keys = Vec::new();
        let mut build_keys = Vec::new();
        if using.is_empty() {
            let (probe_key, build_key) =
                equality_keys(on.as_ref(), &probe_schema, &build_schema)?;
            probe_keys.push(probe_key);
            build_keys.push(build_key);
        } else {
            for column in using {
                probe_keys.push(position(&probe_schema, &column.name)?);
                build_keys.push(position(&build_schema, &column.name)?);
            }
        }

        let join = JoinPlan {
            join_type: kind,
            probe_keys,
            build_keys,
            outputs: join_outputs(outputs, schema, &probe_schema, &build_schema)?,
            probe_types: probe_schema
                .fields
                .iter()
                .map(|field| field.data_type.clone())
                .collect(),
            build_types: build_schema
                .fields
                .iter()
                .map(|field| field.data_type.clone())
                .collect(),
        };

        let mut stage = Stage::new(StageId::new());
        stage.tasks.push(
            Task::join(
                TaskId::new(),
                PartitionId::from_raw(0),
                probe_source,
                build_source,
                join,
                Arc::new(probe_program),
                Arc::new(DataSink::Client {
                    schema: Arc::new(schema.clone()),
                }),
            )
            .with_limit(limit),
        );
        Ok(stage)
    }
}

/// The join a plan is built around, the schema the plan produces, and the
/// projection above it when there is one.
///
/// A projection above the join is not a second operator: the join is where the
/// pipeline breaks, so the projection is the join's output column list. Its
    /// **expressions** are what identify the source columns — its schema holds fresh
    /// output ids that match neither input, which is why they cannot be used.
fn join_root(plan: &LogicalPlan) -> Option<(&LogicalPlan, &VeinSchema, Option<&[LogicalExpr]>)> {
    match plan {
        LogicalPlan::Join { schema, .. } => Some((plan, schema.as_ref(), None)),
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } if matches!(input.as_ref(), LogicalPlan::Join { .. }) => {
            Some((input.as_ref(), schema.as_ref(), Some(exprs)))
        }
        // A limit sits above whatever it windows, so the join beneath it is still
        // the pipeline breaker that has to be built.
        LogicalPlan::Limit { input, .. } => join_root(input),
        _ => None,
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

/// The output layout: each field of the join's schema, probe side matched first.
///
/// Probe-first is what makes a `USING` key appear once — the binder leaves it on
/// the left — rather than twice.
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
        if let Some((join, schema, outputs)) = join_root(plan) {
            return self.build_join(join, schema, outputs, limit);
        }
        let (source, program, out_schema) = self.compile_pipeline(plan)?;
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
        let sources = if limit.is_some() || aggregate.is_some() || matches!(sink, DataSink::Storage(_))
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
                .with_aggregate(aggregate.clone()),
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
    use super::split_file_scan_source;
    use common::identifiers::TaskId;
    use common::protocol::{DataSource, FileFormat, FileScanTask};
    use plan::schema::VeinSchema;
    use std::sync::Arc;

    #[test]
    fn splits_multi_file_scan_into_stable_single_file_units() {
        let source = DataSource::File(FileScanTask {
            files: vec!["a.parquet".to_string(), "b.parquet".to_string()],
            format: FileFormat::Parquet,
            byte_range: None,
            projected: Arc::new(VeinSchema::empty()),
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
