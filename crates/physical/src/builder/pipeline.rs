//! Linear pipeline builder abstraction: translates unbroken operator sequences into a single [`Stage`].

use crate::compiler::PipelineCompiler;
use crate::stage::Stage;
use crate::task::Task;
use common::Result;
use common::identifiers::{PartitionId, StageId, TaskId};
use common::protocol::{
    DataSink, DataSource, JoinKind, JoinOutput, JoinPlan, LimitSpec, MicroProgram,
    PhysicalInputPlan,
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
                .with_input_plan(Arc::new(input_plan)),
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
        let residual;
        if *join_type == JoinType::Cross {
            if on.is_some() || !using.is_empty() {
                return Err(VeinError::planning(
                    "CROSS JOIN cannot contain an ON or USING condition",
                    None,
                ));
            }
            residual = None;
        } else if using.is_empty() {
            let mut conjuncts = Vec::new();
            if let Some(on) = on {
                flatten_conjunction(on, &mut conjuncts);
            }
            let (keys, residual_conjuncts) =
                extract_equi_join_keys(&conjuncts, &probe_schema, &build_schema)?;
            (probe_keys, build_keys) = keys;
            residual = PipelineCompiler::compile_join_residual(
                &residual_conjuncts,
                &probe_schema,
                &build_schema,
                probe_keys[0],
            )?;
        } else {
            for column in using {
                probe_keys.push(position(&probe_schema, &column.name)?);
                build_keys.push(position(&build_schema, &column.name)?);
            }
            let mut conjuncts = Vec::new();
            if let Some(on) = on {
                flatten_conjunction(on, &mut conjuncts);
            }
            residual = PipelineCompiler::compile_join_residual(
                &conjuncts.into_iter().cloned().collect::<Vec<_>>(),
                &probe_schema,
                &build_schema,
                probe_keys[0],
            )?;
        }

        let (outputs, output_schema) =
            available_join_outputs(schema, &probe_schema, &build_schema)?;
        let join = JoinPlan {
            join_type: join_kind(join_type),
            probe_keys,
            build_keys,
            residual,
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
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Distinct { input, .. } => Self::contains_join(input),
            _ => false,
        }
    }

    /// Whether this subtree contains a node whose output is produced by a
    /// recursive pipeline boundary rather than the ordinary scan VM.
    fn contains_recursive_boundary(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Join { .. } | LogicalPlan::Aggregate { .. } | LogicalPlan::Distinct { .. } => true,
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::SubqueryAlias { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => Self::contains_recursive_boundary(input),
            _ => false,
        }
    }

    fn compile_join_input_or_leaf(
        &self,
        plan: &LogicalPlan,
    ) -> Result<(PhysicalInputPlan, VeinSchema)> {
        match plan {
            LogicalPlan::Join { .. } => self.compile_join_input(plan),
            LogicalPlan::Aggregate { input, .. } | LogicalPlan::Distinct { input, .. }
            if Self::contains_join(input) =>
                {
                    let (child, child_schema) = self.compile_join_input_or_leaf(input)?;
                    let (program, output_schema) =
                        PipelineCompiler::compile_join_transform(plan, &child_schema)?;
                    let aggregate = PipelineCompiler::aggregate(plan)?.ok_or_else(|| {
                        VeinError::planning(
                            "recursive aggregate input did not produce an aggregate plan",
                            None,
                        )
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
            // Keep ordering and row-window boundaries in the recursive tree:
            // moving them to task-level metadata can change their order relative
            // to joins and blocking operators such as aggregates.
            LogicalPlan::Sort { input, .. } if Self::contains_recursive_boundary(input) => {
                let (child, child_schema) = self.compile_join_input_or_leaf(input)?;
                let (sort_plan, key_program) = PipelineCompiler::recursive_sort(plan)?.ok_or_else(|| {
                    VeinError::planning("recursive sort did not produce a sort plan", None)
                })?;
                let output_columns = u32::try_from(child_schema.fields.len()).map_err(|_| {
                    VeinError::planning("recursive sort input has too many columns", None)
                })?;
                Ok((
                    PhysicalInputPlan::Sort {
                        input: Box::new(child),
                        plan: sort_plan,
                        key_program: key_program.map(Arc::new),
                        output_columns,
                    },
                    child_schema,
                ))
            }
            LogicalPlan::Limit { input, offset, count, .. }
                if Self::contains_recursive_boundary(input) =>
            {
                let (child, schema) = self.compile_join_input_or_leaf(input)?;
                Ok((
                    PhysicalInputPlan::Limit {
                        input: Box::new(child),
                        spec: LimitSpec { offset: *offset, count: *count },
                    },
                    schema,
                ))
            }
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::SubqueryAlias { input, .. }
            if Self::contains_recursive_boundary(input) =>
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

fn flatten_conjunction<'a>(expression: &'a LogicalExpr, output: &mut Vec<&'a LogicalExpr>) {
    if let LogicalExpr::BinaryOp {
        left,
        op: BinaryOp::And,
        right,
        ..
    } = expression
    {
        flatten_conjunction(left, output);
        flatten_conjunction(right, output);
    } else {
        output.push(expression);
    }
}

fn is_column_equality(expression: &LogicalExpr) -> bool {
    matches!(
        expression,
        LogicalExpr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
            ..
        } if matches!(left.as_ref(), LogicalExpr::Column(_))
            && matches!(right.as_ref(), LogicalExpr::Column(_))
    )
}

/// Extract all column equalities that compare one probe column with one build
/// column. Other conjuncts remain residual predicates, so same-side equalities
/// and non-equality conditions still participate in the complete `ON` test.
fn extract_equi_join_keys(
    conjuncts: &[&LogicalExpr],
    probe: &VeinSchema,
    build: &VeinSchema,
) -> Result<((Vec<u32>, Vec<u32>), Vec<LogicalExpr>)> {
    let mut probe_keys = Vec::new();
    let mut build_keys = Vec::new();
    let mut residual = Vec::new();

    for conjunct in conjuncts {
        if is_column_equality(conjunct) {
            if let Ok((probe_key, build_key)) = equality_keys(Some(conjunct), probe, build) {
                probe_keys.push(probe_key);
                build_keys.push(build_key);
                continue;
            }
        }
        residual.push((*conjunct).clone());
    }

    if probe_keys.is_empty() {
        return Err(VeinError::planning(
            "a hash join needs at least one equality between probe-side and build-side columns in its ON condition",
            None,
        ));
    }

    Ok(((probe_keys, build_keys), residual))
}

/// Maps each parser join flavor to its physical-plan counterpart.
fn join_kind(join_type: &JoinType) -> JoinKind {
    match join_type {
        JoinType::Inner => JoinKind::Inner,
        JoinType::Left => JoinKind::Left,
        JoinType::Right => JoinKind::Right,
        JoinType::Full => JoinKind::Full,
        JoinType::Semi => JoinKind::Semi,
        JoinType::Anti => JoinKind::Anti,
        JoinType::Single => JoinKind::Single,
        JoinType::Cross => JoinKind::Cross,
    }
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
    let left_probe = probe.position_of_id(left.id);
    let left_build = build.position_of_id(left.id);
    let right_probe = probe.position_of_id(right.id);
    let right_build = build.position_of_id(right.id);

    match (
        (left_probe, left_build),
        (right_probe, right_build),
    ) {
        ((Some(probe_key), None), (None, Some(build_key)))
        | ((None, Some(build_key)), (Some(probe_key), None)) => {
            Ok((probe_key as u32, build_key as u32))
        }
        _ => Err(VeinError::planning(
            "join equality must compare one probe-side column with one build-side column",
            None,
        )),
    }
}

fn position(schema: &VeinSchema, name: &str) -> Result<u32> {
    schema
        .position_by_name(name)
        .map(|index| index as u32)
        .ok_or_else(|| {
            VeinError::planning(format!("join key column '{name}' is not in the input"), None)
        })
}

/// Maps every declared join output to its physical input position.
/// Probe-first preserves `USING` semantics. Column pruning narrows the logical
/// join schema to parent-required outputs, so a missing declared field violates
/// the plan contract and must fail here rather than disappear silently.
fn available_join_outputs(
    schema: &VeinSchema,
    probe: &VeinSchema,
    build: &VeinSchema,
) -> Result<(Vec<JoinOutput>, VeinSchema)> {
    let mut outputs = Vec::with_capacity(schema.fields.len());
    let mut fields = Vec::with_capacity(schema.fields.len());
    for field in &schema.fields {
        let probe_index = probe.position_of_id(field.id).or_else(|| {
            probe.fields.iter().position(|candidate| {
                candidate.name == field.name && candidate.qualifier == field.qualifier
            })
        });
        if let Some(index) = probe_index {
            let index = u32::try_from(index).map_err(|_| {
                VeinError::planning("join probe input has too many columns", None)
            })?;
            outputs.push(JoinOutput::Probe(index));
            fields.push(Arc::clone(field));
            continue;
        }
        let build_index = build.position_of_id(field.id).or_else(|| {
            build.fields.iter().position(|candidate| {
                candidate.name == field.name && candidate.qualifier == field.qualifier
            })
        });
        if let Some(index) = build_index {
            let index = u32::try_from(index).map_err(|_| {
                VeinError::planning("join build input has too many columns", None)
            })?;
            outputs.push(JoinOutput::Build(index));
            fields.push(Arc::clone(field));
            continue;
        }

        return Err(VeinError::planning(
            format!(
                "join output column '{}' (ColumnId({})) is missing from both physical inputs",
                field.name, field.id.0
            ),
            None,
        ));
    }
    Ok((outputs, VeinSchema::new(fields)))
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
        if Self::contains_join(plan) {
            return self.build_join(plan);
        }
        let limit = PipelineCompiler::window(plan)?;
        let aggregate = PipelineCompiler::aggregate(plan)?;
        let sort = PipelineCompiler::sort(plan)?;
        let sort_key_program = PipelineCompiler::sort_key_program(plan)?.map(Arc::new);
        let post_aggregate_program = PipelineCompiler::post_aggregate_program(plan)?.map(Arc::new);
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
    use super::{
        available_join_outputs, equality_keys, extract_equi_join_keys, split_file_scan_source,
        DefaultPipelineStageBuilder, PipelineStageBuilder,
    };
    use common::identifiers::TaskId;
    use common::protocol::{DataSource, FileFormat, FileScanTask, PhysicalInputPlan};
    use plan::schema::{ColumnId, ResolvedColumn, VeinDataType, VeinSchema};
    use plan::{LogicalExpr, LogicalPlan, SortKey as LogicalSortKey};
    use std::sync::Arc;

    fn test_column(id: usize, name: &str) -> Arc<ResolvedColumn> {
        Arc::new(ResolvedColumn::new(
            ColumnId(id),
            name,
            None,
            VeinDataType::Int64,
        ))
    }

    fn equality(left: Arc<ResolvedColumn>, right: Arc<ResolvedColumn>) -> LogicalExpr {
        LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(left)),
            op: parser::ast::expr::BinaryOp::Eq,
            right: Box::new(LogicalExpr::Column(right)),
            return_type: VeinDataType::Boolean,
        }
    }

    fn join_key_schemas() -> (VeinSchema, VeinSchema, Arc<ResolvedColumn>, Arc<ResolvedColumn>) {
        let probe_key = test_column(11, "key");
        let build_key = test_column(21, "key");
        let probe_schema = VeinSchema::new(vec![test_column(10, "other"), Arc::clone(&probe_key)]);
        let build_schema = VeinSchema::new(vec![Arc::clone(&build_key), test_column(22, "other")]);
        (probe_schema, build_schema, probe_key, build_key)
    }

    #[test]
    fn resolves_probe_to_build_equality_keys() {
        let (probe, build, probe_key, build_key) = join_key_schemas();

        assert_eq!(
            equality_keys(Some(&equality(probe_key, build_key)), &probe, &build)
                .expect("probe-to-build equality should resolve"),
            (1, 0)
        );
    }

    #[test]
    fn resolves_build_to_probe_equality_keys() {
        let (probe, build, probe_key, build_key) = join_key_schemas();

        assert_eq!(
            equality_keys(Some(&equality(build_key, probe_key)), &probe, &build)
                .expect("build-to-probe equality should resolve"),
            (1, 0)
        );
    }

    #[test]
    fn join_output_mapping_rejects_declared_columns_missing_from_both_inputs() {
        let probe_key = test_column(11, "probe_key");
        let build_key = test_column(21, "build_key");
        let missing_output = test_column(99, "required_value");
        let probe = VeinSchema::new(vec![Arc::clone(&probe_key)]);
        let build = VeinSchema::new(vec![Arc::clone(&build_key)]);
        let declared_output = VeinSchema::new(vec![missing_output]);

        let error = available_join_outputs(&declared_output, &probe, &build)
            .expect_err("a declared but unavailable output must fail planning");

        let message = error.to_string();
        assert!(message.contains("required_value"), "{message}");
        assert!(message.contains("ColumnId(99)"), "{message}");
        assert!(message.contains("both physical inputs"), "{message}");
    }

    #[test]
    fn join_output_mapping_preserves_only_declared_pruned_outputs() {
        let probe_key = test_column(11, "probe_key");
        let probe_value = test_column(12, "probe_value");
        let build_key = test_column(21, "build_key");
        let unused_build_value = test_column(22, "unused_build_value");
        let probe = VeinSchema::new(vec![Arc::clone(&probe_key), Arc::clone(&probe_value)]);
        let build = VeinSchema::new(vec![
            Arc::clone(&build_key),
            Arc::clone(&unused_build_value),
        ]);
        let declared_output = VeinSchema::new(vec![Arc::clone(&probe_value)]);

        let (outputs, output_schema) =
            available_join_outputs(&declared_output, &probe, &build)
                .expect("a narrowed join output should map successfully");

        assert_eq!(outputs.len(), 1);
        assert!(matches!(outputs[0], common::protocol::JoinOutput::Probe(1)));
        assert_eq!(
            output_schema.fields.iter().map(|field| field.id).collect::<Vec<_>>(),
            vec![probe_value.id]
        );
        assert!(Arc::ptr_eq(&output_schema.fields[0], &probe_value));
    }

    #[test]
    fn extracts_composite_equi_join_keys_and_keeps_other_conjuncts_residual() {
        let probe_key = test_column(11, "probe_key");
        let probe_region = test_column(12, "probe_region");
        let probe_value = test_column(10, "probe_value");
        let build_key = test_column(21, "build_key");
        let build_region = test_column(23, "build_region");
        let build_value = test_column(22, "build_value");
        let probe = VeinSchema::new(vec![
            Arc::clone(&probe_value),
            Arc::clone(&probe_key),
            Arc::clone(&probe_region),
        ]);
        let build = VeinSchema::new(vec![
            Arc::clone(&build_key),
            Arc::clone(&build_value),
            Arc::clone(&build_region),
        ]);
        let residual = LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(probe_value)),
            op: parser::ast::expr::BinaryOp::Gt,
            right: Box::new(LogicalExpr::Column(build_value)),
            return_type: VeinDataType::Boolean,
        };
        let conjuncts = [
            equality(Arc::clone(&probe_key), Arc::clone(&build_key)),
            equality(Arc::clone(&build_region), Arc::clone(&probe_region)),
            residual.clone(),
        ];
        let conjunct_refs: Vec<_> = conjuncts.iter().collect();

        let ((probe_keys, build_keys), residuals) =
            extract_equi_join_keys(&conjunct_refs, &probe, &build)
                .expect("both cross-input equalities should be hash keys");

        assert_eq!(probe_keys, vec![1, 2]);
        assert_eq!(build_keys, vec![0, 2]);
        assert_eq!(residuals, vec![residual]);
    }

    #[test]
    fn keeps_same_side_equality_residual_when_extracting_join_keys() {
        let probe_key = test_column(11, "probe_key");
        let probe_value = test_column(10, "probe_value");
        let build_key = test_column(21, "build_key");
        let probe = VeinSchema::new(vec![Arc::clone(&probe_value), Arc::clone(&probe_key)]);
        let build = VeinSchema::new(vec![Arc::clone(&build_key)]);
        let same_side = equality(Arc::clone(&probe_value), Arc::clone(&probe_key));
        let cross_side = equality(Arc::clone(&build_key), Arc::clone(&probe_key));
        let conjuncts = [same_side.clone(), cross_side];
        let conjunct_refs: Vec<_> = conjuncts.iter().collect();

        let ((probe_keys, build_keys), residuals) =
            extract_equi_join_keys(&conjunct_refs, &probe, &build)
                .expect("cross-input equality should provide the hash key");

        assert_eq!(probe_keys, vec![1]);
        assert_eq!(build_keys, vec![0]);
        assert_eq!(residuals, vec![same_side]);
    }

    #[test]
    fn rejects_equality_keys_from_the_same_input_side() {
        let (probe, build, probe_key, _) = join_key_schemas();

        let error = equality_keys(
            Some(&equality(Arc::clone(&probe_key), probe_key)),
            &probe,
            &build,
        )
            .expect_err("same-side equality cannot be a hash join key");

        assert!(error.to_string().contains("one probe-side column"));
    }

    #[test]
    fn builds_recursive_sort_and_limit_nodes_without_duplicate_task_windows() {
        let probe_column = test_column(1, "probe_key");
        let build_column = test_column(2, "build_key");
        let probe_schema = VeinSchema::new(vec![Arc::clone(&probe_column)]);
        let build_schema = VeinSchema::new(vec![Arc::clone(&build_column)]);
        let output_schema = VeinSchema::new(vec![Arc::clone(&probe_column), Arc::clone(&build_column)]);
        let scan = |url: &str, schema: &VeinSchema| LogicalPlan::ExternalScan {
            url: url.to_owned(),
            options: Vec::new(),
            files: Vec::new(),
            schema: Box::new(schema.clone()),
        };
        let join = LogicalPlan::Join {
            join_type: parser::ast::pipe_op::JoinType::Inner,
            left: Box::new(scan("probe", &probe_schema)),
            right: Box::new(scan("build", &build_schema)),
            on: Some(equality(Arc::clone(&probe_column), Arc::clone(&build_column))),
            using: Vec::new(),
            schema: Box::new(output_schema.clone()),
        };
        let sort = |input| LogicalPlan::Sort {
            input: Box::new(input),
            sort_keys: vec![LogicalSortKey {
                expr: LogicalExpr::Column(Arc::clone(&probe_column)),
                asc: true,
                nulls_first: false,
            }],
            schema: Box::new(output_schema.clone()),
        };
        let limit = |input| LogicalPlan::Limit {
            input: Box::new(input),
            offset: 1,
            count: 2,
            schema: Box::new(output_schema.clone()),
        };

        for plan in [limit(sort(join.clone())), sort(limit(join))] {
            let stage = DefaultPipelineStageBuilder::new()
                .build(&plan)
                .expect("recursive join with sort and limit should plan");
            let task = stage.tasks.first().expect("join stage task");

            assert!(task.limit.is_none(), "recursive limit must not be applied twice");
            assert!(task.sort.is_none(), "recursive sort must not be applied twice");
            let input = task.input_plan.as_deref().expect("recursive input plan");
            match (&plan, input) {
                (LogicalPlan::Limit { .. }, PhysicalInputPlan::Limit { input, .. }) => {
                    assert!(matches!(input.as_ref(), PhysicalInputPlan::Sort { .. }));
                }
                (LogicalPlan::Sort { .. }, PhysicalInputPlan::Sort { input, .. }) => {
                    assert!(matches!(input.as_ref(), PhysicalInputPlan::Limit { .. }));
                }
                _ => panic!("root operator order must be preserved by recursive plan"),
            }
        }
    }

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
