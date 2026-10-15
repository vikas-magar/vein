//! Linear pipeline builder abstraction: translates unbroken operator sequences into a single [`Stage`].

use crate::compiler::PipelineCompiler;
use crate::stage::Stage;
use crate::task::Task;
use crate::pipeline_graph::DerivedPipelineGraph;
use common::Result;
use common::identifiers::{PartitionId, StageId, TaskId};
use common::protocol::{
    DataSink, DataSource, JoinKind, JoinOutput, JoinPlan, LimitSpec, MicroProgram, PhysicalPlan,
    PhysicalPlanNode, PhysicalPlanOperator,
};
use error::VeinError;
use parser::ast::expr::BinaryOp;
use parser::ast::pipe_op::JoinType;
use plan::schema::VeinSchema;
use plan::{LogicalExpr, LogicalPlan};
use std::sync::Arc;

/// Trait for lowering supported logical plans into physical [`Stage`]s.
pub trait PipelineStageBuilder: Send + Sync {
    /// Lowers a logical plan into a stage with an explicit physical sink.
    fn build(&self, plan: &LogicalPlan) -> Result<Stage>;

    /// Compiles a fused source segment and returns its source and schemas.
    fn compile_pipeline(
        &self,
        plan: &LogicalPlan,
    ) -> Result<(DataSource, MicroProgram, VeinSchema, VeinSchema)>;
}

/// Default recursive stage builder with fused scan pipelines.
#[derive(Debug, Default, Clone)]
pub struct DefaultPipelineStageBuilder;

impl DefaultPipelineStageBuilder {
    /// Creates a new default recursive stage builder.
    pub fn new() -> Self {
        Self
    }

    fn compile_join_input(
        &self,
        plan: &LogicalPlan,
    ) -> Result<(Arc<PhysicalPlanNode>, VeinSchema)> {
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
            return self.lower_node(plan);
        };

        let (probe, probe_schema) = self.lower_node(left)?;
        let (build, build_schema) = self.lower_node(right)?;

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
        let output_schema = Arc::new(output_schema);
        Ok((
            Arc::new(PhysicalPlanNode {
                output_schema: Arc::clone(&output_schema),
                operator: PhysicalPlanOperator::Join {
                    probe,
                    build,
                    plan: join,
                },
            }),
            (*output_schema).clone(),
        ))
    }

    /// A scan and its adjacent row-wise operators form one source VM segment.
    /// Keeping this segment fused avoids materializing intermediate batches.
    fn is_source_pipeline(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Scan { .. } | LogicalPlan::ExternalScan { .. } => true,
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::SubqueryAlias { input, .. } => Self::is_source_pipeline(input),
            _ => false,
        }
    }

    /// Lowers every supported relational shape into the canonical physical
    /// tree. Row-wise scan segments are compiled together; recursive operators
    /// retain their logical order and form explicit physical boundaries.
    fn lower_node(&self, plan: &LogicalPlan) -> Result<(Arc<PhysicalPlanNode>, VeinSchema)> {
        if Self::is_source_pipeline(plan) {
            let (source, program, input_schema, schema) = self.compile_pipeline(plan)?;
            let output_schema = Arc::new(schema);
            return Ok((
                Arc::new(PhysicalPlanNode {
                    output_schema: Arc::clone(&output_schema),
                    operator: PhysicalPlanOperator::Pipeline {
                        source: Arc::new(source),
                        input_schema: Arc::new(input_schema),
                        program: Arc::new(program),
                    },
                }),
                (*output_schema).clone(),
            ));
        }

        match plan {
            LogicalPlan::Join { .. } => self.compile_join_input(plan),
            LogicalPlan::Aggregate { input, .. } => {
                let (child, child_schema) = self.lower_node(input)?;
                let (program, output_schema) =
                    PipelineCompiler::compile_recursive_transform(plan, &child_schema)?;
                let aggregate = PipelineCompiler::aggregate(plan)?.ok_or_else(|| {
                    VeinError::planning("aggregate did not produce an aggregate plan", None)
                })?;
                let output_schema = Arc::new(output_schema);
                let transformed = Arc::new(PhysicalPlanNode {
                    output_schema: Arc::clone(&output_schema),
                    operator: PhysicalPlanOperator::Transform {
                        input: child,
                        program: Arc::new(program),
                    },
                });
                Ok((
                    Arc::new(PhysicalPlanNode {
                        output_schema: Arc::clone(&output_schema),
                        operator: PhysicalPlanOperator::Aggregate {
                            input: transformed,
                            plan: aggregate,
                        },
                    }),
                    (*output_schema).clone(),
                ))
            }
            LogicalPlan::Distinct { input, .. } => {
                let (child, child_schema) = self.lower_node(input)?;
                let (program, output_schema) =
                    PipelineCompiler::compile_recursive_transform(plan, &child_schema)?;
                let distinct = PipelineCompiler::aggregate(plan)?.ok_or_else(|| {
                    VeinError::planning("DISTINCT did not produce a distinct plan", None)
                })?;
                let output_schema = Arc::new(output_schema);
                let transformed = Arc::new(PhysicalPlanNode {
                    output_schema: Arc::clone(&output_schema),
                    operator: PhysicalPlanOperator::Transform {
                        input: child,
                        program: Arc::new(program),
                    },
                });
                Ok((
                    Arc::new(PhysicalPlanNode {
                        output_schema: Arc::clone(&output_schema),
                        operator: PhysicalPlanOperator::Distinct {
                            input: transformed,
                            plan: distinct,
                        },
                    }),
                    (*output_schema).clone(),
                ))
            }
            LogicalPlan::Sort { input, .. } => {
                let (child, child_schema) = self.lower_node(input)?;
                let (sort_plan, key_program) = PipelineCompiler::recursive_sort(plan)?
                    .ok_or_else(|| VeinError::planning("sort did not produce a sort plan", None))?;
                let output_columns = u32::try_from(child_schema.fields.len())
                    .map_err(|_| VeinError::planning("sort input has too many columns", None))?;
                let output_schema = Arc::new(child_schema);
                Ok((
                    Arc::new(PhysicalPlanNode {
                        output_schema: Arc::clone(&output_schema),
                        operator: PhysicalPlanOperator::Sort {
                            input: child,
                            plan: sort_plan,
                            key_program: key_program.map(Arc::new),
                            output_columns,
                        },
                    }),
                    (*output_schema).clone(),
                ))
            }
            LogicalPlan::Limit {
                input,
                offset,
                count,
                ..
            } => {
                let (child, schema) = self.lower_node(input)?;
                let output_schema = Arc::new(schema);
                Ok((
                    Arc::new(PhysicalPlanNode {
                        output_schema: Arc::clone(&output_schema),
                        operator: PhysicalPlanOperator::Limit {
                            input: child,
                            spec: LimitSpec {
                                offset: *offset,
                                count: *count,
                            },
                        },
                    }),
                    (*output_schema).clone(),
                ))
            }
            LogicalPlan::Filter { .. }
            | LogicalPlan::Project { .. }
            | LogicalPlan::SubqueryAlias { .. } => {
                let mut chain = Vec::new();
                let mut base = plan;
                loop {
                    match base {
                        LogicalPlan::Filter { input, .. }
                        | LogicalPlan::Project { input, .. }
                        | LogicalPlan::SubqueryAlias { input, .. } => {
                            chain.push(base);
                            base = input;
                        }
                        _ => break,
                    }
                }
                let (child, child_schema) = self.lower_node(base)?;
                chain.reverse();
                let (program, output_schema) =
                    PipelineCompiler::compile_recursive_transforms(&chain, &child_schema)?;
                let output_schema = Arc::new(output_schema);
                Ok((
                    Arc::new(PhysicalPlanNode {
                        output_schema: Arc::clone(&output_schema),
                        operator: PhysicalPlanOperator::Transform {
                            input: child,
                            program: Arc::new(program),
                        },
                    }),
                    (*output_schema).clone(),
                ))
            }
            _ => Err(VeinError::unsupported(
                format!(
                    "logical operator '{}' is not supported by recursive physical lowering",
                    logical_node_name(plan)
                ),
                None,
            )),
        }
    }
}

fn logical_node_name(plan: &LogicalPlan) -> &'static str {
    match plan {
        LogicalPlan::Scan { .. } => "scan",
        LogicalPlan::ExternalScan { .. } => "external scan",
        LogicalPlan::Filter { .. } => "filter",
        LogicalPlan::Project { .. } => "projection",
        LogicalPlan::Aggregate { .. } => "aggregate",
        LogicalPlan::Window { .. } => "window",
        LogicalPlan::Join { .. } => "join",
        LogicalPlan::Sort { .. } => "sort",
        LogicalPlan::Limit { .. } => "limit",
        LogicalPlan::Distinct { .. } => "distinct",
        LogicalPlan::SubqueryAlias { .. } => "subquery alias",
        LogicalPlan::Explain { .. } => "explain",
        LogicalPlan::SetOp { .. } => "set operation",
        LogicalPlan::Sample { .. } => "sample",
        LogicalPlan::Assert { .. } => "assert",
        LogicalPlan::Pivot { .. } => "pivot",
        LogicalPlan::Unpivot { .. } => "unpivot",
        LogicalPlan::Fork { .. } => "fork",
        LogicalPlan::Tee { .. } => "tee",
        LogicalPlan::SemanticFilter { .. } => "semantic filter",
        LogicalPlan::SemanticJoin { .. } => "semantic join",
        LogicalPlan::SemanticCalibrate { .. } => "semantic calibration",
        LogicalPlan::Write { .. } => "write",
        LogicalPlan::Call { .. } => "call",
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

/// Extract all supported direct-column equalities that compare probe and build
/// inputs. Other conjuncts remain residual predicates, so same-side equalities,
/// expression equalities, and non-equality conditions still participate in the
/// complete `ON` test. A hash join requires at least one extracted key; joins
/// with only residual conditions need a different physical algorithm.
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
                let probe_type = probe.fields[probe_key as usize].data_type.hash_join_key_type();
                let build_type = build.fields[build_key as usize].data_type.hash_join_key_type();
                if probe_type.is_some() && probe_type == build_type {
                    probe_keys.push(probe_key);
                    build_keys.push(build_key);
                    continue;
                }
            }
        }
        residual.push((*conjunct).clone());
    }

    if probe_keys.is_empty() {
        return Err(VeinError::unsupported(
            "hash join requires at least one equality between direct columns from opposite inputs with matching supported hash-key types; expression-only and non-equi joins are not supported",
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
        return Err(VeinError::unsupported(
            "join keys must be columns; expressions and constant keys are not supported yet",
            None,
        ));
    };
    let left_probe = probe.position_of_id(left.id);
    let left_build = build.position_of_id(left.id);
    let right_probe = probe.position_of_id(right.id);
    let right_build = build.position_of_id(right.id);

    match ((left_probe, left_build), (right_probe, right_build)) {
        ((Some(probe_key), None), (None, Some(build_key)))
        | ((None, Some(build_key)), (Some(probe_key), None)) => {
            Ok((probe_key as u32, build_key as u32))
        }
        _ => {
            let describe_schema = |schema: &VeinSchema| {
                schema
                    .fields
                    .iter()
                    .map(|field| format!("{}#{}", field.name, field.id.0))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            Err(VeinError::planning(
                format!(
                    "join keys '{}#{}' and '{}#{}' must resolve to one column from each input; \
                     check ON or USING against probe schema [{}] and build schema [{}]",
                    left.name,
                    left.id.0,
                    right.name,
                    right.id.0,
                    describe_schema(probe),
                    describe_schema(build),
                ),
                None,
            ))
        }
    }
}

fn position(schema: &VeinSchema, name: &str) -> Result<u32> {
    schema
        .position_by_name(name)
        .map(|index| index as u32)
        .ok_or_else(|| {
            VeinError::planning(
                format!("join key column '{name}' is not in the input"),
                None,
            )
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
            let index = u32::try_from(index)
                .map_err(|_| VeinError::planning("join probe input has too many columns", None))?;
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
            let index = u32::try_from(index)
                .map_err(|_| VeinError::planning("join build input has too many columns", None))?;
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
                return Err(VeinError::unsupported(
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
                    format!(
                        "join output column {} comes from neither input",
                        column.name
                    ),
                    None,
                ))
            }
        })
        .collect()
}

/// The position of `field` in `schema`, matched by name and qualifier.
#[allow(dead_code)]
fn side_of(schema: &VeinSchema, field: &plan::schema::ResolvedColumnRef) -> Option<usize> {
    schema.fields.iter().position(|candidate| {
        candidate.name == field.name && candidate.qualifier == field.qualifier
    })
}

impl PipelineStageBuilder for DefaultPipelineStageBuilder {
    fn build(&self, plan: &LogicalPlan) -> Result<Stage> {
        let (input, sink) = match plan {
            LogicalPlan::Write {
                input,
                target,
                options,
                schema,
            } => {
                let (input, input_schema) = self.lower_node(input)?;
                let output_schema = Arc::new((**schema).clone());
                if input_schema.fields != output_schema.fields {
                    return Err(VeinError::planning(
                        "WRITE output schema does not match its input schema",
                        None,
                    ));
                }
                let key_values = options
                    .iter()
                    .map(|(key, value)| common::protocol::KeyValue {
                        key: key.clone(),
                        value: value.clone(),
                    })
                    .collect();
                // Build Arrow IPC only for the storage sink; client reads do
                // not need a second schema representation.
                let arrow_schema = output_schema.to_arrow();
                let mut schema_bytes = Vec::new();
                {
                    let mut writer =
                        arrow::ipc::writer::StreamWriter::try_new(&mut schema_bytes, &arrow_schema)
                            .map_err(|error| {
                                VeinError::internal(format!("encode WRITE schema: {error}"))
                            })?;
                    writer.finish().map_err(|error| {
                        VeinError::internal(format!("finish WRITE schema: {error}"))
                    })?;
                }
                let target =
                    common::protocol::StorageTarget::new(target.clone(), key_values, output_schema)
                        .with_schema_ipc(schema_bytes);
                (input, DataSink::Storage(target))
            }
            _ => {
                let (input, schema) = self.lower_node(plan)?;
                (
                    input,
                    DataSink::Client {
                        schema: Arc::new(schema),
                    },
                )
            }
        };

        let sink = Arc::new(sink);
        let canonical_output_schema = match sink.as_ref() {
            DataSink::Client { schema } => Arc::clone(schema),
            DataSink::Storage(target) => Arc::clone(&target.schema),
            DataSink::Discard => Arc::new(VeinSchema::empty()),
        };
        let canonical_root = Arc::new(PhysicalPlanNode {
            output_schema: canonical_output_schema,
            operator: PhysicalPlanOperator::Sink {
                input: Arc::clone(&input),
                sink: Arc::clone(&sink),
            },
        });
        let pipeline_graph = DerivedPipelineGraph::derive(&PhysicalPlan {
            root: Arc::clone(&canonical_root),
        })?;
        Ok(Stage::new(StageId::new())
            .with_pipeline_graph(pipeline_graph)
            .with_task(Task::new(
                TaskId::new(),
                PartitionId::from_raw(0),
                PhysicalPlan {
                    root: canonical_root,
                },
            )))
    }

    fn compile_pipeline(
        &self,
        plan: &LogicalPlan,
    ) -> Result<(DataSource, MicroProgram, VeinSchema, VeinSchema)> {
        PipelineCompiler::compile(plan)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DefaultPipelineStageBuilder, PipelineStageBuilder, available_join_outputs, equality_keys,
        extract_equi_join_keys, join_kind,
    };
    use common::protocol::{DataSink, DataSource, JoinKind, PhysicalPlanOperator};
    use parser::ast::expr::BinaryOp;
    use parser::ast::pipe_op::JoinType;
    use plan::schema::{ColumnId, ResolvedColumn, VeinDataType, VeinSchema};
    use plan::{LogicalExpr, LogicalLiteral, LogicalPlan, SortKey as LogicalSortKey};
    use std::sync::Arc;

    fn test_column(id: usize, name: &str) -> Arc<ResolvedColumn> {
        Arc::new(ResolvedColumn::new(
            ColumnId(id),
            name,
            None,
            VeinDataType::Int64,
        ))
    }

    #[test]
    fn parser_join_flavors_map_to_their_physical_kinds() {
        let mappings = [
            (JoinType::Inner, JoinKind::Inner),
            (JoinType::Left, JoinKind::Left),
            (JoinType::Right, JoinKind::Right),
            (JoinType::Full, JoinKind::Full),
            (JoinType::Semi, JoinKind::Semi),
            (JoinType::Anti, JoinKind::Anti),
            (JoinType::Single, JoinKind::Single),
            (JoinType::Cross, JoinKind::Cross),
        ];

        for (logical, physical) in mappings {
            assert_eq!(join_kind(&logical), physical);
        }
    }

    fn equality(left: Arc<ResolvedColumn>, right: Arc<ResolvedColumn>) -> LogicalExpr {
        LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(left)),
            op: parser::ast::expr::BinaryOp::Eq,
            right: Box::new(LogicalExpr::Column(right)),
            return_type: VeinDataType::Boolean,
        }
    }

    fn join_key_schemas() -> (
        VeinSchema,
        VeinSchema,
        Arc<ResolvedColumn>,
        Arc<ResolvedColumn>,
    ) {
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
    fn invalid_join_key_reports_a_planning_contract_error() {
        let probe = VeinSchema::new(vec![test_column(10, "probe_key")]);
        let build = VeinSchema::new(vec![test_column(20, "build_key")]);
        let predicate = equality(test_column(99, "missing_key"), test_column(20, "build_key"));

        let error = equality_keys(Some(&predicate), &probe, &build)
            .expect_err("a join key absent from both inputs must fail planning");

        assert_eq!(error.category(), error::ErrorCategory::Planning);
        let message = error.to_string();
        assert!(message.contains("missing_key#99"), "{message}");
        assert!(message.contains("build_key#20"), "{message}");
        assert!(message.contains("probe schema [probe_key#10]"), "{message}");
        assert!(message.contains("build schema [build_key#20]"), "{message}");
        assert!(message.contains("check ON or USING"), "{message}");
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

        let (outputs, output_schema) = available_join_outputs(&declared_output, &probe, &build)
            .expect("a narrowed join output should map successfully");

        assert_eq!(outputs.len(), 1);
        assert!(matches!(outputs[0], common::protocol::JoinOutput::Probe(1)));
        assert_eq!(
            output_schema
                .fields
                .iter()
                .map(|field| field.id)
                .collect::<Vec<_>>(),
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
    fn rejects_a_join_condition_without_a_supported_hash_key_type() {
        let probe_key = Arc::new(ResolvedColumn::new(
            ColumnId(31),
            "probe_key",
            None,
            VeinDataType::Int32,
        ));
        let build_key = Arc::new(ResolvedColumn::new(
            ColumnId(41),
            "build_key",
            None,
            VeinDataType::Int32,
        ));
        let probe = VeinSchema::new(vec![Arc::clone(&probe_key)]);
        let build = VeinSchema::new(vec![Arc::clone(&build_key)]);
        let conjunct = equality(probe_key, build_key);

        let error = extract_equi_join_keys(&[&conjunct], &probe, &build)
            .expect_err("an unsupported equality cannot be handed to the hash table");

        assert!(error.to_string().contains("matching supported hash-key type"));
    }

    #[test]
    fn unsupported_equality_remains_residual_when_a_supported_hash_key_exists() {
        let probe_key = test_column(11, "probe_key");
        let build_key = test_column(21, "build_key");
        let probe_i32 = Arc::new(ResolvedColumn::new(
            ColumnId(12),
            "probe_i32",
            None,
            VeinDataType::Int32,
        ));
        let build_i32 = Arc::new(ResolvedColumn::new(
            ColumnId(22),
            "build_i32",
            None,
            VeinDataType::Int32,
        ));
        let probe = VeinSchema::new(vec![Arc::clone(&probe_key), Arc::clone(&probe_i32)]);
        let build = VeinSchema::new(vec![Arc::clone(&build_key), Arc::clone(&build_i32)]);
        let supported = equality(probe_key, build_key);
        let residual = equality(probe_i32, build_i32);

        let ((probe_keys, build_keys), residuals) =
            extract_equi_join_keys(&[&supported, &residual], &probe, &build)
                .expect("the supported equality supplies the hash key");

        assert_eq!(probe_keys, vec![0]);
        assert_eq!(build_keys, vec![0]);
        assert_eq!(residuals, vec![residual]);
    }

    #[test]
    fn expression_equality_remains_residual_when_a_supported_hash_key_exists() {
        let probe_key = test_column(11, "probe_key");
        let probe_value = test_column(12, "probe_value");
        let build_key = test_column(21, "build_key");
        let build_value = test_column(22, "build_value");
        let probe = VeinSchema::new(vec![Arc::clone(&probe_key), Arc::clone(&probe_value)]);
        let build = VeinSchema::new(vec![Arc::clone(&build_key), Arc::clone(&build_value)]);
        let supported = equality(Arc::clone(&probe_key), Arc::clone(&build_key));
        let expression_equality = LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(probe_value)),
                op: BinaryOp::Plus,
                right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(1))),
                return_type: VeinDataType::Int64,
            }),
            op: BinaryOp::Eq,
            right: Box::new(LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(build_value)),
                op: BinaryOp::Plus,
                right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(1))),
                return_type: VeinDataType::Int64,
            }),
            return_type: VeinDataType::Boolean,
        };

        let ((probe_keys, build_keys), residuals) = extract_equi_join_keys(
            &[&supported, &expression_equality],
            &probe,
            &build,
        )
            .expect("the direct equality supplies a hash key");

        assert_eq!(probe_keys, vec![0]);
        assert_eq!(build_keys, vec![0]);
        assert_eq!(residuals, vec![expression_equality]);
    }

    #[test]
    fn rejects_expression_only_join_equality_with_supported_boundary_error() {
        let probe_value = test_column(12, "probe_value");
        let build_value = test_column(22, "build_value");
        let probe = VeinSchema::new(vec![Arc::clone(&probe_value)]);
        let build = VeinSchema::new(vec![Arc::clone(&build_value)]);
        let expression_equality = LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(probe_value)),
                op: BinaryOp::Plus,
                right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(1))),
                return_type: VeinDataType::Int64,
            }),
            op: BinaryOp::Eq,
            right: Box::new(LogicalExpr::Column(build_value)),
            return_type: VeinDataType::Boolean,
        };

        let error = extract_equi_join_keys(&[&expression_equality], &probe, &build)
            .expect_err("expression-only equality cannot run as a hash join");

        let message = error.to_string();
        assert!(message.contains("direct columns from opposite inputs"), "{message}");
        assert!(message.contains("expression-only and non-equi joins are not supported"), "{message}");
    }

    #[test]
    fn rejects_pure_non_equi_join_with_supported_boundary_error() {
        let probe_value = test_column(12, "probe_value");
        let build_value = test_column(22, "build_value");
        let probe = VeinSchema::new(vec![Arc::clone(&probe_value)]);
        let build = VeinSchema::new(vec![Arc::clone(&build_value)]);
        let non_equi = LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(probe_value)),
            op: BinaryOp::Gt,
            right: Box::new(LogicalExpr::Column(build_value)),
            return_type: VeinDataType::Boolean,
        };

        let error = extract_equi_join_keys(&[&non_equi], &probe, &build)
            .expect_err("pure non-equi conditions need a non-hash join algorithm");

        let message = error.to_string();
        assert!(message.contains("direct columns from opposite inputs"), "{message}");
        assert!(message.contains("expression-only and non-equi joins are not supported"), "{message}");
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

        assert_eq!(error.category(), error::ErrorCategory::Planning);
        let message = error.to_string();
        assert!(message.contains("key#11"), "{message}");
        assert!(message.contains("must resolve to one column from each input"), "{message}");
        assert!(message.contains("probe schema [other#10, key#11]"), "{message}");
        assert!(message.contains("build schema [key#21, other#22]"), "{message}");
    }

    #[test]
    fn builds_recursive_sort_and_limit_nodes_without_duplicate_task_windows() {
        let probe_column = test_column(1, "probe_key");
        let build_column = test_column(2, "build_key");
        let probe_schema = VeinSchema::new(vec![Arc::clone(&probe_column)]);
        let build_schema = VeinSchema::new(vec![Arc::clone(&build_column)]);
        let output_schema =
            VeinSchema::new(vec![Arc::clone(&probe_column), Arc::clone(&build_column)]);
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
            on: Some(equality(
                Arc::clone(&probe_column),
                Arc::clone(&build_column),
            )),
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

            let PhysicalPlanOperator::Sink { input, .. } = &task.plan.root.operator else {
                panic!("canonical plan must have a sink root");
            };
            match (&plan, &input.operator) {
                (LogicalPlan::Limit { .. }, PhysicalPlanOperator::Limit { input, .. }) => {
                    assert!(matches!(input.operator, PhysicalPlanOperator::Sort { .. }));
                }
                (LogicalPlan::Sort { .. }, PhysicalPlanOperator::Sort { input, .. }) => {
                    assert!(matches!(input.operator, PhysicalPlanOperator::Limit { .. }));
                }
                _ => panic!("root operator order must be preserved by recursive plan"),
            }
        }
    }

    #[test]
    fn recursive_lowering_fuses_adjacent_transforms_above_join() {
        let probe = test_column(1, "probe_key");
        let build = test_column(2, "build_value");
        let probe_schema = VeinSchema::new(vec![Arc::clone(&probe)]);
        let build_schema = VeinSchema::new(vec![Arc::clone(&build)]);
        let joined_schema = VeinSchema::new(vec![Arc::clone(&probe), Arc::clone(&build)]);
        let scan = |name: &str, schema: &VeinSchema| LogicalPlan::ExternalScan {
            url: name.to_owned(),
            options: Vec::new(),
            files: Vec::new(),
            schema: Box::new(schema.clone()),
        };
        let join = LogicalPlan::Join {
            join_type: parser::ast::pipe_op::JoinType::Inner,
            left: Box::new(scan("probe", &probe_schema)),
            right: Box::new(scan("build", &build_schema)),
            on: Some(equality(Arc::clone(&probe), Arc::clone(&build))),
            using: Vec::new(),
            schema: Box::new(joined_schema.clone()),
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(join),
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(Arc::clone(&probe))),
                op: parser::ast::expr::BinaryOp::Gt,
                right: Box::new(LogicalExpr::Literal(plan::LogicalLiteral::Int64(0))),
                return_type: VeinDataType::Boolean,
            },
        };
        let project = LogicalPlan::Project {
            input: Box::new(filter),
            exprs: vec![LogicalExpr::Column(Arc::clone(&build))],
            schema: Box::new(VeinSchema::new(vec![Arc::clone(&build)])),
        };

        let stage = DefaultPipelineStageBuilder::new()
            .build(&project)
            .expect("row-wise chain above join should lower recursively");
        let task = stage.tasks.first().expect("join task");
        let PhysicalPlanOperator::Sink { input, .. } = &task.plan.root.operator else {
            panic!("canonical plan must have a sink root");
        };
        let PhysicalPlanOperator::Transform {
            input: join,
            program,
        } = &input.operator
        else {
            panic!("adjacent filter and projection should fuse into one transform");
        };
        assert!(matches!(join.operator, PhysicalPlanOperator::Join { .. }));
        assert!(program.instructions.iter().any(|instruction| matches!(
            instruction,
            common::protocol::MicroOp::FilterMask { .. }
        )));
        assert!(program.instructions.iter().any(|instruction| matches!(
            instruction,
            common::protocol::MicroOp::ProjectRegisters { out_regs } if out_regs.len() == 1
        )));
    }

    #[test]
    fn recursive_lowering_fuses_source_filter_and_projection_into_pipeline() {
        let value = test_column(1, "value");
        let input_schema = VeinSchema::new(vec![Arc::clone(&value)]);
        let scan = LogicalPlan::ExternalScan {
            url: "memory://source".to_owned(),
            options: Vec::new(),
            files: Vec::new(),
            schema: Box::new(input_schema.clone()),
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(scan),
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(Arc::clone(&value))),
                op: parser::ast::expr::BinaryOp::Gt,
                right: Box::new(LogicalExpr::Literal(plan::LogicalLiteral::Int64(0))),
                return_type: VeinDataType::Boolean,
            },
        };
        let project = LogicalPlan::Project {
            input: Box::new(filter),
            exprs: vec![LogicalExpr::Column(Arc::clone(&value))],
            schema: Box::new(VeinSchema::new(vec![value])),
        };

        let stage = DefaultPipelineStageBuilder::new()
            .build(&project)
            .expect("row-wise operators over a scan should lower as one source pipeline");
        let task = stage.tasks.first().expect("pipeline task");
        let PhysicalPlanOperator::Sink { input, .. } = &task.plan.root.operator else {
            panic!("canonical plan must have a sink root");
        };
        let PhysicalPlanOperator::Pipeline { program, .. } = &input.operator else {
            panic!("source row-wise operators should remain fused with the scan");
        };
        assert!(program.instructions.iter().any(|instruction| matches!(
            instruction,
            common::protocol::MicroOp::FilterMask { .. }
        )));
        assert!(program.instructions.iter().any(|instruction| matches!(
            instruction,
            common::protocol::MicroOp::ProjectRegisters { out_regs } if out_regs.len() == 1
        )));
    }

    #[test]
    fn recursive_lowering_names_unsupported_logical_operators() {
        let schema = VeinSchema::empty();
        let call = LogicalPlan::Call {
            input: Box::new(LogicalPlan::ExternalScan {
                url: "memory://source".to_owned(),
                options: Vec::new(),
                files: Vec::new(),
                schema: Box::new(schema.clone()),
            }),
            name: "custom_call".to_owned(),
            args: Vec::new(),
            schema: Box::new(schema),
        };

        let error = match DefaultPipelineStageBuilder::new().build(&call) {
            Ok(_) => panic!("CALL has no physical implementation yet"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("operator 'call'"));
        assert!(error.to_string().contains("recursive physical lowering"));
    }

    #[test]
    fn multi_file_scan_keeps_one_canonical_task_and_graph_references() {
        let column = test_column(0, "id");
        let schema = VeinSchema::new(vec![column]);
        let scan = LogicalPlan::Scan {
            table: Arc::new(
                plan::TableMetadata::new("items", Arc::new(schema.clone()))
                    .with_format(plan::TableFormat::Parquet)
                    .with_files(vec!["first.parquet".into(), "second.parquet".into()]),
            ),
            projection: None,
            filters: Vec::new(),
            schema: Box::new(schema),
        };

        let stage = DefaultPipelineStageBuilder::new()
            .build(&scan)
            .expect("multi-file scan should lower to one stage");
        assert_eq!(stage.tasks().len(), 1);
        assert_eq!(stage.pipeline_graph().pipelines().len(), 1);
        assert!(stage.pipeline_graph().dependencies().is_empty());

        let PhysicalPlanOperator::Sink { input, .. } = &stage.tasks()[0].plan.root.operator else {
            panic!("stage task should retain a canonical sink root");
        };
        let PhysicalPlanOperator::Pipeline { source, .. } = &input.operator else {
            panic!("scan should remain one canonical source pipeline");
        };
        let DataSource::File(scan) = source.as_ref() else {
            panic!("scan source should retain its file set");
        };
        assert_eq!(scan.files, vec!["first.parquet", "second.parquet"]);
        assert!(Arc::ptr_eq(
            &stage.pipeline_graph().pipelines()[0].source.node,
            input
        ));
    }

    #[test]
    fn write_over_recursive_join_preserves_storage_sink_metadata() {
        let left_key = test_column(0, "left_key");
        let left_value = Arc::new(ResolvedColumn::new(
            ColumnId(1),
            "left_value",
            None,
            VeinDataType::Utf8,
        ));
        let left_schema = VeinSchema::new(vec![Arc::clone(&left_key), left_value]);
        let left_scan = LogicalPlan::Scan {
            table: Arc::new(
                plan::TableMetadata::new("left_table", Arc::new(left_schema.clone()))
                    .with_format(plan::TableFormat::Parquet)
                    .with_files(vec!["left.parquet".to_string()]),
            ),
            projection: None,
            filters: Vec::new(),
            schema: Box::new(left_schema.clone()),
        };

        let right_key = test_column(2, "right_key");
        let right_value = Arc::new(ResolvedColumn::new(
            ColumnId(3),
            "right_value",
            None,
            VeinDataType::Utf8,
        ));
        let right_schema = VeinSchema::new(vec![Arc::clone(&right_key), right_value]);
        let right_scan = LogicalPlan::Scan {
            table: Arc::new(
                plan::TableMetadata::new("right_table", Arc::new(right_schema.clone()))
                    .with_format(plan::TableFormat::Parquet)
                    .with_files(vec!["right.parquet".to_string()]),
            ),
            projection: None,
            filters: Vec::new(),
            schema: Box::new(right_schema.clone()),
        };

        let output_schema = VeinSchema::new(
            left_schema
                .fields
                .iter()
                .chain(&right_schema.fields)
                .cloned()
                .collect(),
        );
        let join = LogicalPlan::Join {
            join_type: JoinType::Inner,
            left: Box::new(left_scan),
            right: Box::new(right_scan),
            on: Some(equality(left_key, right_key)),
            using: Vec::new(),
            schema: Box::new(output_schema.clone()),
        };
        let write = LogicalPlan::Write {
            input: Box::new(join),
            target: "file:///output/result".to_string(),
            options: vec![
                ("format".to_string(), "parquet".to_string()),
                ("compression".to_string(), "zstd".to_string()),
            ],
            schema: Box::new(output_schema.clone()),
        };

        let stage = DefaultPipelineStageBuilder::new()
            .build(&write)
            .expect("WRITE over a recursively lowered join should build");
        assert_eq!(stage.tasks().len(), 1);
        assert_eq!(stage.pipeline_graph().pipelines().len(), 2);
        assert_eq!(stage.pipeline_graph().dependencies().len(), 1);
        assert_eq!(
            stage.pipeline_graph().execution_order(),
            Some(vec![crate::pipeline_graph::PipelineId(0), crate::pipeline_graph::PipelineId(1)])
        );

        let root = &stage.tasks()[0].plan.root;
        let PhysicalPlanOperator::Sink { input, sink } = &root.operator else {
            panic!("WRITE must lower to an explicit sink root");
        };
        let DataSink::Storage(target) = sink.as_ref() else {
            panic!("WRITE must retain a storage sink instead of a client sink");
        };
        assert_eq!(target.destination_uri, "file:///output/result");
        assert_eq!(target.get_option("format"), Some("parquet"));
        assert_eq!(target.get_option("compression"), Some("zstd"));
        assert_eq!(target.schema.fields, output_schema.fields);
        assert!(!target.schema_ipc.is_empty());
        assert_eq!(root.output_schema.fields, output_schema.fields);
        assert!(matches!(input.operator, PhysicalPlanOperator::Join { .. }));
    }
}
