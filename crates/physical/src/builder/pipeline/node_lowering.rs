use std::sync::Arc;

use common::Result;
use common::protocol::{LimitSpec, PhysicalPlanNode, PhysicalPlanOperator};
use error::VeinError;
use plan::schema::VeinSchema;
use plan::LogicalPlan;

use super::{
    DefaultPipelineStageBuilder, PipelineCompiler, PipelineStageBuilder, logical_node_name,
};

impl DefaultPipelineStageBuilder {
    /// A scan and its adjacent row-wise operators form one source VM segment.
    /// Keeping this segment fused avoids materializing intermediate batches.
    fn is_source_pipeline(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Scan { .. }
            | LogicalPlan::ExternalScan { .. }
            | LogicalPlan::Intermediate { .. } => true,
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::SubqueryAlias { input, .. } => Self::is_source_pipeline(input),
            _ => false,
        }
    }

    /// Lowers every supported relational shape into the canonical physical
    /// tree. Row-wise scan segments are compiled together; recursive operators
    /// retain their logical order and form explicit physical boundaries.
    pub(super) fn lower_node(&self, plan: &LogicalPlan) -> Result<(Arc<PhysicalPlanNode>, VeinSchema)> {
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
                let aggregate = self.aggregate_plan(plan)?.ok_or_else(|| {
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
                let distinct = self.aggregate_plan(plan)?.ok_or_else(|| {
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
                while let LogicalPlan::Filter { input, .. }
                | LogicalPlan::Project { input, .. }
                | LogicalPlan::SubqueryAlias { input, .. } = base
                {
                    chain.push(base);
                    base = input;
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
