use super::*;

impl PhysicalPlan {
    /// Split this canonical plan at one node for adaptive execution.
    ///
    /// The producer retains the selected subtree and writes its output through a
    /// client-shaped sink, which can be replaced by the checkpoint sink by the
    /// coordinator. The continuation preserves the surrounding immutable
    /// operator tree and replaces the selected subtree with an intermediate
    /// source. It returns `None` if the node is absent, is the output sink, or is
    /// referenced more than once in the plan.
    pub fn split_at_checkpoint(
        &self,
        target: &Arc<PhysicalPlanNode>,
        source_id: IntermediateSourceId,
    ) -> Option<CheckpointPlanFragments> {
        if Arc::ptr_eq(&self.root, target)
            || matches!(target.operator, PhysicalPlanOperator::Sink { .. })
        {
            return None;
        }

        let source = Arc::new(PhysicalPlanNode {
            output_schema: Arc::clone(&target.output_schema),
            operator: PhysicalPlanOperator::Pipeline {
                source: Arc::new(DataSource::Intermediate(source_id)),
                input_schema: Arc::clone(&target.output_schema),
                program: Arc::new(ExpressionProgram::empty()),
            },
        });
        let (continuation_root, replacements) = replace_plan_node(&self.root, target, &source);
        if replacements != 1 {
            return None;
        }

        let producer = PhysicalPlan {
            root: Arc::new(PhysicalPlanNode {
                output_schema: Arc::clone(&target.output_schema),
                operator: PhysicalPlanOperator::Sink {
                    input: Arc::clone(target),
                    sink: Arc::new(DataSink::Client {
                        schema: Arc::clone(&target.output_schema),
                    }),
                },
            }),
        };
        Some(CheckpointPlanFragments {
            producer,
            continuation: PhysicalPlan {
                root: continuation_root,
            },
        })
    }
}

fn replace_plan_node(
    current: &Arc<PhysicalPlanNode>,
    target: &Arc<PhysicalPlanNode>,
    replacement: &Arc<PhysicalPlanNode>,
) -> (Arc<PhysicalPlanNode>, usize) {
    if Arc::ptr_eq(current, target) {
        return (Arc::clone(replacement), 1);
    }

    let (operator, replacements) = match &current.operator {
        PhysicalPlanOperator::Extension { descriptor, inputs } => {
            let mut replacements = 0;
            let inputs = inputs.iter().map(|input| {
                let (input, count) = replace_plan_node(input, target, replacement);
                replacements += count;
                input
            }).collect::<Vec<_>>().into();
            (PhysicalPlanOperator::Extension { descriptor: Arc::clone(descriptor), inputs }, replacements)
        }
        PhysicalPlanOperator::Pipeline {
            source,
            input_schema,
            program,
        } => (
            PhysicalPlanOperator::Pipeline {
                source: Arc::clone(source),
                input_schema: Arc::clone(input_schema),
                program: Arc::clone(program),
            },
            0,
        ),
        PhysicalPlanOperator::Transform { input, program } => {
            let (input, count) = replace_plan_node(input, target, replacement);
            (
                PhysicalPlanOperator::Transform {
                    input,
                    program: Arc::clone(program),
                },
                count,
            )
        }
        PhysicalPlanOperator::Join { probe, build, plan } => {
            let (probe, probe_count) = replace_plan_node(probe, target, replacement);
            let (build, build_count) = replace_plan_node(build, target, replacement);
            (
                PhysicalPlanOperator::Join {
                    probe,
                    build,
                    plan: plan.clone(),
                },
                probe_count + build_count,
            )
        }
        PhysicalPlanOperator::Aggregate { input, plan } => {
            let (input, count) = replace_plan_node(input, target, replacement);
            (
                PhysicalPlanOperator::Aggregate {
                    input,
                    plan: plan.clone(),
                },
                count,
            )
        }
        PhysicalPlanOperator::Distinct { input, plan } => {
            let (input, count) = replace_plan_node(input, target, replacement);
            (
                PhysicalPlanOperator::Distinct {
                    input,
                    plan: plan.clone(),
                },
                count,
            )
        }
        PhysicalPlanOperator::Sort {
            input,
            plan,
            key_program,
            output_columns,
        } => {
            let (input, count) = replace_plan_node(input, target, replacement);
            (
                PhysicalPlanOperator::Sort {
                    input,
                    plan: plan.clone(),
                    key_program: key_program.clone(),
                    output_columns: *output_columns,
                },
                count,
            )
        }
        PhysicalPlanOperator::Limit { input, spec } => {
            let (input, count) = replace_plan_node(input, target, replacement);
            (PhysicalPlanOperator::Limit { input, spec: *spec }, count)
        }
        PhysicalPlanOperator::Sink { input, sink } => {
            let (input, count) = replace_plan_node(input, target, replacement);
            (
                PhysicalPlanOperator::Sink {
                    input,
                    sink: Arc::clone(sink),
                },
                count,
            )
        }
    };

    (
        Arc::new(PhysicalPlanNode {
            output_schema: Arc::clone(&current.output_schema),
            operator,
        }),
        replacements,
    )
}
