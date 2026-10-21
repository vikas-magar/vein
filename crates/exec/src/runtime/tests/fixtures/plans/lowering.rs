use super::*;
use super::super::executor::InputPlanFixture;

pub(in crate::runtime::tests) fn schema_for_types(type_list: &[types::VeinDataType]) -> Arc<types::VeinSchema> {
    Arc::new(types::VeinSchema::new(
        type_list
            .iter()
            .enumerate()
            .map(|(index, data_type)| {
                Arc::new(types::ResolvedColumn::new(
                    types::ColumnId(index),
                    format!("column_{index}"),
                    None,
                    data_type.clone(),
                ))
            })
            .collect(),
    ))
}

pub(in crate::runtime::tests) fn int64_schema() -> Arc<types::VeinSchema> {
    schema_for_types(&[types::VeinDataType::Int64])
}

pub(in crate::runtime::tests) fn plan_node(
    output_schema: Arc<types::VeinSchema>,
    operator: PhysicalPlanOperator,
) -> Arc<PhysicalPlanNode> {
    Arc::new(PhysicalPlanNode {
        output_schema,
        operator,
    })
}

pub(in crate::runtime::tests) fn canonical_input(input: InputPlanFixture) -> Arc<PhysicalPlanNode> {
    match input {
        InputPlanFixture::Pipeline { source, program } => {
            let input_schema = int64_schema();
            plan_node(
                Arc::clone(&input_schema),
                PhysicalPlanOperator::Pipeline {
                    source: Arc::new(source),
                    input_schema,
                    program,
                },
            )
        }
        InputPlanFixture::Transform { input, program } => {
            let input = canonical_input(*input);
            let projected = program.steps.iter().rev().find_map(|step| {
                if let ExpressionStep::Projection(expressions) = step {
                    Some(expressions)
                } else {
                    None
                }
            });
            let output_schema = if let Some(expressions) = projected {
                Arc::new(types::VeinSchema::new(
                    expressions
                        .iter()
                        .enumerate()
                        .map(|(index, expression)| {
                            if let ExprKind::Column { index: column } = expression.kind {
                                let column = usize::try_from(column)
                                    .expect("test projection column fits address space");
                                Arc::clone(input.output_schema.fields.get(column)
                                    .expect("test projection selects an input column"))
                            } else {
                                Arc::new(types::ResolvedColumn::new(
                                    types::ColumnId(index), format!("expression_{index}"), None,
                                    expression.return_type.clone(),
                                ))
                            }
                        })
                        .collect(),
                ))
            } else {
                Arc::clone(&input.output_schema)
            };
            plan_node(
                output_schema,
                PhysicalPlanOperator::Transform { input, program },
            )
        }
        InputPlanFixture::Aggregate { input, plan } => {
            let output_schema = schema_for_types(&plan.types);
            plan_node(
                output_schema,
                PhysicalPlanOperator::Aggregate {
                    input: canonical_input(*input),
                    plan,
                },
            )
        }
        InputPlanFixture::Distinct { input, plan } => {
            let output_schema = schema_for_types(&plan.types);
            plan_node(
                output_schema,
                PhysicalPlanOperator::Distinct {
                    input: canonical_input(*input),
                    plan,
                },
            )
        }
        InputPlanFixture::Join { probe, build, plan } => {
            let probe = canonical_input(*probe);
            let build = canonical_input(*build);
            let output_schema = Arc::new(types::VeinSchema::new(
                plan.outputs
                    .iter()
                    .map(|column| {
                        let (field, null_extended) = match column {
                            JoinOutput::Probe(index) => (
                                Arc::clone(
                                    probe
                                        .output_schema
                                        .fields
                                        .get(*index as usize)
                                        .expect("test join probe output exists"),
                                ),
                                matches!(plan.join_type, JoinKind::Right | JoinKind::Full),
                            ),
                            JoinOutput::Build(index) => (
                                Arc::clone(
                                    build
                                        .output_schema
                                        .fields
                                        .get(*index as usize)
                                        .expect("test join build output exists"),
                                ),
                                matches!(
                                        plan.join_type,
                                        JoinKind::Left | JoinKind::Single | JoinKind::Full
                                    ),
                            ),
                        };
                        if null_extended && !field.nullable {
                            Arc::new(field.as_ref().clone().with_nullable(true))
                        } else {
                            field
                        }
                    })
                    .collect(),
            ));
            plan_node(
                output_schema,
                PhysicalPlanOperator::Join { probe, build, plan },
            )
        }
        InputPlanFixture::Sort {
            input,
            plan,
            key_program,
            output_columns,
        } => {
            let input = canonical_input(*input);
            plan_node(
                Arc::clone(&input.output_schema),
                PhysicalPlanOperator::Sort {
                    input,
                    plan,
                    key_program,
                    output_columns,
                },
            )
        }
        InputPlanFixture::Limit { input, spec } => {
            let input = canonical_input(*input);
            plan_node(
                Arc::clone(&input.output_schema),
                PhysicalPlanOperator::Limit { input, spec },
            )
        }
    }
}
