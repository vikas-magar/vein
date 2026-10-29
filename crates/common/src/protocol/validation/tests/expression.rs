use super::*;
#[test]
fn validates_recursive_passthrough_and_sink_schema_contracts() {
    let schema = schema("value");
    let source = pipeline(Arc::clone(&schema), ExpressionProgram::empty());
    let limited = node(
        Arc::clone(&schema),
        PhysicalPlanOperator::Limit {
            input: source,
            spec: LimitSpec {
                offset: 2,
                count: 5,
            },
        },
    );

    client_plan(limited, schema)
        .validate()
        .expect("valid recursive plan should pass schema validation");
}

#[test]
fn accepts_empty_expression_pipeline_as_input_passthrough() {
    let schema = schema("value");
    let source = pipeline(Arc::clone(&schema), ExpressionProgram::empty());

    client_plan(source, Arc::clone(&schema))
        .validate()
        .expect("an empty expression pipeline passes through its input");
}

#[test]
fn validates_filter_and_projection_expression_steps() {
    let schema = schema("value");
    let input = PhysicalExpr {
        return_type: VeinDataType::Int64,
        kind: ExprKind::Column { index: 0 },
    };
    let predicate = PhysicalExpr {
        return_type: VeinDataType::Boolean,
        kind: ExprKind::Binary {
            op: crate::protocol::BinaryExprOp::Greater,
            left: Box::new(input.clone()),
            right: Box::new(PhysicalExpr {
                return_type: VeinDataType::Int64,
                kind: ExprKind::Literal(types::LogicalLiteral::Int64(0)),
            }),
        },
    };
    let source = pipeline(
        Arc::clone(&schema),
        ExpressionProgram {
            steps: vec![
                ExpressionStep::Filter(predicate),
                ExpressionStep::Projection(vec![input]),
            ],
        },
    );

    client_plan(source, schema)
        .validate()
        .expect("typed filter and projection steps preserve their ordered schema");
}

#[test]
fn rejects_expression_column_outside_the_current_schema() {
    let schema = schema("available_column");
    let source = pipeline(
        Arc::clone(&schema),
        ExpressionProgram {
            steps: vec![ExpressionStep::Projection(vec![PhysicalExpr {
                return_type: VeinDataType::Int64,
                kind: ExprKind::Column { index: 1 },
            }])],
        },
    );

    let error = client_plan(source, schema)
        .validate()
        .expect_err("an expression cannot refer outside the current input schema");
    let message = error.to_string();
    assert!(message.contains("root.input.steps[0].projection[0]"), "{message}");
    assert!(message.contains("column 1 is outside schema width 1"), "{message}");
}
