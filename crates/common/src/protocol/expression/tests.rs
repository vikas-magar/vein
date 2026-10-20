use super::*;

#[test]
fn expression_program_round_trips_typed_filter_and_projection() {
    let column = PhysicalExpr {
        return_type: types::VeinDataType::Int64,
        kind: ExprKind::Column { index: 0 },
    };
    let predicate = PhysicalExpr {
        return_type: types::VeinDataType::Boolean,
        kind: ExprKind::Binary {
            op: BinaryExprOp::Greater,
            left: Box::new(column.clone()),
            right: Box::new(PhysicalExpr {
                return_type: types::VeinDataType::Int64,
                kind: ExprKind::Literal(types::LogicalLiteral::Int64(10)),
            }),
        },
    };
    let program = ExpressionProgram {
        steps: vec![
            ExpressionStep::Filter(predicate),
            ExpressionStep::Projection(vec![column]),
        ],
    };

    let bytes = serde_json::to_vec(&program).expect("serialize expression program");
    let decoded: ExpressionProgram =
        serde_json::from_slice(&bytes).expect("deserialize expression program");

    assert_eq!(decoded, program);
}
