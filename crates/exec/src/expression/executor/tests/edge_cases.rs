use super::*;
use arrow::array::{Int32Array, StringArray};

#[tokio::test]
async fn all_false_filter_emits_no_batch() {
    let (batch, _) = input();
    let predicate = PhysicalExpr {
        kind: ExprKind::Binary {
            op: BinaryExprOp::Greater,
            left: Box::new(column(0)),
            right: Box::new(literal(3)),
        },
        return_type: VeinDataType::Boolean,
    };
    let output = run(ExpressionProgram { steps: vec![ExpressionStep::Filter(predicate)] }, batch).await;

    assert!(output.is_empty());
}

#[tokio::test]
async fn projection_cast_runs_inside_the_batch_processor() {
    let (batch, _) = input();
    let cast = PhysicalExpr {
        kind: ExprKind::Cast {
            expr: Box::new(column(0)),
            target_type: VeinDataType::Int32,
        },
        return_type: VeinDataType::Int32,
    };
    let output = run(ExpressionProgram { steps: vec![ExpressionStep::Projection(vec![cast])] }, batch).await;
    let [view] = output.as_slice() else { panic!("expected one cast result") };
    let result = view.materialize_column(0).unwrap();

    assert_eq!(result.as_any().downcast_ref::<Int32Array>().unwrap().values(), &[1, 2, 3]);
}

#[tokio::test]
async fn registered_scalar_function_runs_inside_the_batch_processor() {
    let (batch, _) = input();
    let source = batch.source_arrays_for_column(1).unwrap()[0].clone();
    let lower = PhysicalExpr {
        kind: ExprKind::Call {
            name: "LOWER".into(),
            args: vec![PhysicalExpr {
                kind: ExprKind::Column { index: 1 },
                return_type: VeinDataType::Utf8,
            }],
        },
        return_type: VeinDataType::Utf8,
    };
    let output = run_with_functions(
        ExpressionProgram {
            steps: vec![ExpressionStep::Projection(vec![
                PhysicalExpr {
                    kind: ExprKind::Column { index: 1 },
                    return_type: VeinDataType::Utf8,
                },
                lower,
            ])],
        },
        batch,
        Arc::new(FunctionRegistry::with_builtins()),
    ).await;
    let [view] = output.as_slice() else { panic!("expected one function result") };
    let result = view.materialize_column(1).unwrap();

    assert_eq!(result.as_any().downcast_ref::<StringArray>().unwrap(), &StringArray::from(vec![Some("a"), None, Some("c")]));
    assert!(Arc::ptr_eq(&source, &view.source_arrays_for_column(0).unwrap()[0]));
}
