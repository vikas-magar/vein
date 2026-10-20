use super::bind;
use plan::{LogicalExpr, LogicalPlan, VeinDataType};

#[tokio::test]
async fn decimal_literal_coerces_to_float64_aggregate_result() {
    let plan = bind(
        "FROM decimal_values |> AGGREGATE AVG(amount) * 0.1 AS scaled_average",
    )
        .await;
    let LogicalPlan::Project { exprs, schema, .. } = plan else {
        panic!("expected scalar projection over aggregate results");
    };

    assert_eq!(schema.fields[0].data_type, VeinDataType::Float64);
    let LogicalExpr::BinaryOp { left, right, .. } = &exprs[0] else {
        panic!("expected arithmetic expression");
    };
    assert_eq!(left.data_type(), VeinDataType::Float64);
    assert!(matches!(
        right.as_ref(),
        LogicalExpr::Cast { target_type: VeinDataType::Float64, .. }
    ));
}

#[tokio::test]
async fn null_literal_preserves_decimal_average_result_type_and_nullability() {
    let plan = bind("FROM decimal_values |> AGGREGATE AVG(amount) * NULL AS scaled_average").await;
    let LogicalPlan::Project { exprs, schema, .. } = plan else {
        panic!("expected scalar projection over aggregate results");
    };

    assert_eq!(schema.fields[0].data_type, VeinDataType::Float64);
    assert!(schema.fields[0].nullable, "multiplication by NULL yields NULL");
    let LogicalExpr::BinaryOp { right, .. } = &exprs[0] else {
        panic!("expected arithmetic expression");
    };
    assert_eq!(right.data_type(), VeinDataType::Null);
}

#[tokio::test]
async fn decimal_division_binds_float64_casts_for_both_operands() {
    let plan = bind("FROM decimal_values |> SELECT amount / 7.0 AS ratio").await;
    let LogicalPlan::Project { exprs, schema, .. } = plan else {
        panic!("expected a projection for decimal division");
    };

    assert_eq!(schema.fields[0].data_type, VeinDataType::Float64);
    let LogicalExpr::BinaryOp { left, right, .. } = &exprs[0] else {
        panic!("expected a binary division expression");
    };
    for operand in [left.as_ref(), right.as_ref()] {
        assert!(matches!(
            operand,
            LogicalExpr::Cast {
                target_type: VeinDataType::Float64,
                ..
            }
        ));
    }
}
