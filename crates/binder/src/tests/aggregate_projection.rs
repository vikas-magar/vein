use super::{assert_chain, bind};
use plan::{LogicalExpr, LogicalPlan};

#[tokio::test]
async fn scalar_expressions_over_aggregates_lower_to_vector_projection() {
    let plan = bind(
        "FROM users |> AGGREGATE SUM(age) * 2 AS doubled, SUM(id) + COUNT(*) AS combined GROUP BY name",
    )
        .await;
    assert_chain(&plan, &["Project", "Aggregate", "Scan"]);

    let LogicalPlan::Project {
        input,
        exprs: expressions,
        schema,
    } = plan
    else {
        panic!("expected post-aggregate Project");
    };
    let LogicalPlan::Aggregate {
        aggs,
        schema: aggregate_schema,
        ..
    } = *input
    else {
        panic!("expected Aggregate under Project");
    };

    assert_eq!(aggs.len(), 3, "each raw aggregate is computed once");
    assert_eq!(aggregate_schema.fields.len(), 4, "group key plus raw results");
    assert_eq!(expressions.len(), schema.fields.len());
    assert_eq!(schema.fields.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(), ["name", "doubled", "combined"]);
    assert!(matches!(expressions[0], LogicalExpr::Column(_)));
    assert!(matches!(expressions[1], LogicalExpr::BinaryOp { .. }));
    assert!(matches!(expressions[2], LogicalExpr::BinaryOp { .. }));
}

#[tokio::test]
async fn case_can_reference_multiple_aggregate_results() {
    let plan = bind(
        "FROM users |> AGGREGATE CASE WHEN SUM(age) > 10 THEN SUM(id) ELSE 0 END AS bucket GROUP BY name",
    )
        .await;
    assert_chain(&plan, &["Project", "Aggregate", "Scan"]);

    let LogicalPlan::Project {
        input,
        exprs: expressions,
        ..
    } = plan
    else {
        panic!("expected post-aggregate Project");
    };
    let LogicalPlan::Aggregate { aggs, .. } = *input else {
        panic!("expected Aggregate under Project");
    };
    assert_eq!(aggs.len(), 2);
    assert!(matches!(expressions[1], LogicalExpr::Case { .. }));
}

#[tokio::test]
async fn aggregate_division_preserves_nullable_result_schema() {
    let plan = bind("FROM users |> AGGREGATE SUM(age) / SUM(id) AS ratio").await;
    let LogicalPlan::Project { exprs, schema, .. } = plan else {
        panic!("expected post-aggregate Project");
    };
    assert!(matches!(exprs[0], LogicalExpr::BinaryOp { .. }));
    assert!(schema.fields[0].nullable, "empty-input SUM produces NULL");
}

#[tokio::test]
async fn direct_aggregate_calls_keep_the_existing_plan_shape() {
    let plan = bind("FROM users |> AGGREGATE SUM(age) AS total GROUP BY name").await;
    assert_chain(&plan, &["Aggregate", "Scan"]);
}
