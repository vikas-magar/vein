use super::{assert_chain, bind, bind_err};

#[tokio::test]
async fn in_subquery_accepts_one_projected_column() {
    let plan = bind("FROM users |> WHERE id IN (FROM orders |> SELECT user_id)").await;
    assert_chain(&plan, &["Filter", "Scan"]);
}

#[tokio::test]
async fn in_subquery_rejects_multiple_projected_columns() {
    let error = bind_err("FROM users |> WHERE id IN (FROM orders |> SELECT user_id, amount)")
        .await;
    assert!(
        error.to_string().contains("exactly one column"),
        "unexpected error: {error}"
    );
}
