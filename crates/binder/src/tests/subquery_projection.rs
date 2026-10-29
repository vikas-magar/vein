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
        error.to_string().contains("IN subquery must return exactly one column"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn grouped_membership_requires_an_explicit_projection() {
    for operator in ["IN", "NOT IN"] {
        let prefix = format!(
            "FROM users |> WHERE id {operator} (FROM orders |> AGGREGATE sum(amount) AS total GROUP BY user_id |> WHERE total > 300"
        );
        let error = bind_err(&format!("{prefix})")).await;
        assert!(error.to_string().contains(&format!("{operator} subquery")));
        assert!(error.to_string().contains("returned 2"));
        bind(&format!("{prefix} |> SELECT user_id)")).await;
    }
}

#[tokio::test]
async fn scalar_arity_remains_separate_from_exists() {
    let error = bind_err("FROM users |> WHERE id = (FROM orders |> SELECT user_id, amount)").await;
    assert!(error.to_string().contains("scalar subquery must return exactly one column"));
    bind("FROM users |> WHERE EXISTS (FROM orders |> SELECT user_id, amount)").await;
}
