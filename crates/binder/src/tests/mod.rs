//! Golden-style binder tests: bind a SQL string and assert on the resulting
//! logical plan tree shape (nodes top-down), schemas, and semantics.
//!
//! Every assertion runs through [`crate::testing::plan`], which binds with
//! the in-memory catalog and mock registry, so tests read like executable
//! specs: `bind(sql).await` then shape/schema assertions.

use plan::{LogicalPlan, PlanTraversal};
use plan::ResolvedColumn;
use crate::testing;

/// Binds `sql` and unwraps the plan (tests assume the query is valid).
pub async fn bind(sql: &str) -> LogicalPlan {
    testing::plan(sql).await.expect("query should bind")
}

/// Binds `sql` and asserts the binder rejected it with a semantic error.
pub async fn bind_err(sql: &str) -> error::VeinError {
    testing::plan(sql)
        .await
        .expect_err("query should fail to bind")
}

/// Asserts the plan is the expected node-kind sequence when walking `left`
/// children down from the root (ignoring multi-child nodes).
pub fn assert_chain(plan: &LogicalPlan, expected_kinds: &[&str]) {
    let mut node = plan;
    let mut got = Vec::new();
    loop {
        got.push(node_kind(node));
        let children = node.children();
        match children.as_slice() {
            [only] => node = only,
            [] => break,
            _ => {
                panic!(
                    "expected a linear plan chain but node {} has {} children",
                    node_kind(node),
                    children.len()
                );
            }
        }
    }
    assert_eq!(
        got, expected_kinds,
        "plan chain mismatch\n  plan: {got:?}\n  want: {expected_kinds:?}"
    );
}

/// Canonical node-kind label used by tree assertions.
pub fn node_kind(plan: &LogicalPlan) -> &'static str {
    match plan {
        LogicalPlan::Scan { .. } => "Scan",
        LogicalPlan::Filter { .. } => "Filter",
        LogicalPlan::Project { .. } => "Project",
        LogicalPlan::Aggregate { .. } => "Aggregate",
        LogicalPlan::Sort { .. } => "Sort",
        LogicalPlan::Limit { .. } => "Limit",
        LogicalPlan::Distinct { .. } => "Distinct",
        LogicalPlan::SubqueryAlias { .. } => "SubqueryAlias",
        LogicalPlan::Explain { .. } => "Explain",
        LogicalPlan::SetOp { .. } => "SetOp",
        LogicalPlan::Sample { .. } => "Sample",
        LogicalPlan::Assert { .. } => "Assert",
        LogicalPlan::Fork { .. } => "Fork",
        LogicalPlan::Tee { .. } => "Tee",
        LogicalPlan::Window { .. } => "Window",
        LogicalPlan::SemanticFilter { .. } => "SemanticFilter",
        LogicalPlan::SemanticJoin { .. } => "SemanticJoin",
        LogicalPlan::SemanticCalibrate { .. } => "SemanticCalibrate",
        LogicalPlan::Write { .. } => "Write",
    }
}

/// Asserts the schema of `plan`, column by column: name, qualifier, nullability.
pub fn assert_schema(plan: &LogicalPlan, expected: &[(&str, Option<&str>, bool)]) {
    let fields = &plan.schema().fields;
    assert_eq!(
        fields.len(),
        expected.len(),
        "column count mismatch for {:?}\n  got:  {}\n  want: {}",
        plan.schema().fields,
        fields
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>()
            .join(", "),
        expected
            .iter()
            .map(|(n, _, _)| *n)
            .collect::<Vec<_>>()
            .join(", "),
    );
    for (field, (name, qualifier, nullable)) in fields.iter().zip(expected) {
        assert_eq!(&field.name, name, "column name mismatch");
        assert_eq!(
            field.qualifier.as_deref(),
            *qualifier,
            "qualifier of {name}"
        );
        assert_eq!(
            field.nullable, *nullable,
            "nullability of {name} (id={:?})",
            field.id
        );
    }
}

/// True when the plan's schema contains a column with exactly this identity.
pub fn has_column(plan: &LogicalPlan, name: &str) -> bool {
    plan.schema().fields.iter().any(|f| f.name == name)
}

/// The output column of the topmost projection for `index`.
pub fn output_column(plan: &LogicalPlan, index: usize) -> &ResolvedColumn {
    &plan.schema().fields[index]
}

#[cfg(test)]
mod smoke {
    use super::*;

    #[tokio::test]
    async fn from_select_binds() {
        let plan = bind("FROM users |> SELECT id, name").await;
        assert_chain(&plan, &["Project", "Scan"]);
        // SELECT columns are unqualified (Google: projection resets qualifiers).
        assert_schema(&plan, &[("id", None, false), ("name", None, false)]);
    }

    #[tokio::test]
    async fn where_binds_filter() {
        let plan = bind("FROM users |> WHERE age > 18").await;
        assert_chain(&plan, &["Filter", "Scan"]);
    }

    #[tokio::test]
    async fn unknown_function_is_not_found() {
        let err = bind_err("FROM users |> SELECT definitely_not_a_fn(1)").await;
        assert!(matches!(err, error::VeinError::NotFound(_)), "got {err}");
    }

    #[tokio::test]
    async fn unknown_table_is_not_found() {
        let err = bind_err("FROM missing_table |> SELECT *").await;
        assert!(matches!(err, error::VeinError::NotFound(_)), "got {err}");
    }
}

#[cfg(test)]
mod window {
    use super::*;

    /// `Project → Window → Scan`: the window call is lifted below the
    /// projection, which references the window output via a placeholder.
    #[tokio::test]
    async fn select_lifts_window_below_projection() {
        let plan = bind("FROM users |> SELECT id, SUM(age) OVER () AS total_age").await;
        assert_chain(&plan, &["Project", "Window", "Scan"]);
        assert_schema(&plan, &[("id", None, false), ("total_age", None, false)]);
    }

    #[tokio::test]
    async fn select_window_without_alias_uses_fn_name() {
        let plan = bind("FROM users |> SELECT SUM(age) OVER ()").await;
        assert_chain(&plan, &["Project", "Window", "Scan"]);
        assert_schema(&plan, &[("sum", None, false)]);
    }

    /// EXTEND keeps the input columns and appends the window output.
    #[tokio::test]
    async fn extend_appends_window_output() {
        let plan = bind("FROM users |> EXTEND SUM(age) OVER () AS total_age").await;
        assert_chain(&plan, &["Project", "Window", "Scan"]);
        // Input columns keep their qualifiers; the extended column is fresh.
        assert_schema(
            &plan,
            &[
                ("id", Some("users"), false),
                ("name", Some("users"), false),
                ("age", Some("users"), false),
                ("total_age", None, false),
            ],
        );
    }

    /// Window functions are refused outside projection operators.
    #[tokio::test]
    async fn where_rejects_window_function() {
        let err = bind_err("FROM users |> WHERE SUM(age) OVER () > 5").await;
        assert!(
            err.to_string()
                .contains("window functions are only allowed"),
            "got {err}"
        );
    }

    /// AGGREGATE refuses window functions (Google: use EXTEND before it).
    #[tokio::test]
    async fn aggregate_rejects_window_function() {
        let err = bind_err("FROM users |> AGGREGATE SUM(age) OVER ()").await;
        assert!(
            err.to_string()
                .contains("window functions are not allowed inside AGGREGATE"),
            "got {err}"
        );
    }

    /// SET refuses window functions (deferred feature).
    #[tokio::test]
    async fn set_rejects_window_function() {
        let err = bind_err("FROM users |> SET age = SUM(age) OVER ()").await;
        assert!(
            err.to_string()
                .contains("window functions are not allowed inside SET"),
            "got {err}"
        );
    }

    /// Multiple window calls in one operator share a single Window node.
    #[tokio::test]
    async fn multiple_calls_share_one_window_node() {
        let plan =
            bind("FROM users |> SELECT SUM(age) OVER () AS total_age, COUNT(*) OVER () AS n").await;
        assert_chain(&plan, &["Project", "Window", "Scan"]);
        assert_schema(&plan, &[("total_age", None, false), ("n", None, false)]);
    }

    /// Conflicting OVER clauses in one operator are a semantic error.
    #[tokio::test]
    async fn conflicting_partitions_rejected() {
        let err = bind_err(
            "FROM users |> SELECT SUM(age) OVER () AS a, SUM(age) OVER (PARTITION BY id) AS b",
        )
        .await;
        assert!(err.to_string().contains("same PARTITION BY"), "got {err}");
    }
}

#[cfg(test)]
mod explain {
    use super::*;

    /// EXPLAIN wraps the fully-bound inner plan in an `Explain` node whose
    /// schema mirrors the inner plan's.
    #[tokio::test]
    async fn explain_wraps_bound_plan() {
        let plan = bind("EXPLAIN FROM users |> SELECT id, name").await;
        assert_chain(&plan, &["Explain", "Project", "Scan"]);
        assert_schema(&plan, &[("id", None, false), ("name", None, false)]);
    }

    /// EXPLAIN of a semantically invalid query still fails: the inner query
    /// is fully bound before wrapping.
    #[tokio::test]
    async fn explain_propagates_bind_errors() {
        let err = bind_err("EXPLAIN FROM missing_table |> SELECT *").await;
        assert!(matches!(err, error::VeinError::NotFound(_)), "got {err}");
    }
}
