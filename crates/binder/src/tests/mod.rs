//! Golden-style binder tests: bind a SQL string and assert on the resulting
//! logical plan tree shape (nodes top-down), schemas, and semantics.
//!
//! Every assertion runs through [`crate::testing::plan`], which binds with
//! the in-memory catalog and mock registry, so tests read like executable
//! specs: `bind(sql).await` then shape/schema assertions.

use crate::testing;
use plan::{LogicalExpr, LogicalPlan, PlanTraversal};

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
        LogicalPlan::ExternalScan { .. } => "ExternalScan",
        LogicalPlan::Intermediate { .. } => "Intermediate",
        LogicalPlan::Filter { .. } => "Filter",
        LogicalPlan::Project { .. } => "Project",
        LogicalPlan::Aggregate { .. } => "Aggregate",
        LogicalPlan::Sort { .. } => "Sort",
        LogicalPlan::Limit { .. } => "Limit",
        LogicalPlan::Distinct { .. } => "Distinct",
        LogicalPlan::SubqueryAlias { .. } => "SubqueryAlias",
        LogicalPlan::Explain { .. } => "Explain",
        LogicalPlan::SetOp { .. } => "SetOp",
        LogicalPlan::Join { .. } => "Join",
        LogicalPlan::Sample { .. } => "Sample",
        LogicalPlan::Assert { .. } => "Assert",
        LogicalPlan::Fork { .. } => "Fork",
        LogicalPlan::Tee { .. } => "Tee",
        LogicalPlan::Window { .. } => "Window",
        LogicalPlan::SemanticFilter { .. } => "SemanticFilter",
        LogicalPlan::SemanticJoin { .. } => "SemanticJoin",
        LogicalPlan::SemanticCalibrate { .. } => "SemanticCalibrate",
        LogicalPlan::Write { .. } => "Write",
        LogicalPlan::Call { .. } => "Call",
        LogicalPlan::Pivot { .. } => "Pivot",
        LogicalPlan::Unpivot { .. } => "Unpivot",
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

    /// `table.*` expands only that table's columns, keeping their qualifier.
    #[tokio::test]
    async fn qualified_wildcard_expands_table_columns() {
        let plan = bind("FROM users |> SELECT users.*").await;
        assert_chain(&plan, &["Project", "Scan"]);
        assert_schema(
            &plan,
            &[
                ("id", None, false),
                ("name", None, false),
                ("age", None, false),
            ],
        );
    }

    /// A `table.*` for a table not in scope is a semantic error.
    #[tokio::test]
    async fn qualified_wildcard_unknown_table_is_error() {
        let err = bind_err("FROM users |> SELECT orders.*").await;
        assert!(
            matches!(err, error::VeinError::Semantic { .. }),
            "got {err}"
        );
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
        assert_schema(&plan, &[("id", None, false), ("total_age", None, true)]);
    }

    #[tokio::test]
    async fn select_window_without_alias_uses_fn_name() {
        let plan = bind("FROM users |> SELECT SUM(age) OVER ()").await;
        assert_chain(&plan, &["Project", "Window", "Scan"]);
        assert_schema(&plan, &[("sum", None, true)]);
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
                ("total_age", None, true),
            ],
        );
    }

    /// EXTEND of an aliased existing column applies the alias and resets the
    /// qualifier, rather than dropping the alias for the bare column name.
    #[tokio::test]
    async fn extend_alias_renames_existing_column() {
        let plan = bind("FROM users |> EXTEND id AS user_id").await;
        assert_chain(&plan, &["Project", "Scan"]);
        assert_schema(
            &plan,
            &[
                ("id", Some("users"), false),
                ("name", Some("users"), false),
                ("age", Some("users"), false),
                ("user_id", None, false),
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
        assert_schema(&plan, &[("total_age", None, true), ("n", None, true)]);
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

#[cfg(test)]
mod subquery {
    use super::*;

    /// A correlated `EXISTS` subquery must resolve the outer column through the
    /// captured correlation scopes (producing `OuterColumn`), not fail with an
    /// "unknown column" error. This guards the owned-snapshot correlation
    /// mechanism in `ScopeStack`.
    #[tokio::test]
    async fn correlated_exists_resolves_outer_column() {
        let plan = bind(
            "FROM users |> WHERE EXISTS (FROM orders |> WHERE orders.user_id = users.id |> SELECT user_id)",
        )
            .await;
        // Binding succeeded => `users.id` resolved via the correlation snapshot.
        assert_chain(&plan, &["Filter", "Scan"]);
    }

    /// An uncorrelated scalar subquery still binds (no outer reference).
    #[tokio::test]
    async fn uncorrelated_scalar_subquery_binds() {
        let plan = bind("FROM users |> WHERE id = (FROM orders |> SELECT id)").await;
        assert_chain(&plan, &["Filter", "Scan"]);
    }

    /// A subquery referencing a genuinely unknown column is a semantic error,
    /// even when another (correlated) column resolves fine.
    #[tokio::test]
    async fn subquery_unknown_column_is_semantic_error() {
        let err = bind_err(
            "FROM users |> WHERE EXISTS (FROM orders |> WHERE orders.user_id = users.id |> SELECT nope)",
        )
            .await;
        assert!(
            matches!(err, error::VeinError::Semantic { .. }),
            "got {err}"
        );
    }

    /// Guard against accidental identifier capture: an unqualified column in a
    /// subquery cannot silently resolve to an outer column. It must be explicitly
    /// qualified (e.g. `users.age`), otherwise it fails with a diagnostic hint.
    #[tokio::test]
    async fn unqualified_outer_column_capture_is_rejected() {
        // `users` has `age`, but `orders` does NOT have `age`.
        // In legacy SQL, `age > 18` inside the subquery would silently capture `users.age`.
        // In Vein, this is rejected with a helpful diagnostic hint.
        let err =
            bind_err("FROM users |> WHERE EXISTS (FROM orders |> WHERE age > 18 |> SELECT id)")
                .await;
        assert!(
            matches!(err, error::VeinError::Semantic { .. }),
            "got {err}"
        );
        assert!(
            err.to_string()
                .contains("Hint: 'age' exists in outer scope 'users'"),
            "expected diagnostic hint, got: {err}"
        );
    }

    /// When explicitly qualified (`users.age`), outer column references resolve
    /// cleanly to `OuterColumn`.
    #[tokio::test]
    async fn qualified_outer_column_resolves_successfully() {
        let plan =
            bind("FROM users |> WHERE EXISTS (FROM orders |> WHERE users.age > 18 |> SELECT id)")
                .await;
        assert_chain(&plan, &["Filter", "Scan"]);
    }
}

#[cfg(test)]
mod projection_mutations {
    use super::*;

    /// DROP removes the named columns and keeps the rest.
    #[tokio::test]
    async fn drop_removes_columns() {
        let plan = bind("FROM users |> DROP name").await;
        assert_chain(&plan, &["Project", "Scan"]);
        assert_schema(
            &plan,
            &[("id", Some("users"), false), ("age", Some("users"), false)],
        );
    }

    /// DROP of an unknown column is a semantic error (mirrors SET), not a
    /// silent no-op that would mask a misspelled column name.
    #[tokio::test]
    async fn drop_unknown_column_is_error() {
        let err = bind_err("FROM users |> DROP does_not_exist").await;
        assert!(
            matches!(err, error::VeinError::Semantic { .. }),
            "got {err}"
        );
        assert!(err.to_string().contains("cannot find column"), "got {err}");
    }

    /// SET overwrites a column's value; the other columns pass through.
    #[tokio::test]
    async fn set_overwrites_column() {
        let plan = bind("FROM users |> SET age = age + 1").await;
        assert_chain(&plan, &["Project", "Scan"]);
        assert_schema(
            &plan,
            &[
                ("id", Some("users"), false),
                ("name", Some("users"), false),
                ("age", Some("users"), false),
            ],
        );
    }

    #[tokio::test]
    async fn computed_projection_mutations_preserve_expression_nullability() {
        let select = bind("FROM users |> SELECT id + 1 AS next_id, LOWER(name) AS lowered").await;
        assert_schema(
            &select,
            &[("next_id", None, false), ("lowered", None, true)],
        );

        let extend = bind("FROM users |> EXTEND LOWER(name) AS lowered").await;
        assert!(extend.schema().fields.last().unwrap().nullable);

        let set = bind("FROM users |> SET name = LOWER(name)").await;
        assert!(set.schema().fields[1].nullable);
    }

    /// SET of an unknown column is a semantic error.
    #[tokio::test]
    async fn set_unknown_column_is_error() {
        let err = bind_err("FROM users |> SET does_not_exist = 1").await;
        assert!(
            matches!(err, error::VeinError::Semantic { .. }),
            "got {err}"
        );
    }
}

#[cfg(test)]
mod join_and_sources {
    use super::*;
    use parser::ast::pipe_op::JoinType;

    /// An `INNER JOIN ... ON` binds into a `Join` node with both sides' columns.
    #[tokio::test]
    async fn inner_join_binds_join_node() {
        let plan = bind("FROM users |> JOIN orders ON users.id = orders.user_id").await;
        if let LogicalPlan::Join {
            join_type,
            on,
            using,
            schema,
            ..
        } = &plan
        {
            assert!(matches!(join_type, JoinType::Inner));
            assert!(on.is_some(), "INNER join needs an ON predicate");
            assert!(using.is_empty());
            // users(id,name,age) ++ orders(id,user_id,amount).
            assert_eq!(schema.fields.len(), 6);
        } else {
            panic!("expected a Join plan, got {:?}", node_kind(&plan));
        }
    }

    #[tokio::test]
    async fn join_output_nullability_follows_join_flavor() {
        for (flavor, left_nullable, right_nullable) in [
            ("JOIN", false, false),
            ("LEFT JOIN", false, true),
            ("RIGHT JOIN", true, false),
            ("FULL JOIN", true, true),
        ] {
            let query = format!(
                "FROM users |> {flavor} orders ON users.id = orders.user_id"
            );
            let plan = bind(&query).await;
            let LogicalPlan::Join { schema, .. } = plan else {
                panic!("expected a Join plan for {flavor}");
            };

            assert_eq!(schema.fields.len(), 6, "{flavor}");
            assert!(
                schema.fields[..3]
                    .iter()
                    .all(|field| field.nullable == left_nullable),
                "left-side nullability mismatch for {flavor}: {:?}",
                &schema.fields[..3]
            );
            assert!(
                schema.fields[3..]
                    .iter()
                    .all(|field| field.nullable == right_nullable),
                "right-side nullability mismatch for {flavor}: {:?}",
                &schema.fields[3..]
            );
        }

        let projected = bind(
            "FROM users |> LEFT JOIN orders ON users.id = orders.user_id |> SELECT orders.amount",
        )
            .await;
        let LogicalPlan::Project { schema, .. } = projected else {
            panic!("expected a Project above the left join");
        };
        assert!(
            schema.fields[0].nullable,
            "projecting a null-extended input column must retain nullable metadata"
        );
    }

    /// Qualified self-join references must resolve to their respective input scopes.
    #[tokio::test]
    async fn qualified_self_join_columns_resolve_to_each_side() {
        let plan = bind("FROM users AS a |> JOIN users AS b ON a.id = b.id").await;
        let LogicalPlan::Join {
            on: Some(LogicalExpr::BinaryOp { left, right, .. }),
            ..
        } = plan
        else {
            panic!("expected an equality predicate on the self-join");
        };

        let (LogicalExpr::Column(left), LogicalExpr::Column(right)) = (*left, *right) else {
            panic!("expected column references in the self-join predicate");
        };

        assert_eq!(left.qualifier.as_deref(), Some("a"));
        assert_eq!(right.qualifier.as_deref(), Some("b"));
        assert_ne!(left.id, right.id);
    }

    /// `USING (col)` synthesizes an equality predicate and de-duplicates the
    /// joined column in the output schema.
    #[tokio::test]
    async fn using_dedupes_join_column() {
        let plan = bind("FROM users |> JOIN orders USING (id)").await;
        if let LogicalPlan::Join {
            on, using, schema, ..
        } = &plan
        {
            assert!(on.is_some(), "USING must synthesize an ON predicate");
            assert_eq!(using.len(), 1);
            // users(id,name,age) ++ (orders minus id) => 5 columns, id once.
            assert_eq!(schema.fields.len(), 5);
        } else {
            panic!("expected a Join plan");
        }
    }

    /// A `CROSS JOIN` carries no predicate.
    #[tokio::test]
    async fn cross_join_has_no_predicate() {
        let plan = bind("FROM users |> CROSS JOIN orders").await;
        if let LogicalPlan::Join { on, .. } = &plan {
            assert!(on.is_none(), "CROSS join has no ON predicate");
        } else {
            panic!("expected a Join plan");
        }
    }

    /// `LEFT JOIN` preserves the join flavor.
    #[tokio::test]
    async fn left_join_preserves_flavor() {
        let plan = bind("FROM users |> LEFT JOIN orders ON users.id = orders.user_id").await;
        if let LogicalPlan::Join { join_type, .. } = &plan {
            assert!(matches!(join_type, JoinType::Left));
        } else {
            panic!("expected a Join plan");
        }
    }

    /// An unqualified reference to a column present on both sides is ambiguous.
    #[tokio::test]
    async fn join_ambiguous_unqualified_column() {
        let err = bind_err("FROM users |> JOIN orders ON id = id").await;
        assert!(
            matches!(err, error::VeinError::Semantic { .. }),
            "got {err}"
        );
    }

    /// A `FROM (subquery)` source binds into a `SubqueryAlias` plan node.
    #[tokio::test]
    async fn subquery_source_binds() {
        let plan = bind("FROM (FROM users |> SELECT id, name) AS u |> SELECT u.id").await;
        assert_chain(&plan, &["Project", "SubqueryAlias", "Project", "Scan"]);
    }

    /// A join may take a subquery as its right-hand source.
    #[tokio::test]
    async fn join_subquery_right_source() {
        let plan =
            bind("FROM users |> JOIN (FROM orders |> SELECT user_id AS uid) o ON users.id = o.uid")
                .await;
        assert!(
            matches!(&plan, LogicalPlan::Join { .. }),
            "got {:?}",
            node_kind(&plan)
        );
    }
}

#[cfg(test)]
mod call_and_external {
    use super::*;

    /// A `CALL name(args)` pipe binds into a `Call` node carrying the bound
    /// arguments and passing the input stream through.
    #[tokio::test]
    async fn call_binds_call_node() {
        let plan = bind("FROM users |> CALL my_udf(users.id, 1)").await;
        if let LogicalPlan::Call {
            name, args, schema, ..
        } = &plan
        {
            assert_eq!(name, "my_udf");
            assert_eq!(args.len(), 2);
            assert_eq!(schema.fields.len(), 3, "Call mirrors the input schema");
        } else {
            panic!("expected a Call plan, got {:?}", node_kind(&plan));
        }
    }

    /// An object store source binds into an `ExternalScan` node.
    #[tokio::test]
    async fn object_store_source_binds_external_scan() {
        let plan = bind("FROM 's3://bucket/data' WITH (format = 'parquet') |> SELECT 1").await;
        assert_chain(&plan, &["Project", "ExternalScan"]);
    }

    #[tokio::test]
    async fn catalog_resolved_external_source_retains_shared_metadata() {
        let plan = bind("FROM 'users' |> SELECT 1").await;
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected project over external scan");
        };
        let LogicalPlan::ExternalScan {
            source_metadata: Some(metadata),
            ..
        } = *input
        else {
            panic!("resolved external source must retain its catalog metadata");
        };
        assert_eq!(metadata.name, "users");
    }
}

#[cfg(test)]
mod pivot_unpivot {
    use super::*;

    /// A PIVOT pipe binds into a `Pivot` node whose schema appends one
    /// column per pivot value.
    #[tokio::test]
    async fn pivot_binds_pivot_node() {
        let plan = bind("FROM orders |> PIVOT SUM(amount) AS total BY user_id IN (1, 2)").await;
        let LogicalPlan::Pivot {
            alias,
            value_column,
            values,
            schema,
            ..
        } = &plan
        else {
            panic!("expected a Pivot plan, got {:?}", node_kind(&plan));
        };
        assert_eq!(alias.as_deref(), Some("total"));
        assert_eq!(value_column.name, "user_id");
        assert_eq!(values.len(), 2);
        assert_eq!(schema.fields.len(), 5, "orders fields + 2 pivoted columns");
        assert_eq!(schema.fields[3].name, "total_0");
        assert_eq!(schema.fields[4].name, "total_1");
    }

    /// An UNPIVOT pipe binds into an `Unpivot` node that drops the unpivoted
    /// columns and appends `name` and `value` columns.
    #[tokio::test]
    async fn unpivot_binds_unpivot_node() {
        let plan = bind("FROM orders |> UNPIVOT ALL amount BY category IN (user_id, amount)").await;
        let LogicalPlan::Unpivot {
            value_name,
            name_name,
            columns,
            include_nulls,
            schema,
            ..
        } = &plan
        else {
            panic!("expected an Unpivot plan, got {:?}", node_kind(&plan));
        };
        assert_eq!(value_name, "amount");
        assert_eq!(name_name, "category");
        assert_eq!(columns.len(), 2);
        assert!(include_nulls);
        assert_eq!(schema.fields.len(), 3, "orders id + category + amount");
        assert_eq!(schema.fields[1].name, "category");
        assert_eq!(schema.fields[2].name, "amount");
    }
}

#[cfg(test)]
#[path = "subquery_projection.rs"]
mod subquery_projection;
mod substring;

#[cfg(test)]
#[path = "aggregate_projection.rs"]
mod aggregate_projection;

#[cfg(test)]
#[path = "numeric_coercion.rs"]
mod numeric_coercion;

#[cfg(test)]
mod cte_binding {
    use super::*;

    /// A `WITH` pipe registers a CTE that a later (nested) source can reference.
    #[tokio::test]
    async fn with_registers_cte_for_later_source() {
        let plan = bind("FROM users |> WITH cte AS (FROM orders |> SELECT user_id, amount) |> JOIN cte c ON users.id = c.user_id").await;
        assert!(
            matches!(&plan, LogicalPlan::Join { .. }),
            "got {:?}",
            node_kind(&plan)
        );
    }

    /// A CTE used as the sole source substitutes for a catalog table.
    #[tokio::test]
    async fn cte_as_source() {
        let plan = bind("FROM users |> WITH big AS (FROM users |> WHERE age > 18) |> JOIN big b ON users.id = b.id").await;
        assert!(
            matches!(&plan, LogicalPlan::Join { .. }),
            "got {:?}",
            node_kind(&plan)
        );
    }
}

#[cfg(test)]
mod extended_features {
    use super::*;
    use plan::LogicalExpr;
    use plan::LogicalLiteral;

    fn expr_of(plan: &LogicalPlan) -> &LogicalExpr {
        match plan {
            LogicalPlan::Filter { predicate, .. } => predicate,
            LogicalPlan::Project { exprs, .. } => &exprs[0],
            other => panic!("unexpected node {}", node_kind(other)),
        }
    }

    fn between_bounds(
        expr: &LogicalExpr,
    ) -> (&LogicalExpr, &LogicalExpr, &LogicalExpr, &LogicalExpr) {
        let LogicalExpr::BinaryOp {
            left,
            op: parser::ast::expr::BinaryOp::And,
            right,
            ..
        } = expr
        else {
            panic!("BETWEEN should lower to an AND of its inclusive bounds: {expr:?}");
        };
        let LogicalExpr::BinaryOp {
            left: lower_value,
            op: parser::ast::expr::BinaryOp::Geq,
            right: lower,
            ..
        } = left.as_ref()
        else {
            panic!("BETWEEN lower bound should use >=: {left:?}");
        };
        let LogicalExpr::BinaryOp {
            left: upper_value,
            op: parser::ast::expr::BinaryOp::Leq,
            right: upper,
            ..
        } = right.as_ref()
        else {
            panic!("BETWEEN upper bound should use <=: {right:?}");
        };
        (lower_value, lower, upper_value, upper)
    }

    #[tokio::test]
    async fn between_binds_both_inclusive_bounds_and_not_between_negates_them() {
        for (query, negated) in [
            ("FROM users |> SELECT age BETWEEN 18 AND 30", false),
            ("FROM users |> SELECT age NOT BETWEEN 18 AND 30", true),
        ] {
            let plan = bind(query).await;
            let expr = expr_of(&plan);
            let range = match (negated, expr) {
                (false, expr) => expr,
                (true, LogicalExpr::Not { expr }) => expr.as_ref(),
                (true, other) => panic!("NOT BETWEEN should negate the range: {other:?}"),
            };
            let (lower_value, lower, upper_value, upper) = between_bounds(range);

            assert_eq!(lower_value, upper_value, "both bounds must compare the input value");
            assert!(matches!(lower_value, LogicalExpr::Column(column) if column.name == "age"));
            assert!(matches!(lower, LogicalExpr::Literal(LogicalLiteral::Int64(18))));
            assert!(matches!(upper, LogicalExpr::Literal(LogicalLiteral::Int64(30))));
        }
    }

    #[tokio::test]
    async fn between_preserves_null_input_for_three_valued_logic() {
        let plan = bind("FROM users |> SELECT NULL BETWEEN 1 AND 2").await;
        let expr = expr_of(&plan);
        let (lower_value, _, upper_value, _) = between_bounds(expr);

        assert!(matches!(lower_value, LogicalExpr::Literal(LogicalLiteral::Null(_))));
        assert!(matches!(upper_value, LogicalExpr::Literal(LogicalLiteral::Null(_))));
        assert!(expr.nullable(), "a NULL input must remain nullable through BETWEEN");
    }

    #[tokio::test]
    async fn between_coerces_each_decimal_bound_to_the_input_type() {
        let plan = bind("FROM decimal_values |> SELECT amount BETWEEN 0.05 AND 0.07").await;
        let (lower_value, lower, upper_value, upper) = between_bounds(expr_of(&plan));
        let decimal_type = plan::VeinDataType::Decimal128(15, 2);

        assert!(matches!(lower_value, LogicalExpr::Column(column) if column.name == "amount"));
        assert_eq!(lower_value.data_type(), decimal_type);
        assert_eq!(upper_value.data_type(), decimal_type);
        assert_eq!(lower.data_type(), decimal_type);
        assert_eq!(upper.data_type(), decimal_type);
        assert!(matches!(
            lower,
            LogicalExpr::Cast { expr, target_type }
                if *target_type == decimal_type
                    && matches!(expr.as_ref(), LogicalExpr::Literal(LogicalLiteral::Decimal(5, 2, 2)))
        ));
        assert!(matches!(
            upper,
            LogicalExpr::Cast { expr, target_type }
                if *target_type == decimal_type
                    && matches!(expr.as_ref(), LogicalExpr::Literal(LogicalLiteral::Decimal(7, 2, 2)))
        ));
    }

    #[tokio::test]
    async fn decimal_literal_binds_exactly_and_compares_with_decimal_column() {
        let projection = bind("FROM decimal_values |> SELECT 0.05 AS threshold").await;
        assert!(matches!(
            expr_of(&projection),
            LogicalExpr::Literal(LogicalLiteral::Decimal(5, 2, 2))
        ));

        let filter = bind("FROM decimal_values |> WHERE amount >= 0.05").await;
        assert_eq!(expr_of(&filter).data_type(), plan::VeinDataType::Boolean);
    }

    #[tokio::test]
    async fn scalar_function_arguments_follow_registered_signatures() {
        let plan = bind("FROM users |> SELECT LOWER(name)").await;
        let LogicalExpr::FunctionCall { args, .. } = expr_of(&plan) else {
            panic!("expected scalar function call");
        };
        assert_eq!(args.len(), 1);
        assert_eq!(args[0].data_type(), plan::VeinDataType::Utf8);

        for query in [
            "FROM users |> SELECT LOWER(age)",
            "FROM users |> SELECT LOWER(name, name)",
        ] {
            let error = bind_err(query).await;
            assert!(
                matches!(error, error::VeinError::Semantic { .. }),
                "got {error}"
            );
        }
    }

    #[tokio::test]
    async fn extract_and_year_accept_temporal_inputs_and_return_int32() {
        let plan = bind(
            "FROM dates |> SELECT EXTRACT(YEAR FROM order_date), EXTRACT(MONTH FROM date64), EXTRACT(HOUR FROM time_of_day), EXTRACT(MINUTE FROM timestamp), YEAR(order_date)",
        )
            .await;
        let LogicalPlan::Project { exprs, .. } = plan else {
            panic!("expected Project plan");
        };
        assert_eq!(exprs.len(), 5);
        for expr in exprs {
            let LogicalExpr::FunctionCall { return_type, .. } = expr else {
                panic!("expected temporal function call");
            };
            assert_eq!(return_type, plan::VeinDataType::Int32);
        }

        for query in [
            "FROM dates |> SELECT EXTRACT(FROM order_date)",
            "FROM dates |> SELECT EXTRACT(YEAR order_date)",
            "FROM dates |> SELECT EXTRACT(YEAR FROM 1)",
            "FROM dates |> SELECT EXTRACT(YEAR FROM order_date, date64)",
        ] {
            let error = bind_err(query).await;
            assert!(
                matches!(error, error::VeinError::Semantic { .. }),
                "got {error}"
            );
        }
    }

    #[tokio::test]
    async fn scalar_function_arguments_coerce_null_and_widening_to_signature_type() {
        let null_plan = bind("FROM users |> SELECT LOWER(NULL)").await;
        let LogicalExpr::FunctionCall { args, .. } = expr_of(&null_plan) else {
            panic!("expected scalar function call");
        };
        assert!(matches!(
            args.as_slice(),
            [LogicalExpr::Cast {
                target_type: plan::VeinDataType::Utf8,
                ..
            }]
        ));

        let numeric_plan = bind("FROM users |> SELECT ROUND(age)").await;
        let LogicalExpr::FunctionCall { args, .. } = expr_of(&numeric_plan) else {
            panic!("expected scalar function call");
        };
        assert!(matches!(
            args.as_slice(),
            [LogicalExpr::Cast {
                target_type: plan::VeinDataType::Float64,
                ..
            }]
        ));
    }

    #[tokio::test]
    async fn vector_dimension_wildcard_matches_registered_scalar_signature() {
        let plan =
            bind("FROM vector_values |> SELECT VECTOR_DISTANCE(left_vector, right_vector)").await;
        assert!(matches!(
            expr_of(&plan),
            LogicalExpr::FunctionCall { name, .. }
                if name.eq_ignore_ascii_case("VECTOR_DISTANCE")
        ));
    }

    /// `COUNT(*)` is lowered to a non-null literal for ordinary aggregates.
    #[tokio::test]
    async fn aggregate_count_star_binds_as_one() {
        for query in [
            "FROM users |> AGGREGATE COUNT(*) AS n",
            "FROM users |> AGGREGATE COUNT(*) AS n GROUP BY name",
        ] {
            let plan = bind(query).await;
            let LogicalPlan::Aggregate { aggs, .. } = plan else {
                panic!("expected aggregate plan");
            };
            assert!(matches!(
                &aggs[0],
                LogicalExpr::FunctionCall { name, args, .. }
                    if name.eq_ignore_ascii_case("count")
                        && matches!(args.as_slice(), [LogicalExpr::Literal(LogicalLiteral::Int64(1))])
            ));
        }
    }

    #[tokio::test]
    async fn count_distinct_binds_its_distinct_metadata_and_rejects_unsupported_forms() {
        let plan = bind("FROM users |> AGGREGATE COUNT(DISTINCT age) AS n GROUP BY name").await;
        let LogicalPlan::Aggregate { aggs, .. } = plan else {
            panic!("expected aggregate plan");
        };
        assert!(matches!(
            &aggs[0],
            LogicalExpr::FunctionCall { name, distinct: true, args, .. }
                if name.eq_ignore_ascii_case("count") && args.len() == 1
        ));

        for (query, message) in [
            (
                "FROM users |> AGGREGATE SUM(DISTINCT age)",
                "supported only for COUNT",
            ),
            (
                "FROM users |> AGGREGATE COUNT(DISTINCT *)",
                "COUNT(DISTINCT *) is not valid",
            ),
        ] {
            let error = bind_err(query).await;
            assert!(error.to_string().contains(message), "got {error} for {query}");
        }
    }

    #[tokio::test]
    async fn aggregate_argument_count_is_validated_during_binding() {
        for query in [
            "FROM users |> AGGREGATE COUNT()",
            "FROM users |> AGGREGATE SUM(age, id)",
        ] {
            let error = bind_err(query).await;
            assert!(
                error.to_string().contains("expects exactly one argument"),
                "got {error} for {query}"
            );
        }
    }

    /// Typed `TIME` string literals bind to a `Time` literal.
    #[tokio::test]
    async fn typed_time_literal_binds() {
        let plan = bind("FROM users |> SELECT TIME '12:30:45' AS t").await;
        let LogicalExpr::Literal(LogicalLiteral::Time(us)) = expr_of(&plan) else {
            panic!("expected Time literal");
        };
        assert_eq!(*us, (12 * 3600 + 30 * 60 + 45) * 1_000_000);
    }

    /// Typed `INTERVAL` string literals bind to an `Interval` literal.
    #[tokio::test]
    async fn typed_interval_literal_binds() {
        let plan = bind("FROM users |> SELECT INTERVAL '1 year 6 months' AS d").await;
        let LogicalExpr::Literal(LogicalLiteral::Interval { months, .. }) = expr_of(&plan) else {
            panic!("expected Interval literal");
        };
        assert_eq!(*months, 18);
    }

    #[tokio::test]
    async fn typed_interval_unit_qualifier_binds() {
        let plan = bind("FROM users |> SELECT INTERVAL '90' DAY AS d").await;
        let LogicalExpr::Literal(LogicalLiteral::Interval {
                                     months,
                                     days,
                                     nanos,
                                 }) = expr_of(&plan)
        else {
            panic!("expected Interval literal");
        };
        assert_eq!((*months, *days, *nanos), (0, 90, 0));
    }

    /// `QUALIFY` lowers to `Filter → Window → Scan`.
    #[tokio::test]
    async fn qualify_lowers_to_filter_above_window() {
        let plan = bind("FROM users |> QUALIFY SUM(age) OVER () > 100").await;
        assert_chain(&plan, &["Filter", "Window", "Scan"]);
    }

    /// `QUALIFY` without a window function is rejected.
    #[tokio::test]
    async fn qualify_requires_window_function() {
        let err = bind_err("FROM users |> QUALIFY age > 18").await;
        assert!(
            err.to_string()
                .contains("QUALIFY predicate must contain a window function"),
            "got: {err}"
        );
    }
}

#[cfg(test)]
mod aggregate_types {
    use super::*;
    use plan::VeinDataType;

    #[tokio::test]
    async fn grouping_sets_bind_all_keys_once_in_the_output_schema() {
        let plan = bind(
            "FROM users |> AGGREGATE COUNT(*) AS n GROUP BY GROUPING SETS ((name, age), (name), ())",
        )
            .await;
        let LogicalPlan::Aggregate {
            grouping_sets,
            schema,
            ..
        } = plan
        else {
            panic!("expected Aggregate plan");
        };

        assert_eq!(grouping_sets.len(), 3);
        assert_eq!(
            grouping_sets.iter().map(Vec::len).collect::<Vec<_>>(),
            [2, 1, 0]
        );
        assert_eq!(schema.fields.len(), 3);
        assert_eq!(schema.fields[0].name, "name");
        assert_eq!(schema.fields[1].name, "age");
        assert_eq!(schema.fields[2].name, "n");
    }

    #[tokio::test]
    async fn aggregate_output_types_follow_numeric_input_types() {
        let plan = bind(
            "FROM users |> AGGREGATE SUM(id) AS total, MIN(age) AS youngest, AVG(age) AS mean_age",
        )
            .await;
        let LogicalPlan::Aggregate { schema, .. } = plan else {
            panic!("expected Aggregate plan");
        };

        assert_eq!(schema.fields[0].data_type, VeinDataType::Int64);
        assert_eq!(schema.fields[1].data_type, VeinDataType::Int64);
        assert_eq!(schema.fields[2].data_type, VeinDataType::Float64);
    }

    #[tokio::test]
    async fn aggregate_result_nullability_is_conservative_and_grouping_is_inferred() {
        let plan = bind(
            "FROM users |> AGGREGATE SUM(age) AS total, COUNT(*) AS n GROUP BY age + 1",
        )
            .await;
        let LogicalPlan::Aggregate { schema, .. } = plan else {
            panic!("expected Aggregate plan");
        };

        assert_eq!(schema.fields.len(), 3);
        assert!(schema.fields[1].nullable, "SUM can return NULL on empty input");
        assert!(schema.fields[2].nullable, "function nullability is conservative");
        assert!(!schema.fields[0].nullable, "non-null grouping inputs stay non-null");
    }

    #[tokio::test]
    async fn decimal_aggregate_types_preserve_scale_and_widen_sum_precision() {
        let plan = bind(
            "FROM decimal_values |> AGGREGATE SUM(amount) AS total, MIN(amount) AS smallest, AVG(amount) AS mean",
        )
            .await;
        let LogicalPlan::Aggregate { schema, .. } = plan else {
            panic!("expected Aggregate plan");
        };

        assert_eq!(schema.fields[0].data_type, VeinDataType::Decimal128(38, 2));
        assert_eq!(schema.fields[1].data_type, VeinDataType::Decimal128(15, 2));
        assert_eq!(schema.fields[2].data_type, VeinDataType::Float64);
    }

    #[tokio::test]
    async fn sum_result_types_cover_numeric_layouts() {
        let plan = bind(
            "FROM numeric_values |> AGGREGATE SUM(i8_value), SUM(i16_value), SUM(i32_value), SUM(u8_value), SUM(u16_value), SUM(u32_value), SUM(u64_value), SUM(f16_value), SUM(f32_value), SUM(decimal256_value)",
        )
            .await;
        let LogicalPlan::Aggregate { schema, .. } = plan else {
            panic!("expected Aggregate plan");
        };

        let expected = [
            VeinDataType::Int64,
            VeinDataType::Int64,
            VeinDataType::Int64,
            VeinDataType::UInt64,
            VeinDataType::UInt64,
            VeinDataType::UInt64,
            VeinDataType::UInt64,
            VeinDataType::Float64,
            VeinDataType::Float64,
            VeinDataType::Decimal256(76, 3),
        ];
        assert_eq!(
            schema
                .fields
                .iter()
                .map(|field| field.data_type.clone())
                .collect::<Vec<_>>(),
            expected
        );
    }

    #[tokio::test]
    async fn decimal_comparison_binds_both_sides_to_a_common_type() {
        let plan = bind("FROM decimal_values |> WHERE amount > amount * 0.0001").await;
        let LogicalPlan::Filter { predicate, .. } = plan else {
            panic!("expected Filter plan");
        };
        let LogicalExpr::BinaryOp { left, right, .. } = predicate else {
            panic!("expected a comparison predicate");
        };
        let common_type = left.data_type();
        assert_eq!(right.data_type(), common_type);
        assert_eq!(common_type, VeinDataType::Decimal128(35, 8));
        assert!(
            matches!(left.as_ref(), LogicalExpr::Cast { target_type, .. } if *target_type == common_type),
            "the narrower decimal operand must be cast to the comparison type: {left:?}"
        );
    }
}

#[cfg(test)]
mod temporal_functions {
    use super::*;
    use parser::ast::expr::BinaryOp;
    use plan::{LogicalExpr, LogicalLiteral, VeinDataType};

    #[tokio::test]
    async fn shift_lowers_to_temporal_interval_arithmetic() {
        let plan =
            bind("FROM users |> SELECT SHIFT(DATE '1998-12-01', -90, 'DAY') AS shifted").await;
        let LogicalPlan::Project { exprs, .. } = plan else {
            panic!("expected projection");
        };
        assert!(matches!(
            &exprs[0],
            LogicalExpr::BinaryOp {
                op: BinaryOp::Plus,
                return_type: VeinDataType::Date32,
                right,
                ..
            } if matches!(right.as_ref(), LogicalExpr::Literal(LogicalLiteral::Interval {
                months: 0, days: -90, nanos: 0
            }))
        ));
    }

    #[tokio::test]
    async fn shift_supports_calendar_and_subday_units() {
        for (unit, expected) in [
            (
                "MONTH",
                LogicalLiteral::Interval {
                    months: 1,
                    days: 0,
                    nanos: 0,
                },
            ),
            (
                "HOUR",
                LogicalLiteral::Interval {
                    months: 0,
                    days: 0,
                    nanos: 3_600_000_000_000,
                },
            ),
            (
                "MICROSECOND",
                LogicalLiteral::Interval {
                    months: 0,
                    days: 0,
                    nanos: 1_000,
                },
            ),
        ] {
            let sql = format!("FROM users |> SELECT SHIFT(DATE '2024-01-01', 1, '{unit}')");
            let plan = bind(&sql).await;
            let LogicalPlan::Project { exprs, .. } = plan else {
                panic!("expected projection");
            };
            let LogicalExpr::BinaryOp { right, .. } = &exprs[0] else {
                panic!("SHIFT should lower to binary arithmetic");
            };
            assert!(matches!(right.as_ref(), LogicalExpr::Literal(actual) if actual == &expected));
        }
    }

    #[tokio::test]
    async fn shift_rejects_nonconstant_amount_invalid_units_and_non_temporal_values() {
        for (query, message) in [
            (
                "FROM users |> SELECT SHIFT(DATE '2024-01-01', age, 'DAY')",
                "constant integer",
            ),
            (
                "FROM users |> SELECT SHIFT(DATE '2024-01-01', 1, age)",
                "constant string",
            ),
            (
                "FROM users |> SELECT SHIFT(DATE '2024-01-01', 1, 'FORTNIGHT')",
                "unknown temporal unit",
            ),
            (
                "FROM users |> SELECT SHIFT(age, 1, 'DAY')",
                "no implicit coercion",
            ),
        ] {
            let error = bind_err(query).await;
            assert!(error.to_string().contains(message), "{error} for {query}");
        }
    }

    #[tokio::test]
    async fn truncate_binds_to_the_unit_specific_vectorized_function() {
        let plan = bind("FROM users |> SELECT TRUNCATE(DATE '2024-03-31', 'QUARTER') AS bucket").await;
        let LogicalPlan::Project { exprs, schema, .. } = plan else {
            panic!("expected projection");
        };
        assert!(matches!(
            &exprs[0],
            LogicalExpr::FunctionCall { name, args, return_type, .. }
                if name == "TRUNCATE_QUARTER"
                    && args.len() == 1
                    && *return_type == VeinDataType::Date32
        ));
        assert_eq!(schema.fields[0].data_type, VeinDataType::Date32);
    }

    #[tokio::test]
    async fn truncate_validates_constant_unit_and_temporal_compatibility() {
        for (query, message) in [
            (
                "FROM users |> SELECT TRUNCATE(DATE '2024-01-01', age)",
                "constant string",
            ),
            (
                "FROM users |> SELECT TRUNCATE(DATE '2024-01-01', 'FORTNIGHT')",
                "unknown temporal unit",
            ),
            (
                "FROM users |> SELECT TRUNCATE(TIME '12:30:00', 'MONTH')",
                "does not support temporal type",
            ),
            (
                "FROM users |> SELECT TRUNCATE(age, 'DAY')",
                "does not support temporal type",
            ),
        ] {
            let error = bind_err(query).await;
            assert!(error.to_string().contains(message), "{error} for {query}");
        }
    }
}
