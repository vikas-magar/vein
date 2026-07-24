use ordered_float::OrderedFloat;

use crate::ast::data_type::DataType;
use crate::ast::expr::{BinaryOp, Expr, Literal};
use crate::logical_plan::{LogicalPlan, ScanSource, TreeNode};
use crate::optimizer::optimize;
use crate::token::Span;

fn span() -> Span {
    Span::default()
}

fn scan(name: &str) -> LogicalPlan {
    LogicalPlan::Scan {
        source: ScanSource::Table(name.to_string()),
        projection: None,
        filters: vec![],
        limit: None,
    }
}

fn filter(pred: Expr, input: LogicalPlan) -> LogicalPlan {
    LogicalPlan::Filter { predicate: pred, input: Box::new(input) }
}

fn project(exprs: Vec<Expr>, input: LogicalPlan) -> LogicalPlan {
    LogicalPlan::Projection { expr: exprs, input: Box::new(input) }
}

fn col(name: &str) -> Expr {
    Expr::Column { table: None, column: name.to_string(), span: span() }
}

fn int(n: i64) -> Expr {
    Expr::Literal(Literal::Int(n), span())
}

fn float(n: f64) -> Expr {
    Expr::Literal(Literal::Float(OrderedFloat(n)), span())
}

fn strlit(s: &str) -> Expr {
    Expr::Literal(Literal::String(s.to_string()), span())
}

fn typed_string_date(value: &str) -> Expr {
    Expr::TypedString { data_type: DataType::Date, value: value.to_string(), span: span() }
}

fn between(expr: Expr, low: Expr, high: Expr) -> Expr {
    Expr::Between { expr: Box::new(expr), low: Box::new(low), high: Box::new(high), negated: false, span: span() }
}

fn gt(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp { op: BinaryOp::Gt, left: Box::new(left), right: Box::new(right), span: span() }
}

fn ge(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp { op: BinaryOp::Geq, left: Box::new(left), right: Box::new(right), span: span() }
}

fn lt(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp { op: BinaryOp::Lt, left: Box::new(left), right: Box::new(right), span: span() }
}

fn eq(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp { op: BinaryOp::Eq, left: Box::new(left), right: Box::new(right), span: span() }
}

fn and(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp { op: BinaryOp::And, left: Box::new(left), right: Box::new(right), span: span() }
}

fn or(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp { op: BinaryOp::Or, left: Box::new(left), right: Box::new(right), span: span() }
}

fn in_list(expr: Expr, items: Vec<Expr>) -> Expr {
    Expr::InList { expr: Box::new(expr), list: items, negated: false, span: span() }
}

/// Count Filter nodes in a plan tree.
fn count_filter_nodes(plan: &LogicalPlan) -> usize {
    let mut count = 0;
    if matches!(plan, LogicalPlan::Filter { .. }) {
        count += 1;
    }
    let _ = plan.apply_children(&mut |child| {
        count += count_filter_nodes(child);
        Ok(())
    });
    count
}

#[test]
fn test_filter_pushdown_simple() {
    let plan = filter(
        Expr::BinaryOp { op: BinaryOp::Gt, left: Box::new(col("x")), right: Box::new(int(5)), span: span() },
        scan("t"),
    );
    let (optimized, fired) = optimize(&plan, None);
    assert!(fired.contains(&"filter_pushdown"));
    match optimized {
        LogicalPlan::Scan { ref filters, .. } => assert_eq!(filters.len(), 1),
        _ => panic!("expected Scan"),
    }
}

#[test]
fn test_filter_pushdown_double_evaluation() {
    let plan = filter(
        Expr::BinaryOp { op: BinaryOp::Eq, left: Box::new(col("a")), right: Box::new(int(1)), span: span() },
        scan("t"),
    );
    let (optimized, _) = optimize(&plan, None);
    let filter_count = count_filter_nodes(&optimized);
    assert_eq!(filter_count, 0, "filter should be fully absorbed");
}

#[test]
fn test_filter_pushdown_partial() {
    let pushable = Expr::BinaryOp { op: BinaryOp::Gt, left: Box::new(col("x")), right: Box::new(int(5)), span: span() };
    let non_pushable = Expr::BinaryOp {
        op: BinaryOp::Gt,
        left: Box::new(Expr::BinaryOp {
            op: BinaryOp::Plus, left: Box::new(col("x")), right: Box::new(int(1)), span: span(),
        }),
        right: Box::new(int(10)), span: span(),
    };
    let conjunct = Expr::BinaryOp { op: BinaryOp::And, left: Box::new(pushable), right: Box::new(non_pushable), span: span() };
    let plan = filter(conjunct, scan("t"));
    let (optimized, fired) = optimize(&plan, None);
    assert!(fired.contains(&"filter_pushdown"));
    match optimized {
        LogicalPlan::Filter { input, .. } => match *input {
            LogicalPlan::Scan { ref filters, .. } => {
                assert_eq!(filters.len(), 1);
            }
            _ => panic!("expected Scan under Filter"),
        },
        other => panic!("expected Filter, got {:?}", other),
    }
}

#[test]
fn test_filter_below_projection() {
    let plan = filter(
        Expr::BinaryOp { op: BinaryOp::Eq, left: Box::new(col("a")), right: Box::new(int(1)), span: span() },
        project(vec![col("a"), col("b")], scan("t")),
    );
    let (optimized, fired) = optimize(&plan, None);
    assert!(fired.contains(&"filter_pushdown"));
    match optimized {
        LogicalPlan::Scan { ref filters, projection, .. } => {
            assert_eq!(filters.len(), 1);
            assert_eq!(projection, Some(vec!["a".to_string(), "b".to_string()]));
        }
        _ => panic!("expected Scan"),
    }
}

#[test]
fn test_limit_into_scan() {
    let plan = LogicalPlan::Limit { limit: 10, offset: 0, input: Box::new(scan("t")) };
    let (optimized, fired) = optimize(&plan, None);
    assert!(fired.contains(&"limit_pushdown"));
    match optimized {
        LogicalPlan::Scan { limit, .. } => assert_eq!(limit, Some(10)),
        _ => panic!("expected Scan"),
    }
}

#[test]
fn test_limit_pushdown_through_projection() {
    let plan = LogicalPlan::Limit {
        limit: 10, offset: 0,
        input: Box::new(project(vec![col("a"), col("b")], scan("t"))),
    };
    let (optimized, fired) = optimize(&plan, None);
    assert!(fired.contains(&"limit_pushdown"));
    match optimized {
        LogicalPlan::Scan { limit, projection, .. } => {
            assert_eq!(limit, Some(10));
            assert_eq!(projection, Some(vec!["a".to_string(), "b".to_string()]));
        }
        _ => panic!("expected Scan"),
    }
}

#[test]
fn test_limit_pushdown_blocked_by_filter() {
    let plan = LogicalPlan::Limit {
        limit: 10, offset: 0,
        input: Box::new(filter(
            Expr::BinaryOp { op: BinaryOp::Gt, left: Box::new(col("a")), right: Box::new(int(1)), span: span() },
            scan("t"),
        )),
    };
    let (optimized, _fired) = optimize(&plan, None);
    match optimized {
        LogicalPlan::Scan { ref filters, limit, .. } => {
            assert_eq!(filters.len(), 1);
            assert_eq!(limit, Some(10));
        }
        other => panic!("expected Scan, got {:?}", other),
    }
}

#[test]
fn test_projection_pushdown_into_scan() {
    let plan = project(vec![col("a"), col("b")], scan("t"));
    let (optimized, fired) = optimize(&plan, None);
    assert!(fired.contains(&"projection_pushdown"));
    match optimized {
        LogicalPlan::Scan { projection, .. } => {
            let cols = projection.unwrap();
            assert_eq!(cols, vec!["a", "b"]);
        }
        _ => panic!("expected Scan"),
    }
}

#[test]
fn test_projection_pushdown_aggregate() {
    let plan = project(
        vec![col("x")],
        LogicalPlan::Aggregate {
            group_expr: vec![col("x")],
            aggregate_expr: vec![Expr::Function {
                name: "sum".into(), args: vec![col("y")], distinct: false, span: span(),
            }],
            input: Box::new(scan("t")),
        },
    );
    let (optimized, fired) = optimize(&plan, None);
    assert!(fired.contains(&"projection_pushdown"));
    match optimized {
        LogicalPlan::Projection { input, .. } => match *input {
            LogicalPlan::Aggregate { input: agg_input, .. } => match *agg_input {
                LogicalPlan::Scan { projection, .. } => {
                    let cols = projection.expect("expected projection");
                    assert!(cols.contains(&"x".to_string()), "should contain group column x");
                    assert!(cols.contains(&"y".to_string()), "should contain aggregate arg column y");
                }
                _ => panic!("expected Scan under Aggregate"),
            },
            _ => panic!("expected Aggregate under Projection"),
        },
        _ => panic!("expected Projection"),
    }
}

#[test]
fn test_multi_pass_pipeline() {
    let plan = LogicalPlan::Limit {
        limit: 10, offset: 0,
        input: Box::new(project(
            vec![col("a")],
            filter(
                Expr::BinaryOp { op: BinaryOp::Gt, left: Box::new(col("a")), right: Box::new(int(1)), span: span() },
                scan("t"),
            ),
        )),
    };
    let (optimized, fired) = optimize(&plan, None);
    assert!(!fired.is_empty());
    assert!(fired.contains(&"limit_pushdown"));
    assert!(fired.contains(&"filter_pushdown"));
    assert!(fired.contains(&"projection_pushdown"));
    match optimized {
        LogicalPlan::Scan { ref filters, limit, projection, .. } => {
            assert_eq!(filters.len(), 1);
            assert_eq!(limit, Some(10));
            assert_eq!(projection, Some(vec!["a".to_string()]));
        }
        other => panic!("expected Scan, got {:?}", other),
    }
}

#[test]
fn test_optimizer_convergence() {
    let plan = LogicalPlan::Limit {
        limit: 10, offset: 0,
        input: Box::new(project(
            vec![col("a")],
            filter(
                Expr::BinaryOp { op: BinaryOp::Gt, left: Box::new(col("a")), right: Box::new(int(1)), span: span() },
                scan("t"),
            ),
        )),
    };
    let (_, fired) = optimize(&plan, Some(100));
    assert!(fired.len() < 50, "optimizer should converge");
}

#[test]
fn test_noop_passthrough() {
    let plan = LogicalPlan::Aggregate {
        group_expr: vec![col("x")],
        aggregate_expr: vec![],
        input: Box::new(scan("t")),
    };
    let (optimized, fired) = optimize(&plan, None);
    assert!(fired.is_empty());
    match optimized {
        LogicalPlan::Aggregate { .. } => {}
        _ => panic!("expected Aggregate"),
    }
}

// --- TPC-H-based optimizer tests ---

#[test]
fn test_tpch_q6_between_typed_string_and_pushdown() {
    // TPC-H Q6: WHERE l_shipdate >= DATE '1994-01-01'
    //              AND l_shipdate < DATE '1995-01-01'
    //              AND l_discount BETWEEN 0.05 AND 0.07
    //              AND l_quantity < 24
    // All 4 predicates are now pushable (BETWEEN + TypedString support added)
    let pred = and(
        and(
            ge(col("l_shipdate"), typed_string_date("1994-01-01")),
            lt(col("l_shipdate"), typed_string_date("1995-01-01")),
        ),
        and(
            between(col("l_discount"), float(0.05), float(0.07)),
            lt(col("l_quantity"), int(24)),
        ),
    );
    let plan = filter(pred, scan("lineitem"));
    let (optimized, fired) = optimize(&plan, None);
    assert!(fired.contains(&"filter_pushdown"), "filter pushdown should fire");
    let filter_count = count_filter_nodes(&optimized);
    assert_eq!(filter_count, 0, "all predicates should be absorbed into Scan");
    match optimized {
        LogicalPlan::Scan { ref filters, .. } => {
            assert_eq!(filters.len(), 4, "all 4 predicates should be in Scan.filters");
        }
        other => panic!("expected Scan, got {:?}", other),
    }
}

#[test]
fn test_tpch_q12_inlist_and_typed_string_partial_pushdown() {
    // TPC-H Q12-like: WHERE l_shipmode IN ('MAIL', 'SHIP')
    //                    AND l_commitdate < l_receiptdate
    //                    AND l_receiptdate >= DATE '1994-01-01'
    // InList + column comparison are pushable.
    // TypedString comparison is now also pushable.
    let pred = and(
        and(
            in_list(col("l_shipmode"), vec![strlit("MAIL"), strlit("SHIP")]),
            lt(col("l_commitdate"), col("l_receiptdate")),
        ),
        ge(col("l_receiptdate"), typed_string_date("1994-01-01")),
    );
    let plan = filter(pred, scan("lineitem"));
    let (optimized, fired) = optimize(&plan, None);
    assert!(fired.contains(&"filter_pushdown"), "filter pushdown should fire");
    let filter_count = count_filter_nodes(&optimized);
    assert_eq!(filter_count, 0, "all predicates are pushable and should be absorbed");
    match optimized {
        LogicalPlan::Scan { ref filters, .. } => {
            assert_eq!(filters.len(), 3, "all 3 predicates should be in Scan.filters");
        }
        other => panic!("expected Scan, got {:?}", other),
    }
}

#[test]
fn test_filter_remaining_preserved_through_aggregate() {
    // CRITICAL: Filter with pushable predicates above Aggregate
    // should keep ALL predicates (no silent dropping of remaining).
    let pred = and(
        gt(col("x"), int(5)),
        Expr::BinaryOp {
            op: BinaryOp::Gt,
            left: Box::new(Expr::BinaryOp {
                op: BinaryOp::Plus, left: Box::new(col("x")), right: Box::new(int(1)), span: span(),
            }),
            right: Box::new(int(10)), span: span(),
        },
    );
    let agg = LogicalPlan::Aggregate {
        group_expr: vec![col("x")],
        aggregate_expr: vec![],
        input: Box::new(scan("t")),
    };
    let plan = filter(pred, agg);
    let (optimized, _fired) = optimize(&plan, None);
    // Aggregate blocks pushdown — Filter must remain with ALL predicates
    match optimized {
        LogicalPlan::Filter { predicate, input } => {
            match *input {
                LogicalPlan::Aggregate { .. } => {
                    // Verify predicate contains both conjuncts (not just the pushable one)
                    let conjuncts = split_conjunction_for_test(&predicate);
                    assert_eq!(conjuncts.len(), 2,
                        "Filter above Aggregate must retain BOTH predicates, not just the pushable one");
                }
                other => panic!("expected Aggregate under Filter, got {:?}", other),
            }
        }
        other => panic!("expected Filter, got {:?}", other),
    }
}

fn split_conjunction_for_test(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryOp { op: BinaryOp::And, left, right, .. } => {
            let mut result = split_conjunction_for_test(left);
            result.extend(split_conjunction_for_test(right));
            result
        }
        other => vec![other.clone()],
    }
}

#[test]
fn test_or_predicate_not_pushed() {
    // OR predicates cannot be split into conjunctions — entire Filter stays
    let pred = or(
        eq(col("a"), int(1)),
        eq(col("b"), int(2)),
    );
    let plan = filter(pred.clone(), scan("t"));
    let (optimized, _fired) = optimize(&plan, None);
    // filter_pushdown may fire with no effect (no AND to split)
    let filter_count = count_filter_nodes(&optimized);
    assert_eq!(filter_count, 1, "OR predicate should remain as Filter");
    match optimized {
        LogicalPlan::Filter { predicate, input } => {
            assert_eq!(predicate, pred, "OR predicate must be unchanged");
            assert!(matches!(*input, LogicalPlan::Scan { .. }), "input should still be Scan");
        }
        other => panic!("expected Filter, got {:?}", other),
    }
}

#[test]
fn test_between_pushdown() {
    let pred = between(col("a"), int(1), int(10));
    let plan = filter(pred, scan("t"));
    let (optimized, fired) = optimize(&plan, None);
    assert!(fired.contains(&"filter_pushdown"), "BETWEEN should be pushable");
    let filter_count = count_filter_nodes(&optimized);
    assert_eq!(filter_count, 0, "BETWEEN should be absorbed into Scan");
    match optimized {
        LogicalPlan::Scan { ref filters, .. } => {
            assert_eq!(filters.len(), 1);
        }
        other => panic!("expected Scan, got {:?}", other),
    }
}

#[test]
fn test_typed_string_pushdown() {
    let pred = ge(col("l_shipdate"), typed_string_date("1994-01-01"));
    let plan = filter(pred, scan("lineitem"));
    let (optimized, fired) = optimize(&plan, None);
    assert!(fired.contains(&"filter_pushdown"), "TypedString comparison should be pushable");
    let filter_count = count_filter_nodes(&optimized);
    assert_eq!(filter_count, 0, "TypedString comparison should be absorbed into Scan");
    match optimized {
        LogicalPlan::Scan { ref filters, .. } => {
            assert_eq!(filters.len(), 1);
        }
        other => panic!("expected Scan, got {:?}", other),
    }
}

#[test]
fn test_limit_through_subquery_alias() {
    let plan = LogicalPlan::Limit {
        limit: 10, offset: 0,
        input: Box::new(LogicalPlan::SubqueryAlias {
            alias: "t1".into(),
            input: Box::new(scan("t")),
        }),
    };
    let (optimized, fired) = optimize(&plan, None);
    assert!(fired.contains(&"limit_pushdown"));
    // Limit pushed through SubqueryAlias into Scan; SubqueryAlias remains
    match optimized {
        LogicalPlan::SubqueryAlias { alias, input } => {
            assert_eq!(alias, "t1");
            match *input {
                LogicalPlan::Scan { limit, .. } => assert_eq!(limit, Some(10)),
                other => panic!("expected Scan under SubqueryAlias, got {:?}", other),
            }
        }
        other => panic!("expected SubqueryAlias, got {:?}", other),
    }
}

#[test]
fn test_subquery_alias_filter_pushdown() {
    // Filter -> SubqueryAlias -> Scan: filter pushes through alias into Scan
    let plan = filter(
        eq(col("a"), int(1)),
        LogicalPlan::SubqueryAlias {
            alias: "t1".into(),
            input: Box::new(scan("t")),
        },
    );
    let (optimized, fired) = optimize(&plan, None);
    assert!(fired.contains(&"filter_pushdown"), "filter should push through SubqueryAlias");
    let filter_count = count_filter_nodes(&optimized);
    assert_eq!(filter_count, 0, "filter should be fully absorbed");
    // SubqueryAlias remains with filter in Scan
    match optimized {
        LogicalPlan::SubqueryAlias { alias, input } => {
            assert_eq!(alias, "t1");
            match *input {
                LogicalPlan::Scan { ref filters, .. } => assert_eq!(filters.len(), 1),
                other => panic!("expected Scan under SubqueryAlias, got {:?}", other),
            }
        }
        other => panic!("expected SubqueryAlias, got {:?}", other),
    }
}

#[test]
fn test_table_alias_filter_pushdown() {
    // Filter -> TableAlias -> Scan: filter pushes through alias into Scan
    let plan = filter(
        eq(col("a"), int(1)),
        LogicalPlan::TableAlias {
            alias: "t1".into(),
            input: Box::new(scan("t")),
        },
    );
    let (optimized, fired) = optimize(&plan, None);
    assert!(fired.contains(&"filter_pushdown"), "filter should push through TableAlias");
    let filter_count = count_filter_nodes(&optimized);
    assert_eq!(filter_count, 0, "filter should be fully absorbed");
    // TableAlias remains with filter in Scan
    match optimized {
        LogicalPlan::TableAlias { alias, input } => {
            assert_eq!(alias, "t1");
            match *input {
                LogicalPlan::Scan { ref filters, .. } => assert_eq!(filters.len(), 1),
                other => panic!("expected Scan under TableAlias, got {:?}", other),
            }
        }
        other => panic!("expected TableAlias, got {:?}", other),
    }
}