mod filter_pushdown;
mod limit_pushdown;
mod projection_pushdown;

pub use filter_pushdown::FilterPushdown;
pub use limit_pushdown::LimitPushdown;
pub use projection_pushdown::ProjectionPushdown;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::{LogicalPlan, ScanSource};
    use crate::ast::expr::{BinaryOp, Expr, Literal};
    use crate::optimizer::OptimizerRule;
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

    #[test]
    fn test_filter_into_scan() {
        let plan = filter(Expr::BinaryOp { op: BinaryOp::Gt, left: Box::new(col("a")), right: Box::new(int(1)), span: span() }, scan("t"));
        let result = FilterPushdown.apply(&plan).unwrap();
        assert!(result.transformed);
        match result.data {
            LogicalPlan::Scan { ref filters, .. } => {
                assert_eq!(filters.len(), 1);
            }
            _ => panic!("expected Scan"),
        }
    }

    #[test]
    fn test_filter_below_projection() {
        let plan = filter(
            Expr::BinaryOp { op: BinaryOp::Eq, left: Box::new(col("a")), right: Box::new(int(1)), span: span() },
            project(vec![col("a"), col("b")], scan("t")),
        );
        let result = FilterPushdown.apply(&plan).unwrap();
        assert!(result.transformed);
        // Filter a=1 is pushable into Scan through Projection
        match result.data {
            LogicalPlan::Projection { input, .. } => {
                match *input {
                    LogicalPlan::Scan { ref filters, .. } => {
                        assert_eq!(filters.len(), 1);
                    }
                    _ => panic!("expected Scan under Projection"),
                }
            }
            _ => panic!("expected Projection"),
        }
    }

    #[test]
    fn test_limit_below_projection() {
        let plan = LogicalPlan::Limit {
            limit: 10,
            offset: 0,
            input: Box::new(project(vec![col("a")], scan("t"))),
        };
        let result = LimitPushdown.apply(&plan).unwrap();
        assert!(result.transformed);
        match result.data {
            LogicalPlan::Projection { input, .. } => {
                match *input {
                    LogicalPlan::Limit { limit, .. } => {
                        assert_eq!(limit, 10);
                    }
                    _ => panic!("expected Limit under Projection"),
                }
            }
            _ => panic!("expected Projection"),
        }
    }

    #[test]
    fn test_limit_pushdown_skips_with_offset() {
        let plan = LogicalPlan::Limit {
            limit: 10,
            offset: 5,
            input: Box::new(project(vec![col("a")], scan("t"))),
        };
        let result = LimitPushdown.apply(&plan).unwrap();
        assert!(!result.transformed);
    }

    #[test]
    fn test_projection_into_scan() {
        let plan = project(vec![col("a"), col("b")], scan("t"));
        let result = ProjectionPushdown.apply(&plan).unwrap();
        assert!(result.transformed);
        match result.data {
            LogicalPlan::Scan { projection, .. } => {
                let cols = projection.unwrap();
                assert_eq!(cols, vec!["a", "b"]);
            }
            _ => panic!("expected Scan"),
        }
    }

    #[test]
    fn test_multiple_pass_pipeline() {
        let plan = LogicalPlan::Limit {
            limit: 10,
            offset: 0,
            input: Box::new(project(
                vec![col("a"), col("b")],
                filter(
                    Expr::BinaryOp { op: BinaryOp::Gt, left: Box::new(col("a")), right: Box::new(int(1)), span: span() },
                    scan("t"),
                ),
            )),
        };
        let (result, fired) = crate::optimizer::optimize(&plan, None);
        assert!(!fired.is_empty());
        // All three rules fire; everything absorbed into Scan
        match result {
            LogicalPlan::Scan { ref filters, limit, projection, .. } => {
                assert_eq!(filters.len(), 1);
                assert_eq!(limit, Some(10));
                assert_eq!(projection, Some(vec!["a".to_string(), "b".to_string()]));
            }
            _ => panic!("expected Scan"),
        }
    }

    #[test]
    fn test_filter_partial_pushdown() {
        // Filter with a mix of pushable (col > 1) and non-pushable (col + 1 > 2) predicates
        let and_pred = Expr::BinaryOp {
            op: BinaryOp::And,
            left: Box::new(Expr::BinaryOp {
                op: BinaryOp::Gt, left: Box::new(col("a")), right: Box::new(int(1)), span: span(),
            }),
            right: Box::new(Expr::BinaryOp {
                op: BinaryOp::Gt,
                left: Box::new(Expr::BinaryOp {
                    op: BinaryOp::Plus, left: Box::new(col("a")), right: Box::new(int(1)), span: span(),
                }),
                right: Box::new(int(2)), span: span(),
            }),
            span: span(),
        };
        let plan = filter(and_pred, scan("t"));
        let result = FilterPushdown.apply(&plan).unwrap();
        assert!(result.transformed);
        // Should have a Filter remaining (non-pushable part) above a Scan with one filter
        match result.data {
            LogicalPlan::Filter { input, .. } => {
                match *input {
                    LogicalPlan::Scan { ref filters, .. } => {
                        assert_eq!(filters.len(), 1);
                    }
                    _ => panic!("expected Scan under Filter"),
                }
            }
            other => panic!("expected Filter, got {:?}", other),
        }
    }

    #[test]
    fn test_filter_non_pushable_retained() {
        // Filter with only non-pushable predicates
        let expr = Expr::BinaryOp {
            op: BinaryOp::Gt,
            left: Box::new(Expr::BinaryOp {
                op: BinaryOp::Plus, left: Box::new(col("a")), right: Box::new(int(1)), span: span(),
            }),
            right: Box::new(int(2)), span: span(),
        };
        let plan = filter(expr.clone(), scan("t"));
        let result = FilterPushdown.apply(&plan).unwrap();
        assert!(!result.transformed);
    }

    #[test]
    fn test_limit_into_scan() {
        let plan = LogicalPlan::Limit { limit: 10, offset: 0, input: Box::new(scan("t")) };
        let result = LimitPushdown.apply(&plan).unwrap();
        assert!(result.transformed);
        match result.data {
            LogicalPlan::Scan { limit, .. } => {
                assert_eq!(limit, Some(10));
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
                aggregate_expr: vec![col("y")],
                input: Box::new(scan("t")),
            },
        );
        let result = ProjectionPushdown.apply(&plan).unwrap();
        assert!(result.transformed);
        match result.data {
            LogicalPlan::Projection { input, .. } => match *input {
                LogicalPlan::Aggregate { input: agg_input, .. } => match *agg_input {
                    LogicalPlan::Scan { projection, .. } => {
                        let cols = projection.unwrap();
                        assert!(cols.contains(&"x".to_string()));
                        assert!(cols.contains(&"y".to_string()));
                    }
                    _ => panic!("expected Scan under Aggregate"),
                },
                _ => panic!("expected Aggregate under Projection"),
            },
            _ => panic!("expected Projection"),
        }
    }

    #[test]
    fn test_optimizer_convergence() {
        let plan = LogicalPlan::Limit {
            limit: 10,
            offset: 0,
            input: Box::new(project(
                vec![col("a")],
                filter(
                    Expr::BinaryOp { op: BinaryOp::Gt, left: Box::new(col("a")), right: Box::new(int(1)), span: span() },
                    scan("t"),
                ),
            )),
        };
        let (_, fired) = crate::optimizer::optimize(&plan, Some(100));
        assert!(fired.len() < 50, "optimizer should converge");
    }
}
