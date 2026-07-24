use crate::ast::expr::Expr;
use crate::logical_plan::{LogicalPlan, PlanError, Transformed, TreeNode};
use crate::optimizer::OptimizerRule;

pub struct ProjectionPushdown;

impl OptimizerRule for ProjectionPushdown {
    fn name(&self) -> &'static str {
        "projection_pushdown"
    }

    fn apply(&self, plan: &LogicalPlan) -> Result<Transformed<LogicalPlan>, PlanError> {
        let result = plan
            .clone()
            .transform_up(&|node| -> Result<Transformed<LogicalPlan>, Box<dyn std::error::Error>> {
                match node {
                    LogicalPlan::Projection { ref expr, input } => {
                        match *input {
                            LogicalPlan::Scan { source, filters, limit, .. } => {
                                let cols = extract_column_names(expr);
                                let projection = if cols.is_empty() { None } else { Some(cols) };
                                Ok(Transformed::yes(LogicalPlan::Scan { source, projection, filters, limit }))
                            }
                            LogicalPlan::Aggregate { group_expr, aggregate_expr, input: agg_input } => {
                                match *agg_input {
                                    LogicalPlan::Scan { source, filters, limit, .. } => {
                                        let group_cols = extract_column_names(&group_expr);
                                        let aggr_cols = extract_column_names(&aggregate_expr);

                                        let mut needed = group_cols.clone();
                                        for c in &aggr_cols {
                                            if !needed.contains(c) {
                                                needed.push(c.clone());
                                            }
                                        }

                                        let projection = if needed.is_empty() { None } else { Some(needed) };
                                        let scan = LogicalPlan::Scan { source, projection, filters, limit };
                                        let agg = LogicalPlan::Aggregate {
                                            group_expr, aggregate_expr,
                                            input: Box::new(scan),
                                        };
                                        Ok(Transformed::yes(LogicalPlan::Projection {
                                            expr: expr.clone(),
                                            input: Box::new(agg),
                                        }))
                                    }
                                    other => Ok(Transformed::no(LogicalPlan::Projection {
                                        expr: expr.clone(),
                                        input: Box::new(LogicalPlan::Aggregate {
                                            group_expr, aggregate_expr,
                                            input: Box::new(other),
                                        }),
                                    })),
                                }
                            }
                            other => Ok(Transformed::no(LogicalPlan::Projection {
                                expr: expr.clone(),
                                input: Box::new(other),
                            })),
                        }
                    }
                    _ => Ok(Transformed::no(node)),
                }
            })
            .map_err(|e| PlanError::Internal(e.to_string()))?;
        Ok(result)
    }
}

fn extract_column_names(exprs: &[crate::ast::expr::SpannedExpr]) -> Vec<String> {
    let mut cols = Vec::new();
    for e in exprs {
        collect_columns(e, &mut cols);
    }
    cols
}

use crate::ast::expr::SpannedExpr;
fn collect_columns(expr: &SpannedExpr, cols: &mut Vec<String>) {
    match &expr.node {
        Expr::Column { column, .. } => {
            if !cols.contains(column) {
                cols.push(column.clone());
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_columns(left, cols);
            collect_columns(right, cols);
        }
        Expr::UnaryOp { expr: inner, .. } => collect_columns(inner.as_ref(), cols),
        Expr::Function { args, .. } => {
            for a in args {
                collect_columns(a, cols);
            }
        }
        Expr::WindowFunction { args, partition_by, order_by, .. } => {
            for a in args {
                collect_columns(a, cols);
            }
            for p in partition_by {
                collect_columns(p, cols);
            }
            for o in order_by {
                collect_columns(&o.expr, cols);
            }
        }
        Expr::Cast { expr: inner, .. } => collect_columns(inner.as_ref(), cols),
        Expr::Alias { expr: inner, .. } => collect_columns(inner.as_ref(), cols),
        Expr::Between { expr: inner, low, high, .. } => {
            collect_columns(inner.as_ref(), cols);
            collect_columns(low.as_ref(), cols);
            collect_columns(high.as_ref(), cols);
        }
        Expr::InList { expr: inner, list, .. } => {
            collect_columns(inner.as_ref(), cols);
            for l in list {
                collect_columns(l, cols);
            }
        }
        Expr::Case { operands, results, .. } => {
            for o in operands {
                collect_columns(o, cols);
            }
            for r in results {
                collect_columns(r, cols);
            }
        }
        _ => {}
    }
}
