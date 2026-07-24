use crate::ast::expr::{BinaryOp, Expr, UnaryOp};
use crate::logical_plan::{LogicalPlan, PlanError, Transformed, TreeNode};
use crate::optimizer::OptimizerRule;

pub struct FilterPushdown;

impl OptimizerRule for FilterPushdown {
    fn name(&self) -> &'static str {
        "filter_pushdown"
    }

    fn apply(&self, plan: &LogicalPlan) -> Result<Transformed<LogicalPlan>, PlanError> {
        let result = plan
            .clone()
            .transform_down(&|node| -> Result<Transformed<LogicalPlan>, Box<dyn std::error::Error>> {
                match node {
                    LogicalPlan::Filter { predicate, input } => {
                        let conjuncts = split_conjunction(&predicate);
                        let (pushable, remaining): (Vec<_>, Vec<_>) = conjuncts
                            .into_iter()
                            .partition(is_pushable_into_scan);

                        if pushable.is_empty() {
                            // Nothing to push — Filter stays
                            return Ok(Transformed::no(LogicalPlan::Filter { predicate, input }));
                        }

                        match *input {
                            LogicalPlan::Scan { source, projection, filters, limit } => {
                                let mut merged = filters;
                                merged.extend(pushable);
                                let scan = LogicalPlan::Scan { source, projection, filters: merged, limit };
                                if remaining.is_empty() {
                                    Ok(Transformed::yes(scan))
                                } else {
                                    Ok(Transformed::yes(LogicalPlan::Filter {
                                        predicate: combine_conjunction(&remaining),
                                        input: Box::new(scan),
                                    }))
                                }
                            }
                            LogicalPlan::Projection { expr, input: child } => {
                                let new_child = push_predicates_through(*child, &pushable)?;
                                let proj = LogicalPlan::Projection { expr, input: Box::new(new_child) };
                                if remaining.is_empty() {
                                    Ok(Transformed::yes(proj))
                                } else {
                                    Ok(Transformed::yes(LogicalPlan::Filter {
                                        predicate: combine_conjunction(&remaining),
                                        input: Box::new(proj),
                                    }))
                                }
                            }
                            LogicalPlan::TableAlias { alias, input: child } => {
                                let new_child = push_predicates_through(*child, &pushable)?;
                                let alias_node = LogicalPlan::TableAlias { alias, input: Box::new(new_child) };
                                if remaining.is_empty() {
                                    Ok(Transformed::yes(alias_node))
                                } else {
                                    Ok(Transformed::yes(LogicalPlan::Filter {
                                        predicate: combine_conjunction(&remaining),
                                        input: Box::new(alias_node),
                                    }))
                                }
                            }
                            LogicalPlan::SubqueryAlias { alias, input: child } => {
                                let new_child = push_predicates_through(*child, &pushable)?;
                                let alias_node = LogicalPlan::SubqueryAlias { alias, input: Box::new(new_child) };
                                if remaining.is_empty() {
                                    Ok(Transformed::yes(alias_node))
                                } else {
                                    Ok(Transformed::yes(LogicalPlan::Filter {
                                        predicate: combine_conjunction(&remaining),
                                        input: Box::new(alias_node),
                                    }))
                                }
                            }
                            other => {
                                // Can't push through this node type (e.g. Aggregate, Join, Alias)
                                // Keep ALL predicates (pushable + remaining) since none were pushed
                                let mut all = remaining;
                                all.extend(pushable);
                                Ok(Transformed::no(LogicalPlan::Filter {
                                    predicate: combine_conjunction(&all),
                                    input: Box::new(other),
                                }))
                            }
                        }
                    }
                    _ => Ok(Transformed::no(node)),
                }
            })
            .map_err(|e| PlanError::Internal(e.to_string()))?;
        Ok(result)
    }
}

/// Recursively push predicates through Projection nodes into Scan.
/// For non-Scan children, wraps with a Filter (which `transform_down`
/// will re-process — if the child isn't pushable, it converges with `no`).
fn push_predicates_through(input: LogicalPlan, pushable: &[Expr]) -> Result<LogicalPlan, Box<dyn std::error::Error>> {
    match input {
        LogicalPlan::Scan { source, projection, filters, limit } => {
            let mut merged = filters;
            merged.extend(pushable.iter().cloned());
            Ok(LogicalPlan::Scan { source, projection, filters: merged, limit })
        }
        LogicalPlan::Projection { expr, input: child } => {
            let new_child = push_predicates_through(*child, pushable)?;
            Ok(LogicalPlan::Projection { expr, input: Box::new(new_child) })
        }
        LogicalPlan::TableAlias { alias, input: child } => {
            let new_child = push_predicates_through(*child, pushable)?;
            Ok(LogicalPlan::TableAlias { alias, input: Box::new(new_child) })
        }
        LogicalPlan::SubqueryAlias { alias, input: child } => {
            let new_child = push_predicates_through(*child, pushable)?;
            Ok(LogicalPlan::SubqueryAlias { alias, input: Box::new(new_child) })
        }
        other => {
            // Can't push into this node (e.g. Alias, Aggregate, Join).
            // Wrap with Filter — transform_down will re-process this.
            Ok(LogicalPlan::Filter {
                predicate: combine_conjunction(pushable),
                input: Box::new(other),
            })
        }
    }
}

fn is_pushable_into_scan(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryOp { op, left, right, .. } => {
            let is_comparison = matches!(op,
                BinaryOp::Eq | BinaryOp::Neq | BinaryOp::Lt
                | BinaryOp::Gt | BinaryOp::Leq | BinaryOp::Geq
            );
            is_comparison && is_simple_expr(left) && is_simple_expr(right)
        }
        Expr::UnaryOp { op, .. } => matches!(op, UnaryOp::IsNull | UnaryOp::IsNotNull),
        Expr::InList { expr: inner, list, .. } => {
            is_simple_expr(inner) && list.iter().all(|e| is_simple_expr(e))
        }
        Expr::Between { expr: inner, low, high, .. } => {
            is_simple_expr(inner) && is_simple_expr(low) && is_simple_expr(high)
        }
        _ => false,
    }
}

fn is_simple_expr(expr: &Expr) -> bool {
    matches!(expr,
        Expr::Column { .. } | Expr::Literal(..) | Expr::Wildcard { .. } | Expr::TypedString { .. }
    )
}

fn split_conjunction(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryOp { op: BinaryOp::And, left, right, .. } => {
            let mut result = split_conjunction(left);
            result.extend(split_conjunction(right));
            result
        }
        other => vec![other.clone()],
    }
}

fn combine_conjunction(exprs: &[Expr]) -> Expr {
    if exprs.is_empty() {
        return Expr::Literal(crate::ast::expr::Literal::Boolean(true), Default::default());
    }
    let mut iter = exprs.iter();
    let first = iter.next().unwrap().clone();
    iter.fold(first, |acc, e| {
        Expr::BinaryOp {
            left: Box::new(acc),
            op: BinaryOp::And,
            right: Box::new(e.clone()),
            span: Default::default(),
        }
    })
}
