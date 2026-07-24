use crate::logical_plan::{LogicalPlan, PlanError, Transformed, TreeNode};
use crate::optimizer::OptimizerRule;

pub struct LimitPushdown;

impl OptimizerRule for LimitPushdown {
    fn name(&self) -> &'static str {
        "limit_pushdown"
    }

    fn apply(&self, plan: &LogicalPlan) -> Result<Transformed<LogicalPlan>, PlanError> {
        let result = plan
            .clone()
            .transform_up(&|node| -> Result<Transformed<LogicalPlan>, Box<dyn std::error::Error>> {
                match node {
                    LogicalPlan::Limit { limit, offset, input } if offset == 0 => match *input {
                        LogicalPlan::Scan { source, projection, filters, .. } => {
                            Ok(Transformed::yes(LogicalPlan::Scan {
                                source,
                                projection,
                                filters,
                                limit: Some(limit),
                            }))
                        }
                        LogicalPlan::Projection { expr, input: child } => {
                            Ok(Transformed::yes(LogicalPlan::Projection {
                                expr,
                                input: Box::new(LogicalPlan::Limit { limit, offset: 0, input: child }),
                            }))
                        }
                        LogicalPlan::Filter { predicate, input: child } => {
                            Ok(Transformed::yes(LogicalPlan::Filter {
                                predicate,
                                input: Box::new(LogicalPlan::Limit { limit, offset: 0, input: child }),
                            }))
                        }
                        LogicalPlan::Extend { items, input: child } => {
                            Ok(Transformed::yes(LogicalPlan::Extend {
                                items,
                                input: Box::new(LogicalPlan::Limit { limit, offset: 0, input: child }),
                            }))
                        }
                        LogicalPlan::TableAlias { alias, input: child } => {
                            Ok(Transformed::yes(LogicalPlan::TableAlias {
                                alias,
                                input: Box::new(LogicalPlan::Limit { limit, offset: 0, input: child }),
                            }))
                        }
                        LogicalPlan::SubqueryAlias { alias, input: child } => {
                            Ok(Transformed::yes(LogicalPlan::SubqueryAlias {
                                alias,
                                input: Box::new(LogicalPlan::Limit { limit, offset: 0, input: child }),
                            }))
                        }
                        other => Ok(Transformed::no(LogicalPlan::Limit {
                            limit, offset, input: Box::new(other),
                        })),
                    },
                    _ => Ok(Transformed::no(node)),
                }
            })
            .map_err(|e| PlanError::Internal(e.to_string()))?;
        Ok(result)
    }
}
