//! Equality-based predicate transitivity for inner joins.

use std::collections::{HashMap, HashSet};

use error::Result;
use parser::ast::expr::BinaryOp;
use parser::ast::pipe_op::JoinType;
use plan::{LogicalExpr, LogicalPlan, PlanTraversal, VeinSchema};

use crate::context::OptimizerContext;
use crate::rule::{OptimizerRule, RuleType};

use super::predicate_utils::{referenced_columns, substitute_columns};

/// Propagates a deterministic side-local predicate across inner equi-join keys.
///
/// For example, an input predicate `a.id < 10` and join condition
/// `a.id = b.id` produce filters `a.id < 10` and `b.id < 10`.
/// The rule deliberately does not rewrite outer joins, non-equality joins,
/// correlated expressions, or predicates that reference both inputs.
pub struct JoinPredicateTransitivityRule;

impl OptimizerRule for JoinPredicateTransitivityRule {
    fn name(&self) -> &str {
        "JoinPredicateTransitivityRule"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Heuristic
    }

    fn apply(
        &self,
        plan: &LogicalPlan,
        _context: &OptimizerContext,
    ) -> Result<Option<LogicalPlan>> {
        let LogicalPlan::Filter { input, predicate } = plan else {
            return Ok(None);
        };
        let LogicalPlan::Join {
            join_type: JoinType::Inner,
            left,
            right,
            on: Some(on),
            using,
            schema,
        } = input.as_ref()
        else {
            return Ok(None);
        };

        let referenced = referenced_columns(predicate).unwrap_or_default();
        if referenced.is_empty() {
            return Ok(None);
        }

        let left_ids = schema_ids(left.schema());
        let right_ids = schema_ids(right.schema());
        let keys = equi_join_keys(on, &left_ids, &right_ids);
        if keys.is_empty() {
            return Ok(None);
        }

        let (source_ids, target_ids, source_is_left) = if referenced.is_subset(&left_ids) {
            (&left_ids, &right_ids, true)
        } else if referenced.is_subset(&right_ids) {
            (&right_ids, &left_ids, false)
        } else {
            return Ok(None);
        };

        let mut derived = None;
        for (left_key, right_key) in keys {
            let (source_key, target_key) = if source_is_left {
                (left_key, right_key)
            } else {
                (right_key, left_key)
            };
            if !source_ids.contains(&source_key) || !target_ids.contains(&target_key) {
                continue;
            }
            if !referenced.contains(&source_key) {
                continue;
            }

            let Some(target_column) = column_for_id(
                &target_key,
                if source_is_left {
                    right.schema()
                } else {
                    left.schema()
                },
            ) else {
                continue;
            };
            let source_schema = if source_is_left {
                left.schema()
            } else {
                right.schema()
            };
            let mut mapping = source_schema
                .fields
                .iter()
                .map(|field| (field.id, LogicalExpr::Column(field.clone())))
                .collect::<HashMap<_, _>>();
            mapping.insert(source_key, LogicalExpr::Column(target_column));
            derived = substitute_columns(predicate, &mapping);
            if derived.is_some() {
                break;
            }
        }

        let Some(derived) = derived else {
            return Ok(None);
        };

        let (new_left, new_right) = if source_is_left {
            (
                LogicalPlan::Filter {
                    input: left.clone(),
                    predicate: predicate.clone(),
                },
                LogicalPlan::Filter {
                    input: right.clone(),
                    predicate: derived,
                },
            )
        } else {
            (
                LogicalPlan::Filter {
                    input: left.clone(),
                    predicate: derived,
                },
                LogicalPlan::Filter {
                    input: right.clone(),
                    predicate: predicate.clone(),
                },
            )
        };

        Ok(Some(LogicalPlan::Join {
            join_type: JoinType::Inner,
            left: Box::new(new_left),
            right: Box::new(new_right),
            on: Some(on.clone()),
            using: using.clone(),
            schema: schema.clone(),
        }))
    }
}

fn equi_join_keys(
    expr: &LogicalExpr,
    left_ids: &HashSet<plan::ColumnId>,
    right_ids: &HashSet<plan::ColumnId>,
) -> Vec<(plan::ColumnId, plan::ColumnId)> {
    match expr {
        LogicalExpr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
            ..
        } => {
            let mut keys = equi_join_keys(left, left_ids, right_ids);
            keys.extend(equi_join_keys(right, left_ids, right_ids));
            keys
        }
        LogicalExpr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
            ..
        } => {
            let (LogicalExpr::Column(left_column), LogicalExpr::Column(right_column)) =
                (left.as_ref(), right.as_ref())
            else {
                return Vec::new();
            };
            if left_column.data_type != right_column.data_type {
                return Vec::new();
            }
            if left_ids.contains(&left_column.id) && right_ids.contains(&right_column.id) {
                vec![(left_column.id, right_column.id)]
            } else if left_ids.contains(&right_column.id) && right_ids.contains(&left_column.id) {
                vec![(right_column.id, left_column.id)]
            } else {
                Vec::new()
            }
        }
        _ => Vec::new(),
    }
}

fn schema_ids(schema: &VeinSchema) -> HashSet<plan::ColumnId> {
    schema.fields.iter().map(|field| field.id).collect()
}

fn column_for_id(id: &plan::ColumnId, schema: &VeinSchema) -> Option<plan::ResolvedColumnRef> {
    schema.fields.iter().find(|field| field.id == *id).cloned()
}

#[cfg(test)]
mod tests;
