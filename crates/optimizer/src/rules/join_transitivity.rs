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

fn column_for_id(id: &plan::ColumnId, schema: &VeinSchema) -> Option<plan::ResolvedColumn> {
    schema.fields.iter().find(|field| field.id == *id).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use parser::ast::expr::BinaryOp;
    use plan::{ColumnId, LogicalLiteral, ResolvedColumn, TableMetadata};

    fn field(id: usize, name: &str) -> ResolvedColumn {
        ResolvedColumn::new(
            ColumnId(id),
            name,
            Some("t".to_string()),
            plan::VeinDataType::Int64,
        )
    }

    fn scan(fields: Vec<ResolvedColumn>) -> LogicalPlan {
        let schema = std::sync::Arc::new(VeinSchema {
            fields: fields.clone(),
        });
        LogicalPlan::Scan {
            table: std::sync::Arc::new(TableMetadata::new("t", schema)),
            projection: None,
            filters: vec![],
            schema: Box::new(VeinSchema { fields }),
        }
    }

    fn column(field: &ResolvedColumn) -> LogicalExpr {
        LogicalExpr::Column(field.clone())
    }

    fn binary(left: LogicalExpr, op: BinaryOp, right: LogicalExpr) -> LogicalExpr {
        LogicalExpr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
            return_type: plan::VeinDataType::Boolean,
        }
    }

    fn join(left: &ResolvedColumn, right: &ResolvedColumn, join_type: JoinType) -> LogicalPlan {
        let left_plan = scan(vec![left.clone()]);
        let right_plan = scan(vec![right.clone()]);
        LogicalPlan::Join {
            join_type,
            left: Box::new(left_plan),
            right: Box::new(right_plan),
            on: Some(binary(column(left), BinaryOp::Eq, column(right))),
            using: vec![],
            schema: Box::new(VeinSchema {
                fields: vec![left.clone(), right.clone()],
            }),
        }
    }

    #[test]
    fn propagates_left_predicate_to_right() {
        let left = field(1, "a_id");
        let right = field(2, "b_id");
        let plan = LogicalPlan::Filter {
            input: Box::new(join(&left, &right, JoinType::Inner)),
            predicate: binary(
                column(&left),
                BinaryOp::Lt,
                LogicalExpr::Literal(LogicalLiteral::Int64(10)),
            ),
        };

        let rewritten = JoinPredicateTransitivityRule
            .apply(&plan, &OptimizerContext::default())
            .unwrap()
            .expect("predicate should propagate");

        let LogicalPlan::Join { right, .. } = rewritten else {
            panic!("expected join");
        };
        let LogicalPlan::Filter { predicate, .. } = right.as_ref() else {
            panic!("expected right filter");
        };
        assert_eq!(
            predicate,
            &binary(
                column(&right_field()),
                BinaryOp::Lt,
                LogicalExpr::Literal(LogicalLiteral::Int64(10))
            )
        );
    }

    fn right_field() -> ResolvedColumn {
        field(2, "b_id")
    }

    #[test]
    fn propagates_right_predicate_to_left() {
        let left = field(1, "a_id");
        let right = field(2, "b_id");
        let plan = LogicalPlan::Filter {
            input: Box::new(join(&left, &right, JoinType::Inner)),
            predicate: binary(
                column(&right),
                BinaryOp::Gt,
                LogicalExpr::Literal(LogicalLiteral::Int64(1)),
            ),
        };

        let rewritten = JoinPredicateTransitivityRule
            .apply(&plan, &OptimizerContext::default())
            .unwrap()
            .expect("predicate should propagate");

        let LogicalPlan::Join { left, .. } = rewritten else {
            panic!("expected join");
        };
        let LogicalPlan::Filter { predicate, .. } = left.as_ref() else {
            panic!("expected left filter");
        };
        assert_eq!(
            predicate,
            &binary(
                column(&field(1, "a_id")),
                BinaryOp::Gt,
                LogicalExpr::Literal(LogicalLiteral::Int64(1))
            )
        );
    }

    #[test]
    fn rejects_cross_side_predicate() {
        let left = field(1, "a_id");
        let right = field(2, "b_id");
        let plan = LogicalPlan::Filter {
            input: Box::new(join(&left, &right, JoinType::Inner)),
            predicate: binary(column(&left), BinaryOp::Eq, column(&right)),
        };
        assert!(
            JoinPredicateTransitivityRule
                .apply(&plan, &OptimizerContext::default())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn rejects_outer_join() {
        let left = field(1, "a_id");
        let right = field(2, "b_id");
        let plan = LogicalPlan::Filter {
            input: Box::new(join(&left, &right, JoinType::Left)),
            predicate: binary(
                column(&left),
                BinaryOp::Lt,
                LogicalExpr::Literal(LogicalLiteral::Int64(10)),
            ),
        };
        assert!(
            JoinPredicateTransitivityRule
                .apply(&plan, &OptimizerContext::default())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn rejects_non_equality_join() {
        let left = field(1, "a_id");
        let right = field(2, "b_id");
        let join = LogicalPlan::Join {
            join_type: JoinType::Inner,
            left: Box::new(scan(vec![left.clone()])),
            right: Box::new(scan(vec![right.clone()])),
            on: Some(binary(column(&left), BinaryOp::Lt, column(&right))),
            using: vec![],
            schema: Box::new(VeinSchema {
                fields: vec![left.clone(), right.clone()],
            }),
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(join),
            predicate: binary(
                column(&left),
                BinaryOp::Lt,
                LogicalExpr::Literal(LogicalLiteral::Int64(10)),
            ),
        };
        assert!(
            JoinPredicateTransitivityRule
                .apply(&plan, &OptimizerContext::default())
                .unwrap()
                .is_none()
        );
    }
}
