//! Exact population bounds become ordinary predicates on remaining join work.
//! Approximate frequency, histogram, and NDV synopses never prove row exclusion.

use error::Result;
use parser::ast::{expr::BinaryOp, pipe_op::JoinType};
use plan::{ColumnId, LogicalExpr, LogicalLiteral, LogicalPlan, PlanTraversal, VeinDataType};

use super::join_transitivity::{equi_join_keys, schema_ids};
use crate::context::OptimizerContext;
use crate::rule::{OptimizerRule, RuleType};

/// Applies necessary key constraints observed at a completed checkpoint.
/// Only inner joins are eligible: outer/anti/single joins have different
/// preserved-row semantics. Existing pushdown rules handle storage placement.
pub struct CheckpointJoinFilterRule;

impl OptimizerRule for CheckpointJoinFilterRule {
    fn name(&self) -> &str {
        "CheckpointJoinFilterRule"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Heuristic
    }

    fn apply(&self, plan: &LogicalPlan, context: &OptimizerContext) -> Result<Option<LogicalPlan>> {
        let LogicalPlan::Join {
            join_type: JoinType::Inner,
            left,
            right,
            on: Some(on),
            ..
        } = plan else {
            return Ok(None);
        };
        if on.is_volatile() {
            return Ok(None);
        }
        let keys = equi_join_keys(on, &schema_ids(left.schema()), &schema_ids(right.schema()));
        let mut constraints = [Vec::new(), Vec::new()];
        for (left_id, right_id) in keys {
            for (source, source_key, target, target_key, side) in [
                (left.as_ref(), left_id, right.as_ref(), right_id, 1),
                (right.as_ref(), right_id, left.as_ref(), left_id, 0),
            ] {
                let Some(column) = target.schema().fields.iter().find(|c| c.id == target_key) else {
                    continue;
                };
                for bound in observed_bounds(source, source_key, context) {
                    let predicate = match bound {
                        None => LogicalExpr::Literal(LogicalLiteral::Boolean(false)),
                        Some((op, literal)) => LogicalExpr::BinaryOp {
                            left: Box::new(LogicalExpr::Column(column.clone())),
                            op,
                            right: Box::new(LogicalExpr::Literal(literal)),
                            return_type: VeinDataType::Boolean,
                        },
                    };
                    if !contains_predicate(target, &predicate)
                        && !constraints[side].contains(&predicate)
                    {
                        constraints[side].push(predicate);
                    }
                }
            }
        }
        let count = constraints.iter().map(Vec::len).sum::<usize>();
        if count == 0 {
            return Ok(None);
        }
        let mut rewritten = plan.clone();
        let LogicalPlan::Join { left, right, .. } = &mut rewritten else {
            unreachable!();
        };
        for (target, predicates) in [left, right].into_iter().zip(constraints) {
            for predicate in predicates {
                **target = LogicalPlan::Filter {
                    input: target.clone(),
                    predicate,
                };
            }
        }
        tracing::info!(
            target: "vein.adaptive",
            event = "adaptive_checkpoint_filters_derived",
            predicates = count,
            evidence = "complete_population_exact_bounds",
            "checkpoint key bounds pushed into remaining join inputs"
        );
        Ok(Some(rewritten))
    }
}

/// `None` represents a false predicate (no checkpoint key can match).
fn observed_bounds(
    source: &LogicalPlan,
    key: ColumnId,
    context: &OptimizerContext,
) -> Vec<Option<(BinaryOp, LogicalLiteral)>> {
    let source = match source {
        LogicalPlan::Filter { input, .. } => return observed_bounds(input, key, context),
        source => source,
    };
    let LogicalPlan::Intermediate { source_id, row_count, schema } = source else {
        return Vec::new();
    };
    let Some(statistics) = context.intermediate_statistics(*source_id) else {
        return Vec::new();
    };
    if statistics.num_rows != Some(*row_count) {
        return Vec::new();
    }
    if *row_count == 0 {
        return vec![None];
    }
    let Some(column) = schema.fields.iter().find(|column| column.id == key) else {
        return Vec::new();
    };
    let Some(summary) = statistics.column_statistics
        .get(&common::statistics::intermediate_column_key(key)) else {
        return Vec::new();
    };
    if !crate::statistics::valid_column(summary, *row_count, column.nullable) {
        return Vec::new();
    }
    if summary.null_count == Some(*row_count) {
        return vec![None];
    }
    if summary.min_max_complete != Some(true) || summary.nan_count != Some(0) {
        return Vec::new();
    }
    let (Some(min), Some(max)) = (&summary.min_value, &summary.max_value) else {
        return Vec::new();
    };
    if min == max {
        return vec![Some((BinaryOp::Eq, min.clone()))];
    }
    vec![Some((BinaryOp::Geq, min.clone())), Some((BinaryOp::Leq, max.clone()))]
}

fn contains_predicate(plan: &LogicalPlan, predicate: &LogicalExpr) -> bool {
    match plan {
        LogicalPlan::Scan { filters, .. } if filters.contains(predicate) => return true,
        LogicalPlan::Filter { predicate: existing, .. } if existing == predicate => return true,
        LogicalPlan::Limit { count: 0, .. } => return true,
        _ => {}
    }
    plan.children().into_iter().any(|child| contains_predicate(child, predicate))
}

#[cfg(test)]
#[path = "checkpoint_filters/tests.rs"]
mod tests;
