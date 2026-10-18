//! Relational lowering for filter-level `IN` and `NOT IN` subqueries.
mod correlation;

use error::{Result, VeinError};
use parser::ast::expr::BinaryOp;
use parser::ast::pipe_op::JoinType;
use plan::{LogicalExpr, LogicalPlan, PlanTraversal, VeinDataType};

use crate::context::{OptimizerContext, OptimizerPhase};
use crate::rule::{OptimizerRule, RuleType};
use correlation::references_outer_input;

/// Converts a top-level membership conjunct into a hash semi/anti join.
pub struct InSubqueryJoinRule;

impl OptimizerRule for InSubqueryJoinRule {
    fn name(&self) -> &str {
        "InSubqueryJoinRule"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Heuristic
    }

    fn phase(&self) -> OptimizerPhase {
        OptimizerPhase::Heuristic
    }

    fn apply(&self, plan: &LogicalPlan, _context: &OptimizerContext) -> Result<Option<LogicalPlan>> {
        let LogicalPlan::Filter { input, predicate } = plan else {
            return Ok(None);
        };
        let mut terms = Vec::new();
        split_conjunction(predicate, &mut terms);
        let has_membership = terms.iter().any(|term| membership_subquery(term).is_some());
        if !has_membership {
            if terms.iter().any(|term| contains_membership_subquery(term)) {
                return Err(unsupported_shape());
            }
            return Ok(None);
        }

        let mut relation = (**input).clone();
        let mut residuals = Vec::new();
        for term in terms {
            if let Some((probe, subquery, negated)) = membership_subquery(term) {
                relation = membership_join(relation, probe, subquery, negated)?;
            } else if contains_membership_subquery(term) {
                return Err(unsupported_shape());
            } else {
                residuals.push(term.clone());
            }
        }
        let Some(predicate) = conjunction(residuals) else {
            return Ok(Some(relation));
        };
        Ok(Some(LogicalPlan::Filter {
            input: Box::new(relation),
            predicate,
        }))
    }
}

fn membership_join(
    input: LogicalPlan,
    probe: &LogicalExpr,
    subquery: &LogicalPlan,
    negated: bool,
) -> Result<LogicalPlan> {
    let LogicalExpr::Column(probe_column) = probe else {
        return Err(VeinError::planning(
            "IN subquery lowering currently requires a direct probe column",
            None,
        ));
    };
    if input.schema().position_of_id(probe_column.id).is_none() {
        return Err(VeinError::planning(
            "IN subquery probe column is absent from the filtered input",
            None,
        ));
    }
    if subquery.schema().fields.len() != 1 {
        return Err(VeinError::semantic(
            "IN subquery must return exactly one column",
            None,
        ));
    }
    if references_outer_input(subquery, input.schema()) {
        return Err(VeinError::unsupported(
            "correlated IN subqueries are not supported by hash membership lowering",
            None,
        ));
    }

    let build_column = &subquery.schema().fields[0];
    let probe_key = probe_column.data_type.hash_join_key_type();
    let build_key = build_column.data_type.hash_join_key_type();
    if probe_key.is_none() || probe_key != build_key {
        return Err(VeinError::unsupported(
            "IN subquery probe and output columns must have matching hash-join key types",
            None,
        ));
    }

    let on = LogicalExpr::BinaryOp {
        left: Box::new(probe.clone()),
        op: BinaryOp::Eq,
        right: Box::new(LogicalExpr::Column(build_column.clone())),
        return_type: VeinDataType::Boolean,
    };
    let join_type = match (negated, probe_key) {
        (false, _) => JoinType::Semi,
        (true, Some(_)) => JoinType::NullAwareAnti,
        (true, None) => return Err(VeinError::internal("validated IN key type disappeared")),
    };
    tracing::debug!(
        target: "vein.audit",
        event = "in_subquery_lowered",
        negated,
        join_type = ?join_type,
        probe_column = %probe_column.name,
        build_column = %build_column.name,
        "membership subquery lowered to a relational join"
    );
    Ok(LogicalPlan::Join {
        join_type,
        left: Box::new(input.clone()),
        right: Box::new(subquery.clone()),
        on: Some(on),
        using: Vec::new(),
        schema: Box::new(input.schema().clone()),
    })
}

fn membership_subquery(expression: &LogicalExpr) -> Option<(&LogicalExpr, &LogicalPlan, bool)> {
    let LogicalExpr::InList { expr, list, negated } = expression else {
        return None;
    };
    let [LogicalExpr::ScalarSubquery(subquery)] = list.as_slice() else {
        return None;
    };
    Some((expr, subquery, *negated))
}

fn contains_membership_subquery(expression: &LogicalExpr) -> bool {
    membership_subquery(expression).is_some()
        || expression.children().into_iter().any(contains_membership_subquery)
}

fn split_conjunction<'a>(expression: &'a LogicalExpr, terms: &mut Vec<&'a LogicalExpr>) {
    if let LogicalExpr::BinaryOp {
        left,
        op: BinaryOp::And,
        right,
        ..
    } = expression
    {
        split_conjunction(left, terms);
        split_conjunction(right, terms);
    } else {
        terms.push(expression);
    }
}

fn conjunction(mut terms: Vec<LogicalExpr>) -> Option<LogicalExpr> {
    let mut result = terms.pop()?;
    while let Some(term) = terms.pop() {
        result = LogicalExpr::BinaryOp {
            left: Box::new(term),
            op: BinaryOp::And,
            right: Box::new(result),
            return_type: VeinDataType::Boolean,
        };
    }
    Some(result)
}

fn unsupported_shape() -> VeinError {
    VeinError::unsupported(
        "IN subqueries must be top-level conjuncts in a filter; membership under OR or another expression is not supported",
        None,
    )
}

#[cfg(test)]
#[path = "in_subquery/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "in_subquery/scope_tests.rs"]
mod scope_tests;
