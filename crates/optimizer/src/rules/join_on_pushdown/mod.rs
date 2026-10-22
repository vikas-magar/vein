//! Push ON conjuncts into precisely the inputs permitted by join semantics.

use error::Result;
use plan::{LogicalExpr, LogicalPlan, PlanTraversal};

use crate::context::OptimizerContext;
use crate::rule::{OptimizerRule, RuleType};
use crate::rules::predicate_analysis::{conjunction, conjuncts, JoinInputs, PredicateOwner, on_target};

/// Moves safe ON predicates according to inner, outer and existence semantics.
pub struct JoinOnPredicatePushdownRule;

impl OptimizerRule for JoinOnPredicatePushdownRule {
    fn name(&self) -> &str {
        "JoinOnPredicatePushdownRule"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Heuristic
    }

    fn apply(
        &self,
        plan: &LogicalPlan,
        _context: &OptimizerContext,
    ) -> Result<Option<LogicalPlan>> {
        let LogicalPlan::Join {
            join_type,
            left,
            right,
            on: Some(on),
            using,
            schema,
        } = plan
        else {
            return Ok(None);
        };

        // Moving a conjunct changes its evaluation point. Require the complete
        // ON expression to be reorder-safe so a moved predicate cannot run
        // before an unsafe expression that could fail or have side effects.
        if !on.is_reorder_safe() || on.is_volatile() {
            return Ok(None);
        }

        let inputs = JoinInputs::new(left.schema(), right.schema());
        let mut residual = Vec::new();
        let mut left_terms = Vec::new();
        let mut right_terms = Vec::new();
        for term in conjuncts(on) {
            match on_target(*join_type, inputs.owner(term)) {
                Some(PredicateOwner::Left) => left_terms.push(term.clone()),
                Some(PredicateOwner::Right) => right_terms.push(term.clone()),
                _ => residual.push(term.clone()),
            }
        }
        if left_terms.is_empty() && right_terms.is_empty() {
            return Ok(None);
        }

        Ok(Some(LogicalPlan::Join {
            join_type: *join_type,
            left: filtered(left, left_terms),
            right: filtered(right, right_terms),
            on: conjunction(residual),
            using: using.clone(),
            schema: schema.clone(),
        }))
    }
}

fn filtered(input: &LogicalPlan, terms: Vec<LogicalExpr>) -> Box<LogicalPlan> {
    Box::new(match conjunction(terms) {
        Some(predicate) => LogicalPlan::Filter { input: Box::new(input.clone()), predicate },
        None => input.clone(),
    })
}

#[cfg(test)]
mod tests;
