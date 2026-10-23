//! Place existence tests before inner joins that do not supply their columns.
//!
//! This rule changes logical work, never row payloads. It consumes plan nodes
//! and shares column metadata; unknown ownership and unsafe predicates are
//! barriers. Outer, anti, single, and USING joins require separate proofs.

use error::Result;
use plan::{LogicalPlan, PlanTraversal};

use crate::context::{JoinOrderDecision, OptimizerContext};
use crate::rule::{OptimizerRule, RuleType};
use crate::rules::predicate_analysis::{BuiltinPredicateSemantics, PredicateSemantics};

mod eligibility;
mod cost;
mod rewrite;

/// Moves a semi join into the inner-join input that supplies its probe columns.
/// The semantics strategy can conservatively decline predicate movement.
pub struct SemiJoinPlacementRule<S = BuiltinPredicateSemantics> {
    semantics: S,
}

impl Default for SemiJoinPlacementRule {
    fn default() -> Self {
        Self::new(BuiltinPredicateSemantics)
    }
}

impl<S: PredicateSemantics> SemiJoinPlacementRule<S> {
    /// Builds a placement rule using the supplied semantic proof strategy.
    pub fn new(semantics: S) -> Self {
        Self { semantics }
    }
}

impl<S: PredicateSemantics + Send + Sync> OptimizerRule for SemiJoinPlacementRule<S> {
    fn name(&self) -> &str {
        "SemiJoinPlacementRule"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Heuristic
    }

    fn is_plan_rule(&self) -> bool {
        true
    }

    fn apply(&self, plan: &LogicalPlan, context: &OptimizerContext)
             -> Result<Option<LogicalPlan>>
    {
        if eligibility::target(plan, &self.semantics).is_none() {
            return Ok(None);
        }
        let (plan, movements) = rewrite::place(plan.clone(), &self.semantics, context);
        Ok((movements != 0).then_some(plan))
    }

    fn apply_owned_plan(&self, plan: LogicalPlan, context: &OptimizerContext)
                        -> Result<(LogicalPlan, bool, Vec<JoinOrderDecision>)>
    {
        let mut movements = 0;
        let plan = rewrite_tree(plan, &self.semantics, context, &mut movements)?;
        if movements != 0 {
            tracing::info!(target: "vein.optimizer", event = "optimizer_semi_join_placed",
                movements, "existence tests moved before independent inner joins");
        }
        Ok((plan, movements != 0, Vec::new()))
    }
}

fn rewrite_tree<S: PredicateSemantics>(
    plan: LogicalPlan, semantics: &S, context: &OptimizerContext, movements: &mut usize,
) -> Result<LogicalPlan> {
    let plan = plan.map_children(&mut |child| {
        Ok(Box::new(rewrite_tree(*child, semantics, context, movements)?))
    })?;
    let (plan, count) = rewrite::place(plan, semantics, context);
    *movements += count;
    Ok(plan)
}

#[cfg(test)]
mod tests;
