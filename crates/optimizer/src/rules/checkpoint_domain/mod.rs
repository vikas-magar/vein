//! Restrict grouped join inputs to a replayable query-local key domain.
//!
//! This is an equality-domain reduction over an already completed checkpoint,
//! not general dependent-join execution. It changes no aggregate group's rows,
//! outer-join defaults, or payload representation.

use crate::context::OptimizerContext;
use crate::rule::{OptimizerRule, RuleType};
use error::Result;
use plan::LogicalPlan;

mod aggregate;
mod candidate;
mod domain;
mod restriction;
mod rewrite;

/// Uses retained checkpoint tuples to avoid aggregating irrelevant join keys.
#[derive(Default)]
pub struct CheckpointAggregateDomainRule;

impl CheckpointAggregateDomainRule {
    /// Returns an input whose retained tuples can restrict an opposite grouped input.
    /// A pending request is independent of whether CBO has sufficient estimates.
    pub fn checkpoint_input(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        candidate::candidates(plan)
            .find(|candidate| domain::find(candidate.probe, &candidate.probe_keys).is_none())
            .map(|candidate| candidate.probe)
    }

    /// Counts unresolved tuple-domain requests in the current continuation.
    pub fn pending_checkpoints(plan: &LogicalPlan) -> usize {
        use plan::PlanTraversal;
        let mut pending = vec![plan];
        let mut count = 0;
        while let Some(node) = pending.pop() {
            count += usize::from(Self::checkpoint_input(node).is_some());
            pending.extend(node.children());
        }
        count
    }
}

impl OptimizerRule for CheckpointAggregateDomainRule {
    fn name(&self) -> &str { "CheckpointAggregateDomainRule" }

    fn rule_type(&self) -> RuleType { RuleType::Heuristic }

    fn apply_owned(&self, plan: LogicalPlan, _context: &OptimizerContext)
                   -> Result<(LogicalPlan, bool)>
    {
        let Some(restriction) = restriction::prepare(&plan) else { return Ok((plan, false)); };
        Ok((rewrite::apply(plan, restriction), true))
    }

    fn apply(&self, plan: &LogicalPlan, _context: &OptimizerContext)
             -> Result<Option<LogicalPlan>>
    {
        Ok(restriction::prepare(plan).map(|restriction| rewrite::apply(plan.clone(), restriction)))
    }
}

#[cfg(test)]
mod tests;
