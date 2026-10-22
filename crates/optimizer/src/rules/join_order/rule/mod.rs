use super::*;

mod attempt;

/// Statistics-driven join-order rule using a bounded exact subset search.
pub struct JoinOrderRule<C = CumulativeCardinalityCost> {
    cost_model: C,
}

impl Default for JoinOrderRule<CumulativeCardinalityCost> {
    fn default() -> Self {
        Self {
            cost_model: CumulativeCardinalityCost,
        }
    }
}

impl<C: JoinCostModel> JoinOrderRule<C> {
    /// Creates the exact-search rule with a caller-supplied cost model.
    pub fn with_cost_model(cost_model: C) -> Self {
        Self { cost_model }
    }

    fn rewrite_owned(
        &self,
        plan: LogicalPlan,
        context: &OptimizerContext,
        decisions: &mut Vec<JoinOrderDecision>,
    ) -> Result<(LogicalPlan, bool)> {
        match self.attempt(&plan, context) {
            Some(SearchAttempt::Selected(prepared)) => {
                log_decision(&prepared.decision);
                decisions.push(prepared.decision.clone());
                Ok((rebuild(plan, prepared)?, true))
            }
            Some(SearchAttempt::NoChange(decision)) => {
                log_decision(&decision);
                decisions.push(decision);
                Ok((plan, false))
            }
            Some(SearchAttempt::Fallback(decision)) => {
                log_decision(&decision);
                decisions.push(decision);
                self.rewrite_children(plan, context, decisions)
            }
            None => self.rewrite_children(plan, context, decisions),
        }
    }

    fn rewrite_children(
        &self,
        plan: LogicalPlan,
        context: &OptimizerContext,
        decisions: &mut Vec<JoinOrderDecision>,
    ) -> Result<(LogicalPlan, bool)> {
        let changed = std::cell::Cell::new(false);
        let rewritten = plan.map_children(&mut |child| {
            let (child, child_changed) = self.rewrite_owned(*child, context, decisions)?;
            changed.set(changed.get() || child_changed);
            Ok(Box::new(child))
        })?;
        Ok((rewritten, changed.get()))
    }
}

impl<C: JoinCostModel + 'static> OptimizerRule for JoinOrderRule<C> {
    fn name(&self) -> &str {
        "join_order"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::CostBased
    }

    fn is_plan_rule(&self) -> bool {
        true
    }

    fn apply_plan(
        &self,
        _plan: &LogicalPlan,
        _context: &OptimizerContext,
    ) -> Result<Option<LogicalPlan>> {
        Ok(None)
    }

    fn apply_owned_plan(
        &self,
        plan: LogicalPlan,
        context: &OptimizerContext,
    ) -> Result<(LogicalPlan, bool, Vec<JoinOrderDecision>)> {
        let mut decisions = Vec::new();
        let (plan, changed) = self.rewrite_owned(plan, context, &mut decisions)?;
        Ok((plan, changed, decisions))
    }

    fn apply(&self, _plan: &LogicalPlan, _context: &OptimizerContext) -> Result<Option<LogicalPlan>> {
        Ok(None)
    }
}
