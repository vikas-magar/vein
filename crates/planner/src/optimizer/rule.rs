use crate::plan::LogicalPlan;
use error::Result;

/// Defines the category of an Optimizer Rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleType {
    /// Heuristic rules (RBO) that always improve the plan (e.g., constant folding).
    Heuristic,
    /// Rules that generate alternative plans for the CBO to explore in the Memo.
    CostBased,
}

/// An optimization rule that can transform or yield alternative logical plans.
pub trait OptimizerRule: Send + Sync {
    /// The name of the rule (useful for telemetry and debugging).
    fn name(&self) -> &str;

    /// The category of this rule.
    fn rule_type(&self) -> RuleType;

    /// Attempts to apply the rule to a LogicalPlan.
    ///
    /// For Heuristic rules, it returns the optimized plan if it changed it.
    /// For CostBased rules, it returns an alternative plan to insert into the Memo.
    fn apply(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>>;
}
