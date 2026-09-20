//! The optimizer rule trait and rule categories.

use error::Result;
use plan::LogicalPlan;

use crate::context::{OptimizerContext, OptimizerPhase};

/// Defines the category of an optimizer rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleType {
    /// Heuristic rules (RBO) that always improve the plan, e.g. constant folding.
    Heuristic,
    /// Rules that generate alternative plans for a future cost-based optimizer.
    CostBased,
}

/// A logical-plan rewriting rule that operates on a single plan node.
///
/// The pipeline applies a rule bottom-up to every node of the plan: the rule
/// inspects the node it is given and either rewrites it (returning
/// `Some(rewritten)`) or leaves it untouched (returning `None`). Change is
/// detected structurally, so rules must only return `Some` when the node
/// actually differs from the input.
///
/// Rules marked [`RuleType::CostBased`] are reserved for the future
/// cost-based optimizer and are not executed by [`OptimizerPipeline`](crate::OptimizerPipeline).
pub trait OptimizerRule: Send + Sync {
    /// The name of the rule, useful for telemetry and debugging.
    fn name(&self) -> &str;

    /// The category of this rule.
    fn rule_type(&self) -> RuleType;

    /// The execution phase for this rule.
    fn phase(&self) -> OptimizerPhase {
        match self.rule_type() {
            RuleType::Heuristic => OptimizerPhase::Heuristic,
            RuleType::CostBased => OptimizerPhase::CostBased,
        }
    }

    /// Whether this rule needs the complete plan and a top-down analysis.
    ///
    /// Plan-scoped rules are invoked once per sweep through apply_plan
    /// instead of being applied independently to every node.
    fn is_plan_rule(&self) -> bool {
        false
    }

    /// Attempts to rewrite a complete logical plan.
    ///
    /// Node-scoped rules use the default implementation. Plan-scoped rules
    /// override this method to perform analyses such as required-column
    /// propagation that cannot be expressed as a bottom-up node rewrite.
    fn apply_plan(
        &self,
        _plan: &LogicalPlan,
        _context: &OptimizerContext,
    ) -> Result<Option<LogicalPlan>> {
        Ok(None)
    }

    /// Attempts to rewrite a single plan node.
    ///
    /// Returns Some(rewritten) if the node was changed and None otherwise.
    fn apply(&self, plan: &LogicalPlan, context: &OptimizerContext) -> Result<Option<LogicalPlan>>;
}
