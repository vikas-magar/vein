//! Format-neutral lowering hooks for registered physical operators.

use common::Result;
use common::protocol::PhysicalPlanNode;
use common::protocol::extension::OperatorTypeId;
use plan::LogicalPlan;
use std::sync::Arc;

/// Recursive child lowering supplied by the canonical stage builder.
pub trait PhysicalExtensionLoweringContext {
    /// Lowers one child using the same authoritative physical-plan construction.
    fn lower_input(&self, logical: &LogicalPlan) -> Result<Arc<PhysicalPlanNode>>;
}

/// Optional lowering of logical shapes without built-in physical semantics.
pub trait PhysicalExtensionLowerer: Send + Sync {
    /// Registered operator identity this lowerer may produce.
    fn operator_type_id(&self) -> OperatorTypeId;
    /// Produces a canonical extension node, or declines this logical shape.
    fn lower(
        &self,
        logical: &LogicalPlan,
        context: &dyn PhysicalExtensionLoweringContext,
    ) -> Result<Option<Arc<PhysicalPlanNode>>>;
}

#[cfg(test)]
mod tests;
