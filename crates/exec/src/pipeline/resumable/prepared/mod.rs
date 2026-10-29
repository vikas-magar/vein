//! Bound extension instances with factory lookup removed from stage execution.

use super::factory::ErasedOperatorFactory;
use super::{AnyOperatorDefinition, ErasedResumableOperator, OperatorCapabilities, OperatorGlobalState};
use common::Result;
use std::sync::Arc;
mod plan;
mod index;
use common::protocol::pipeline_graph::PipelinePhase;
pub(super) use index::CanonicalPlanIndex;
pub use plan::PreparedExtensionPlan;

/// Validated query-node definition bound to its registered implementation.
pub struct PreparedOperator {
    pub(super) definition: Box<dyn AnyOperatorDefinition>,
    pub(super) factory: Arc<dyn ErasedOperatorFactory>,
    pub(super) global: OperatorGlobalState,
}

impl PreparedOperator {
    /// Creates a worker-local stage without a registry lookup or configuration decode.
    pub fn create_operator(&self, phase: PipelinePhase) -> Result<Box<dyn ErasedResumableOperator>> {
        self.factory.create_operator(self.definition.as_ref(), Arc::clone(&self.global), phase)
    }

    /// Combines completed local state through the already resolved implementation.
    pub fn combine_local_state(&self, phase: PipelinePhase, local: Box<dyn std::any::Any + Send>) -> Result<()> {
        self.factory.combine_local(&self.global, local, phase)
    }

    /// Borrows capabilities resolved before any operator executes.
    pub fn capabilities(&self) -> &OperatorCapabilities { self.factory.capabilities() }

    /// Borrows the validated immutable metadata for this query node.
    pub fn metadata(&self) -> &super::OperatorMetadata { self.definition.metadata() }
}

impl super::executable::PipelineOperatorState for PreparedOperator {
    fn capabilities(&self, phase: PipelinePhase) -> Result<OperatorCapabilities> {
        self.factory.phase_capabilities(self.definition.as_ref(), phase)
    }
    fn create_stage(&self, phase: PipelinePhase) -> Result<Box<dyn ErasedResumableOperator>> {
        self.create_operator(phase)
    }
    fn combine_stage(&self, phase: PipelinePhase, local: Box<dyn std::any::Any + Send>) -> Result<()> {
        self.combine_local_state(phase, local)
    }
}
