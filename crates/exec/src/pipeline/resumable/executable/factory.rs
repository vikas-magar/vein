use super::super::{ErasedResumableOperator, OperatorCapabilities, OperatorMetadata};
use common::Result;
use common::protocol::PhysicalPlanNode;
use common::protocol::pipeline_graph::PipelinePhase;
use std::any::Any;
use std::sync::Arc;

/// Query-owned state shared by every phase of one canonical operator.
///
/// Creating a local stage must not execute input or resolve registrations.
pub trait PipelineOperatorState: Send + Sync {
    /// Validates support and declares properties for a particular phase.
    fn capabilities(&self, phase: PipelinePhase) -> Result<OperatorCapabilities>;
    /// Creates a worker-local stage referencing this shared query state.
    fn create_stage(&self, phase: PipelinePhase) -> Result<Box<dyn ErasedResumableOperator>>;
    /// Consumes completed worker state when combination is declared.
    fn combine_stage(&self, phase: PipelinePhase, local: Box<dyn Any + Send>) -> Result<()>;
}

/// Construction boundary for canonical built-in operators.
///
/// Registered extensions use the same state interface through their bound factory.
pub trait PipelineOperatorProvider: Send + Sync {
    /// Binds one canonical node once for all its requested phases.
    fn prepare(
        &self,
        node: &Arc<PhysicalPlanNode>,
        metadata: Arc<OperatorMetadata>,
        phases: &[PipelinePhase],
    ) -> Result<Arc<dyn PipelineOperatorState>>;
}
