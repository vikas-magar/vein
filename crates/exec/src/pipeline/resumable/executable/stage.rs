use super::super::OperatorCapabilities;
use super::PipelineOperatorState;
use common::protocol::pipeline_graph::PipelineStage;
use std::sync::Arc;

/// Executable binding that retains canonical semantics and one shared state owner.
pub struct ExecutableStage {
    pub(super) definition: PipelineStage,
    pub(super) state: Arc<dyn PipelineOperatorState>,
    pub(super) capabilities: OperatorCapabilities,
}

impl ExecutableStage {
    /// Borrows the canonical node, phase, input port and schemas.
    pub fn definition(&self) -> &PipelineStage { &self.definition }
    /// Declared phase requirements and output guarantees.
    pub fn capabilities(&self) -> &OperatorCapabilities { &self.capabilities }
}
