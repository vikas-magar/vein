use super::super::{ErasedResumableOperator, ParallelismCapability};
use super::ExecutableStage;
use common::protocol::PipelineId;
use common::{Result, VeinError};
use std::sync::Arc;

/// Bound source-to-sink stage sequence derived from a canonical pipeline.
pub struct ExecutablePipeline {
    pub(super) id: PipelineId,
    pub(super) stages: Arc<[ExecutableStage]>,
    pub(super) parallelism: ParallelismCapability,
}

impl ExecutablePipeline {
    /// Stable identity from the derived graph.
    pub fn id(&self) -> PipelineId { self.id }
    /// Source, streaming operators and sink in delivery order.
    pub fn stages(&self) -> &[ExecutableStage] { &self.stages }
    /// Conservative concurrency requirement across all phases in this pipeline.
    pub fn parallelism(&self) -> ParallelismCapability { self.parallelism }
    /// Allocates local stages without copying canonical nodes or input data.
    pub fn create_worker(self: &Arc<Self>) -> Result<PipelineInstance> {
        let stages = self.stages.iter().map(|stage| stage.state.create_stage(stage.definition.phase()))
            .collect::<Result<Vec<_>>>()?;
        let completed = vec![false; stages.len()];
        Ok(PipelineInstance { definition: Arc::clone(self), stages, completed })
    }
}

/// Worker-local stages retained independently from shared operator state.
pub struct PipelineInstance {
    definition: Arc<ExecutablePipeline>,
    stages: Vec<Box<dyn ErasedResumableOperator>>,
    completed: Vec<bool>,
}

impl PipelineInstance {
    /// Keeps the immutable executable definition alive throughout worker execution.
    pub fn definition(&self) -> &Arc<ExecutablePipeline> { &self.definition }
    /// Borrows a local stage; T05 supplies the cooperative driving loop.
    pub fn stage_mut(&mut self, index: usize) -> Option<&mut (dyn ErasedResumableOperator + '_)> {
        match self.stages.get_mut(index) {
            Some(stage) => Some(stage.as_mut()),
            None => None
        }
    }
    /// Returns completed local state once and combines it only when declared.
    pub fn finish_stage(&mut self, index: usize) -> Result<()> {
        let completed = self.completed.get_mut(index)
            .ok_or_else(|| VeinError::invalid_input("pipeline stage index is out of bounds"))?;
        if *completed { return Err(VeinError::invalid_input("pipeline local state was already finalized")); }
        let local = self.stages[index].take_local_state()?;
        *completed = true;
        let stage = &self.definition.stages[index];
        if stage.capabilities.parallelism == ParallelismCapability::CombineLocalState {
            stage.state.combine_stage(stage.definition.phase(), local)?;
        }
        Ok(())
    }
}
