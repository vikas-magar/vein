//! Canonical source-phase binding for the shared scan work queue.
use super::ScanWorkQueue;
use crate::pipeline::resumable::executable::PipelineOperatorState;
use crate::pipeline::resumable::{self, BatchProcessor, ErasedResumableOperator, OperatorCapabilities,
                                 OperatorMetadata, OrderingGuarantee, ParallelismCapability, PartitioningGuarantee};
use common::{Result, VeinError, protocol::pipeline_graph::PipelinePhase};
use std::{any::Any, sync::Arc};

struct Binding {
    queue: Arc<ScanWorkQueue>,
    metadata: Arc<OperatorMetadata>,
    processor: Option<Arc<dyn BatchProcessor>>,
}
impl ScanWorkQueue {
    /// Binds this source into an executable canonical pipeline without opening I/O.
    pub fn pipeline_state(self: &Arc<Self>, metadata: Arc<OperatorMetadata>) -> Result<Arc<dyn PipelineOperatorState>> {
        self.pipeline_state_with_processor(metadata, None)
    }

    /// Binds source-local expressions to a reusable processor template.
    pub fn pipeline_state_with_processor(
        self: &Arc<Self>, metadata: Arc<OperatorMetadata>, processor: Option<Arc<dyn BatchProcessor>>,
    ) -> Result<Arc<dyn PipelineOperatorState>> {
        if metadata.role != resumable::OperatorRole::Source || !metadata.children.is_empty() {
            return Err(VeinError::invalid_input("scan binding requires childless source metadata"));
        }
        Ok(Arc::new(Binding { queue: Arc::clone(self), metadata, processor }))
    }
}
impl PipelineOperatorState for Binding {
    fn capabilities(&self, phase: PipelinePhase) -> Result<OperatorCapabilities> {
        validate_phase(phase)?;
        Ok(OperatorCapabilities::new(vec![], OrderingGuarantee::Unknown,
                                     PartitioningGuarantee::Unknown, ParallelismCapability::Independent))
    }
    fn create_stage(&self, phase: PipelinePhase) -> Result<Box<dyn ErasedResumableOperator>> {
        validate_phase(phase)?;
        let stage = match &self.processor {
            Some(processor) => self.queue.stage().with_processor(processor.fork()?),
            None => self.queue.stage(),
        };
        resumable::erase_operator(stage, &self.metadata, phase)
    }
    fn combine_stage(&self, phase: PipelinePhase, local: Box<dyn Any + Send>) -> Result<()> {
        validate_phase(phase)?;
        local.downcast::<()>().map(|_| ())
            .map_err(|_| VeinError::internal("invalid completed scan local state"))
    }
}
fn validate_phase(phase: PipelinePhase) -> Result<()> {
    if phase != PipelinePhase::Source {
        return Err(VeinError::invalid_input("scan binding only supports the source phase"));
    }
    Ok(())
}
