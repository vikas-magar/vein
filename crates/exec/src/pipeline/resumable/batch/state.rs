use super::{BatchProcessor, stage::BatchProcessorStage};
use crate::pipeline::resumable::executable::PipelineOperatorState;
use crate::pipeline::resumable::{self, ErasedResumableOperator, InputProperties,
                                 OperatorCapabilities, OperatorMetadata, OperatorRole, OrderingGuarantee,
                                 ParallelismCapability, PartitioningGuarantee};
use common::protocol::pipeline_graph::PipelinePhase;
use common::{Result, VeinError};
use std::{any::Any, sync::Arc};

/// Query-scoped factory state for one canonical batch-transform node.
pub struct BatchProcessorState {
    metadata: Arc<OperatorMetadata>,
    processor: Arc<dyn BatchProcessor>,
    parallelism: ParallelismCapability,
}

impl BatchProcessorState {
    /// Binds an immutable processor template; each worker receives a fresh fork.
    pub fn new(metadata: Arc<OperatorMetadata>, processor: Arc<dyn BatchProcessor>) -> Result<Self> {
        if metadata.role != OperatorRole::Transform || metadata.children.len() != 1 {
            return Err(VeinError::planning("batch processor requires one streaming input", None));
        }
        let parallelism = processor.parallelism();
        if parallelism == ParallelismCapability::CombineLocalState {
            return Err(VeinError::planning("batch processors cannot combine worker-local state", None));
        }
        Ok(Self { metadata, processor, parallelism })
    }

    fn validate_phase(phase: PipelinePhase) -> Result<()> {
        if matches!(phase, PipelinePhase::Transform(port) if port.index() == 0) {
            Ok(())
        } else {
            Err(VeinError::invalid_input("batch processor supports transform port 0 only"))
        }
    }
}

impl PipelineOperatorState for BatchProcessorState {
    fn capabilities(&self, phase: PipelinePhase) -> Result<OperatorCapabilities> {
        Self::validate_phase(phase)?;
        Ok(OperatorCapabilities::new(vec![InputProperties::any()],
                                     OrderingGuarantee::PreservesInput(0), PartitioningGuarantee::Unknown, self.parallelism))
    }

    fn create_stage(&self, phase: PipelinePhase) -> Result<Box<dyn ErasedResumableOperator>> {
        Self::validate_phase(phase)?;
        resumable::erase_operator(BatchProcessorStage::new(self.processor.fork()?), &self.metadata, phase)
    }

    fn combine_stage(&self, phase: PipelinePhase, local: Box<dyn Any + Send>) -> Result<()> {
        Self::validate_phase(phase)?;
        local.downcast::<()>().map(|_| ())
            .map_err(|_| VeinError::invalid_input("batch transform returned invalid local state"))
    }
}
