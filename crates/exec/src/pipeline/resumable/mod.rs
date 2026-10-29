//! Contracts for cooperative, resumable pipeline stages.
//!
//! These types define ownership and progress; the existing recursive runtime
//! remains the active driver until the later migration tasks replace it.

mod configuration;
mod batch;
pub use batch::{BatchProcessor, BatchProcessorState};
/// Executable phase bindings over the canonical derived graph.
pub mod executable;
/// Bounded, synchronous driving of a prepared worker pipeline.
pub mod executor;
mod factory;
mod input;
mod operator;
mod prepared;
mod progress;
mod registry;
/// Shared, byte-accounted output admission and result delivery.
pub mod result;
/// Native and cooperative policies over the same scheduler.
pub mod host;
/// Client-independent ready queues, dependency completion and resource wakeups.
pub mod scheduler;
pub use configuration::OperatorConfigurationCodec;
pub use prepared::{PreparedExtensionPlan, PreparedOperator};

pub use common::protocol::extension::{
    AnyOperatorDefinition, OperatorChild, OperatorDefinition, OperatorInputPort, OperatorMetadata,
    OperatorNodeId, OperatorRole, OperatorTypeId,
};
pub use common::protocol::extension::{
    InputProperties, OperatorCapabilities, OrderingGuarantee, OrderingRequirement,
    ParallelismCapability, PartitioningGuarantee, PartitioningRequirement,
};
pub use factory::{OperatorGlobalState, TypedOperatorFactory};
pub use input::{Handoff, InputEvent};
pub use operator::{
    ErasedResumableOperator, ErasedWaitReason, LocalStateOperator, ResumableOperator,
};
pub use progress::OperatorStep;
pub use registry::OperatorFactoryRegistry;

pub(crate) fn erase_operator<O: LocalStateOperator + 'static>(operator: O,
                                                              metadata: &OperatorMetadata, phase: common::protocol::pipeline_graph::PipelinePhase,
) -> common::Result<Box<dyn ErasedResumableOperator>> {
    Ok(Box::new(operator::OperatorAdapter::for_phase(operator, metadata, phase)?))
}

#[cfg(test)]
mod tests;
