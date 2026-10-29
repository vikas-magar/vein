use super::{sink::AggregateSinkStage, source::AggregateSourceStage};
use super::super::{AggregateSink, AGGREGATE_BATCH_ROWS};
use crate::memory::QueryMemoryTracker;
use crate::pipeline::resumable::{self, ErasedResumableOperator, InputProperties,
    OperatorCapabilities, OperatorMetadata, OrderingGuarantee, ParallelismCapability, PartitioningGuarantee};
use crate::pipeline::resumable::executable::PipelineOperatorState;
use common::{Result, VeinError};
use common::protocol::{AggregatePlan, pipeline_graph::PipelinePhase};
use functions::FunctionRegistry;
use std::any::Any;
use std::sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}};

/// Shared query state for one canonical aggregate node's sink and source phases.
pub struct AggregatePipelineState {
    metadata: Arc<OperatorMetadata>,
    canonical: Mutex<AggregateSink>,
    tracker: QueryMemoryTracker,
    registry: Arc<FunctionRegistry>,
    output_started: AtomicBool,
}

impl AggregatePipelineState {
    /// Creates one query-scoped owner for local aggregate states and final output.
    pub fn new(plan: AggregatePlan, tracker: QueryMemoryTracker,
        registry: Arc<FunctionRegistry>, metadata: Arc<OperatorMetadata>,
    ) -> Result<Self> {
        if metadata.role != resumable::OperatorRole::Transform || metadata.children.len() != 1 {
            return Err(VeinError::planning("aggregate phase requires one blocking input", None));
        }
        let canonical = AggregateSink::with_registry_and_memory_tracker(plan, tracker.clone(), Arc::clone(&registry))?;
        Ok(Self { metadata, canonical: Mutex::new(canonical), tracker, registry, output_started: AtomicBool::new(false) })
    }

    fn validate_phase(phase: PipelinePhase) -> Result<()> {
        match phase {
            PipelinePhase::Source => Ok(()),
            PipelinePhase::Sink(port) if port.index() == 0 => Ok(()),
            _ => Err(VeinError::invalid_input("aggregate supports only source and sink port 0 phases")),
        }
    }
}

impl PipelineOperatorState for AggregatePipelineState {
    fn capabilities(&self, phase: PipelinePhase) -> Result<OperatorCapabilities> {
        Self::validate_phase(phase)?;
        let parallelism = match phase {
            PipelinePhase::Source => ParallelismCapability::Serial,
            PipelinePhase::Sink(_) => ParallelismCapability::CombineLocalState,
            _ => unreachable!(),
        };
        Ok(OperatorCapabilities::new(vec![InputProperties::any()], OrderingGuarantee::Unknown,
            PartitioningGuarantee::Unknown, parallelism))
    }

    fn create_stage(&self, phase: PipelinePhase) -> Result<Box<dyn ErasedResumableOperator>> {
        Self::validate_phase(phase)?;
        let stage: Box<dyn ErasedResumableOperator> = match phase {
            PipelinePhase::Sink(_) => {
                let mut plan = self.canonical.lock().map_err(|_| VeinError::internal("aggregate state mutex poisoned"))?.plan.clone();
                plan.estimated_groups = None;
                let sink = AggregateSink::with_registry_and_memory_tracker(plan, self.tracker.clone(), Arc::clone(&self.registry))?;
                resumable::erase_operator(AggregateSinkStage::new(sink), &self.metadata, phase)?
            }
            PipelinePhase::Source => {
                if self.output_started.swap(true, Ordering::AcqRel) {
                    return Err(VeinError::invalid_input("aggregate output phase was already opened"));
                }
                let cursor = self.canonical.lock().map_err(|_| VeinError::internal("aggregate state mutex poisoned"))?
                    .take_output_cursor(AGGREGATE_BATCH_ROWS)?;
                resumable::erase_operator(AggregateSourceStage::new(cursor), &self.metadata, phase)?
            }
            _ => unreachable!(),
        };
        Ok(stage)
    }

    fn combine_stage(&self, phase: PipelinePhase, local: Box<dyn Any + Send>) -> Result<()> {
        Self::validate_phase(phase)?;
        if !matches!(phase, PipelinePhase::Sink(_)) {
            return Err(VeinError::invalid_input("only aggregate sink states can be combined"));
        }
        let local = local.downcast::<AggregateSink>()
            .map_err(|_| VeinError::invalid_input("aggregate local state has the wrong type"))?;
        self.canonical.lock().map_err(|_| VeinError::internal("aggregate state mutex poisoned"))?
            .combine_local_state(*local)
    }
}
