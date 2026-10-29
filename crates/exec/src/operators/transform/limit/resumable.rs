use super::window::LimitWindow;
use crate::pipeline::DataChunkView;
use crate::pipeline::resumable::executable::PipelineOperatorState;
use crate::pipeline::resumable::{self, Handoff, InputEvent, InputProperties, LocalStateOperator,
                                 OperatorCapabilities, OperatorInputPort, OperatorMetadata, OperatorRole, OperatorStep,
                                 OrderingGuarantee, ParallelismCapability, PartitioningGuarantee, ResumableOperator};
use common::protocol::{LimitSpec, pipeline_graph::PipelinePhase};
use common::{Result, VeinError};
use std::convert::Infallible;
use std::{any::Any, sync::Arc};

/// Resumable, single-input LIMIT stage with explicit upstream cancellation.
pub struct LimitState {
    window: LimitWindow,
    input: Option<DataChunkView>,
    ended: bool,
    stop_sent: bool,
}

impl LimitState {
    /// Builds one worker-local row window without retaining input data.
    pub fn new(offset: u64, count: u64) -> Self {
        Self { window: LimitWindow::new(offset, count), input: None, ended: false, stop_sent: false }
    }
}

impl ResumableOperator for LimitState {
    type WaitReason = Infallible;

    fn push_input(&mut self, port: OperatorInputPort, input: InputEvent) -> Result<Handoff<InputEvent>> {
        if port.index() != 0 { return Err(VeinError::invalid_input("LIMIT has only input port 0")); }
        if self.ended || self.stop_sent { return Err(VeinError::invalid_input("LIMIT input is already closed")); }
        if self.input.is_some() { return Ok(Handoff::Retry(input)); }
        match input {
            InputEvent::Batch(batch) => self.input = Some(batch),
            InputEvent::EndOfInput => self.ended = true,
        }
        Ok(Handoff::Accepted)
    }

    fn step(&mut self) -> Result<OperatorStep<Self::WaitReason>> {
        if self.window.is_complete() {
            if self.ended { return Ok(OperatorStep::Finished); }
            if !self.stop_sent {
                self.stop_sent = true;
                return Ok(OperatorStep::StopInput);
            }
            return Ok(OperatorStep::Finished);
        }
        if let Some(batch) = self.input.take() {
            return Ok(match self.window.apply(batch)? {
                Some(output) => OperatorStep::Output(output),
                None => OperatorStep::Runnable,
            });
        }
        Ok(if self.ended { OperatorStep::Finished } else { OperatorStep::NeedsInput })
    }
}

impl LocalStateOperator for LimitState {
    type LocalState = ();
    fn take_local_state(&mut self) -> Result<Self::LocalState> { Ok(()) }
}

/// Query-scoped canonical state for a globally ordered LIMIT transform.
pub struct LimitPipelineState {
    spec: LimitSpec,
    metadata: Arc<OperatorMetadata>,
}

impl LimitPipelineState {
    /// Binds one LIMIT specification to its canonical operator metadata.
    pub fn new(spec: LimitSpec, metadata: Arc<OperatorMetadata>) -> Result<Self> {
        if metadata.role != OperatorRole::Transform || metadata.children.len() != 1 {
            return Err(VeinError::planning("LIMIT requires one streaming input", None));
        }
        Ok(Self { spec, metadata })
    }

    fn validate_phase(phase: PipelinePhase) -> Result<()> {
        if matches!(phase, PipelinePhase::Transform(port) if port.index() == 0) {
            Ok(())
        } else {
            Err(VeinError::invalid_input("LIMIT supports transform port 0 only"))
        }
    }
}

impl PipelineOperatorState for LimitPipelineState {
    fn capabilities(&self, phase: PipelinePhase) -> Result<OperatorCapabilities> {
        Self::validate_phase(phase)?;
        Ok(OperatorCapabilities::new(vec![InputProperties::any()],
                                     OrderingGuarantee::PreservesInput(0), PartitioningGuarantee::Unknown,
                                     ParallelismCapability::Serial))
    }

    fn create_stage(&self, phase: PipelinePhase) -> Result<Box<dyn crate::pipeline::resumable::ErasedResumableOperator>> {
        Self::validate_phase(phase)?;
        resumable::erase_operator(LimitState::new(self.spec.offset, self.spec.count), &self.metadata, phase)
    }

    fn combine_stage(&self, phase: PipelinePhase, local: Box<dyn Any + Send>) -> Result<()> {
        Self::validate_phase(phase)?;
        local.downcast::<()>().map(|_| ())
            .map_err(|_| VeinError::invalid_input("LIMIT returned invalid local state"))
    }
}
