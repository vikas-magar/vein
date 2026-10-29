use crate::pipeline::resumable::{ErasedResumableOperator, ErasedWaitReason, Handoff, InputEvent, OperatorInputPort, OperatorStep};
use common::protocol::pipeline_graph::PipelinePhase;
use common::{Result, VeinError};

pub(super) struct Local {
    pub(super) phase: PipelinePhase,
}
pub(super) struct IdleOperator {
    phase: PipelinePhase,
    closed: bool,
    finished: bool,
    taken: bool,
}
impl IdleOperator {
    pub(super) fn new(phase: PipelinePhase) -> Self {
        Self { phase, closed: false, finished: false, taken: false }
    }
}
impl ErasedResumableOperator for IdleOperator {
    fn push_input(&mut self, port: OperatorInputPort, input: InputEvent) -> Result<Handoff<InputEvent>> {
        if self.phase.input_port() != Some(port) || self.closed || self.finished {
            return Err(VeinError::internal("test stage received input in invalid state"));
        }
        self.closed = matches!(input, InputEvent::EndOfInput);
        Ok(Handoff::Accepted)
    }
    fn step(&mut self) -> Result<OperatorStep<ErasedWaitReason>> {
        if self.finished { return Err(VeinError::internal("test stage already finished")); }
        if self.phase.input_port().is_none() || self.closed {
            self.finished = true;
            Ok(OperatorStep::Finished)
        } else { Ok(OperatorStep::NeedsInput) }
    }
    fn acknowledge_output(&mut self) -> Result<()> { Err(VeinError::internal("test stage produced no output")) }
    fn take_local_state(&mut self) -> Result<Box<dyn std::any::Any + Send>> {
        if !self.finished || self.taken { return Err(VeinError::internal("test local state unavailable")); }
        self.taken = true;
        Ok(Box::new(Local { phase: self.phase }))
    }
}
