use super::*;
use crate::pipeline::resumable::{Handoff, InputEvent, LocalStateOperator, OperatorInputPort, OperatorStep, ResumableOperator};

pub(in crate::pipeline::resumable::executable::tests) struct Stage {
    pub(super) global: Arc<Global>,
    pub(super) phase: Option<PipelinePhase>,
    pub(super) closed: bool,
}
impl ResumableOperator for Stage {
    type WaitReason = ();
    fn push_input(&mut self, _: OperatorInputPort, event: InputEvent) -> Result<Handoff<InputEvent>> {
        self.closed = matches!(event, InputEvent::EndOfInput);
        Ok(Handoff::Accepted)
    }
    fn step(&mut self) -> Result<OperatorStep<()>> {
        match self.phase {
            Some(PipelinePhase::Finalize) => {
                assert_eq!(self.global.counts.inputs.load(Ordering::Relaxed), self.global.ports);
                self.global.counts.finalizations.fetch_add(1, Ordering::Relaxed);
                Ok(OperatorStep::Finished)
            }
            _ if self.closed => Ok(OperatorStep::Finished),
            _ => Ok(OperatorStep::NeedsInput),
        }
    }
}
impl LocalStateOperator for Stage {
    type LocalState = PipelinePhase;
    fn take_local_state(&mut self) -> Result<PipelinePhase> {
        self.phase.take().ok_or_else(|| common::VeinError::internal("test sink state taken twice"))
    }
}
