use super::super::cursor::AggregateOutputCursor;
use crate::pipeline::resumable::{Handoff, InputEvent, LocalStateOperator, OperatorInputPort, OperatorStep, ResumableOperator};
use common::{Result, VeinError};

pub(super) struct AggregateSourceStage {
    cursor: AggregateOutputCursor,
    finished: bool,
}

impl AggregateSourceStage {
    pub(super) fn new(cursor: AggregateOutputCursor) -> Self { Self { cursor, finished: false } }
}

impl ResumableOperator for AggregateSourceStage {
    type WaitReason = ();
    fn push_input(&mut self, _: OperatorInputPort, _: InputEvent) -> Result<Handoff<InputEvent>> {
        Err(VeinError::invalid_input("aggregate output phase does not accept input"))
    }
    fn step(&mut self) -> Result<OperatorStep<()>> {
        if self.finished { return Err(VeinError::internal("aggregate output already finished")); }
        let step = self.cursor.step()?;
        if matches!(step, OperatorStep::Finished) { self.finished = true; }
        Ok(step)
    }
}

impl LocalStateOperator for AggregateSourceStage {
    type LocalState = ();
    fn take_local_state(&mut self) -> Result<()> {
        if !self.finished { return Err(VeinError::internal("aggregate output has not finished")); }
        Ok(())
    }
}
