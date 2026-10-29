use super::super::AggregateSink;
use crate::pipeline::DataChunkView;
use crate::pipeline::resumable::{Handoff, InputEvent, LocalStateOperator, OperatorInputPort, OperatorStep, ResumableOperator};
use common::{Result, VeinError};

pub(super) struct AggregateSinkStage {
    sink: Option<AggregateSink>,
    input: Option<DataChunkView>,
    ended: bool,
    finished: bool,
}

impl AggregateSinkStage {
    pub(super) fn new(sink: AggregateSink) -> Self {
        Self { sink: Some(sink), input: None, ended: false, finished: false }
    }
}

impl ResumableOperator for AggregateSinkStage {
    type WaitReason = ();
    fn push_input(&mut self, port: OperatorInputPort, input: InputEvent) -> Result<Handoff<InputEvent>> {
        if port.index() != 0 { return Err(VeinError::invalid_input("aggregate has only input port 0")); }
        if self.ended || self.finished { return Err(VeinError::invalid_input("aggregate input is closed")); }
        if self.input.is_some() { return Ok(Handoff::Retry(input)); }
        match input {
            InputEvent::Batch(view) => self.input = Some(view),
            InputEvent::EndOfInput => self.ended = true,
        }
        Ok(Handoff::Accepted)
    }

    fn step(&mut self) -> Result<OperatorStep<()>> {
        if let Some(batch) = self.input.take() {
            self.sink.as_mut().ok_or_else(|| VeinError::internal("aggregate local sink is missing"))?
                .accumulate(batch)?;
            return Ok(OperatorStep::Runnable);
        }
        if self.ended { self.finished = true; return Ok(OperatorStep::Finished); }
        Ok(OperatorStep::NeedsInput)
    }
}

impl LocalStateOperator for AggregateSinkStage {
    type LocalState = AggregateSink;
    fn take_local_state(&mut self) -> Result<AggregateSink> {
        if !self.finished { return Err(VeinError::internal("aggregate input has not finished")); }
        self.sink.take().ok_or_else(|| VeinError::internal("aggregate local state was already taken"))
    }
}
