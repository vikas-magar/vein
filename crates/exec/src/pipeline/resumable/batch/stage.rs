use super::BatchProcessor;
use crate::pipeline::DataChunkView;
use crate::pipeline::resumable::{Handoff, InputEvent, LocalStateOperator, OperatorInputPort, OperatorStep, ResumableOperator};
use common::{Result, VeinError};
use std::convert::Infallible;

/// Worker-local streaming transform over a reusable batch processor.
pub(super) struct BatchProcessorStage {
    processor: Box<dyn BatchProcessor>,
    input: Option<DataChunkView>,
    ended: bool,
    finished: bool,
}

impl BatchProcessorStage {
    pub(super) fn new(processor: Box<dyn BatchProcessor>) -> Self {
        Self { processor, input: None, ended: false, finished: false }
    }
}

impl ResumableOperator for BatchProcessorStage {
    type WaitReason = Infallible;

    fn push_input(&mut self, port: OperatorInputPort, input: InputEvent) -> Result<Handoff<InputEvent>> {
        if port.index() != 0 { return Err(VeinError::invalid_input("batch transform has only input port 0")); }
        if self.ended || self.finished { return Err(VeinError::invalid_input("batch transform input is closed")); }
        if self.input.is_some() { return Ok(Handoff::Retry(input)); }
        match input {
            InputEvent::Batch(batch) => self.input = Some(batch),
            InputEvent::EndOfInput => self.ended = true,
        }
        Ok(Handoff::Accepted)
    }

    fn step(&mut self) -> Result<OperatorStep<Self::WaitReason>> {
        if self.finished { return Err(VeinError::internal("batch transform already finished")); }
        if let Some(input) = self.input.take() {
            return Ok(match self.processor.process(input)? {
                Some(output) => OperatorStep::Output(output),
                None => OperatorStep::Runnable,
            });
        }
        if self.ended {
            self.finished = true;
            return Ok(OperatorStep::Finished);
        }
        Ok(OperatorStep::NeedsInput)
    }
}

impl LocalStateOperator for BatchProcessorStage {
    type LocalState = ();

    fn take_local_state(&mut self) -> Result<Self::LocalState> {
        if !self.finished { return Err(VeinError::internal("batch transform is not finished")); }
        Ok(())
    }
}
