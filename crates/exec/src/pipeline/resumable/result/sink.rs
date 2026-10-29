use super::{Admission, ResultPermit, ResultProducer, ResultWait};
use crate::pipeline::resumable::{Handoff, InputEvent, LocalStateOperator, OperatorInputPort,
                                 OperatorStep, ResumableOperator};
use common::{Result, VeinError};

/// Client-neutral terminal sink; admission precedes requesting upstream data.
pub struct ResultSink {
    producer: ResultProducer,
    permit: Option<ResultPermit>,
    ended: bool,
}

impl ResultProducer {
    /// Moves this unique producer into a resumable terminal sink.
    pub fn into_sink(self) -> ResultSink { ResultSink { producer: self, permit: None, ended: false } }
}

impl ResumableOperator for ResultSink {
    type WaitReason = ResultWait;
    fn push_input(&mut self, port: OperatorInputPort, input: InputEvent) -> Result<Handoff<InputEvent>> {
        if port.index() != 0 || self.ended {
            return Err(VeinError::invalid_input("result sink received input on an invalid or closed port"));
        }
        let permit = self.permit.take().ok_or_else(|| VeinError::internal("result sink has no input admission permit"))?;
        match input {
            InputEvent::Batch(view) => permit.prepare(view)?.publish()?,
            InputEvent::EndOfInput => {
                drop(permit);
                self.ended = true;
            }
        }
        Ok(Handoff::Accepted)
    }
    fn step(&mut self) -> Result<OperatorStep<ResultWait>> {
        if self.ended {
            self.producer.finish()?;
            return Ok(OperatorStep::Finished);
        }
        if self.permit.is_none() {
            match self.producer.try_acquire()? {
                Admission::Ready(permit) => self.permit = Some(permit),
                Admission::Blocked(wait) => return Ok(OperatorStep::Blocked(wait)),
            }
        }
        Ok(OperatorStep::NeedsInput)
    }
}

impl LocalStateOperator for ResultSink {
    type LocalState = ();
    fn take_local_state(&mut self) -> Result<()> {
        if !self.ended { return Err(VeinError::internal("result sink local state requested before EOF")); }
        Ok(())
    }
}
