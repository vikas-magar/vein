use crate::pipeline::DataChunkView;
use crate::pipeline::resumable::{
    Handoff, InputEvent, LocalStateOperator, OperatorInputPort, OperatorStep, ResumableOperator,
};
use common::{Result, VeinError};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

pub(in crate::pipeline::resumable::executable::tests) enum Action {
    Need,
    Work,
    Hook(Box<dyn FnOnce() + Send>),
    Block(u8),
    Output(DataChunkView),
    Forward,
    Finish,
    Fail,
    AcceptBatch,
    AcceptEof,
    RetryBatch,
    RetryEof,
}

#[derive(Default)]
pub(in crate::pipeline::resumable::executable::tests) struct Record {
    pub steps: usize,
    pub offers: usize,
    pub batches: usize,
    pub eof: usize,
    pub combined: usize,
    pub received: Vec<DataChunkView>,
}

pub(super) struct Script {
    pub actions: VecDeque<Action>,
    pub record: Arc<Mutex<Record>>,
    pub sink: bool,
    pub input: Option<DataChunkView>,
}

impl Script {
    fn next(&mut self) -> Result<Action> {
        self.actions
            .pop_front()
            .ok_or_else(|| VeinError::internal("test script exhausted"))
    }
}

impl ResumableOperator for Script {
    type WaitReason = u8;
    fn push_input(
        &mut self,
        _: OperatorInputPort,
        input: InputEvent,
    ) -> Result<Handoff<InputEvent>> {
        self.record.lock().unwrap().offers += 1;
        let action = self.next()?;
        match (action, input) {
            (Action::RetryBatch, input @ InputEvent::Batch(_))
            | (Action::RetryEof, input @ InputEvent::EndOfInput) => Ok(Handoff::Retry(input)),
            (Action::AcceptBatch, InputEvent::Batch(view)) => {
                let mut record = self.record.lock().unwrap();
                record.batches += 1;
                if self.sink {
                    record.received.push(view);
                } else {
                    assert!(self.input.is_none(), "driver must drain the previous input");
                    self.input = Some(view);
                }
                Ok(Handoff::Accepted)
            }
            (Action::AcceptEof, InputEvent::EndOfInput) => {
                self.record.lock().unwrap().eof += 1;
                Ok(Handoff::Accepted)
            }
            _ => Err(VeinError::internal(
                "driver offered input at the wrong script position",
            )),
        }
    }

    fn step(&mut self) -> Result<OperatorStep<u8>> {
        self.record.lock().unwrap().steps += 1;
        Ok(match self.next()? {
            Action::Need => OperatorStep::NeedsInput,
            Action::Work => OperatorStep::Runnable,
            Action::Hook(hook) => {
                hook();
                OperatorStep::Runnable
            }
            Action::Block(reason) => OperatorStep::Blocked(reason),
            Action::Output(view) => OperatorStep::Output(view),
            Action::Forward => OperatorStep::Output(
                self.input
                    .take()
                    .ok_or_else(|| VeinError::internal("forwarded before input acceptance"))?,
            ),
            Action::Finish => OperatorStep::Finished,
            Action::Fail => return Err(VeinError::internal("injected stage failure")),
            _ => {
                return Err(VeinError::internal(
                    "driver stepped at an input-only script position",
                ));
            }
        })
    }
}

impl LocalStateOperator for Script {
    type LocalState = ();
    fn take_local_state(&mut self) -> Result<()> {
        assert!(
            self.actions.is_empty(),
            "finalization must drain the complete script"
        );
        Ok(())
    }
}
