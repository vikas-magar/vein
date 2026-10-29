use super::{ScanCursor, ScanReady};
use crate::pipeline::resumable::{BatchProcessor, Handoff, InputEvent, LocalStateOperator, OperatorInputPort,
                                 OperatorStep, ResumableOperator};
use common::{Result, VeinError, cancellation::CancellationToken};
use std::task::{Context, Poll};

/// Scheduler-facing source over the same cursor drained by the production adapter.
pub struct ScanStage {
    cursor: Option<ScanCursor<'static>>,
    processor: Option<Box<dyn BatchProcessor>>,
    ready: ScanReady,
    cancel: CancellationToken,
    finished: bool,
}
impl ScanStage {
    /// Creates local execution state without polling or starting storage reads.
    pub fn new(cursor: ScanCursor<'static>, cancel: CancellationToken) -> Self {
        Self { cursor: Some(cursor), processor: None, ready: ScanReady::default(), cancel, finished: false }
    }

    /// Applies a worker-local processor before the scan batch reaches the graph.
    pub fn with_processor(mut self, processor: Box<dyn BatchProcessor>) -> Self {
        self.processor = Some(processor);
        self
    }
}
impl ResumableOperator for ScanStage {
    type WaitReason = ScanReady;
    fn push_input(&mut self, _: OperatorInputPort, _: InputEvent) -> Result<Handoff<InputEvent>> {
        Err(VeinError::invalid_input("scan source does not accept upstream input"))
    }
    fn step(&mut self) -> Result<OperatorStep<ScanReady>> {
        if self.finished { return Err(VeinError::internal("scan source already finished")); }
        if self.cancel.is_cancelled() {
            self.cursor = None;
            self.finished = true;
            return Err(VeinError::cancelled());
        }
        let waker = self.ready.before_poll();
        let cursor = self.cursor.as_mut().ok_or_else(|| VeinError::internal("scan cursor missing"))?;
        match cursor.as_mut().poll_next(&mut Context::from_waker(&waker)) {
            Poll::Pending => Ok(OperatorStep::Blocked(self.ready.clone())),
            Poll::Ready(Some(Ok(view))) => match self.processor.as_mut() {
                Some(processor) => Ok(match processor.process(view)? {
                    Some(output) => OperatorStep::Output(output),
                    None => OperatorStep::Runnable,
                }),
                None => Ok(OperatorStep::Output(view)),
            },
            Poll::Ready(result) => {
                self.cursor = None;
                self.finished = true;
                match result {
                    Some(Err(error)) => Err(error),
                    None => Ok(OperatorStep::Finished),
                    Some(Ok(_)) => unreachable!(),
                }
            }
        }
    }
}
impl LocalStateOperator for ScanStage {
    type LocalState = ();
    fn take_local_state(&mut self) -> Result<()> {
        if !self.finished { return Err(VeinError::internal("scan is not finished")); }
        Ok(())
    }
}
