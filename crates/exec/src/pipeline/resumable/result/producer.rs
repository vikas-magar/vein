use super::{Admission, ResultPermit, ResultWait, core::{Core, Terminal}, wait::WaitKind};
use common::{Result, VeinError};
use std::sync::Arc;

/// One uniquely owned output ordinal; complete it explicitly after finalization.
pub struct ResultProducer {
    pub(super) core: Arc<Core>,
    pub(super) id: usize,
    pub(super) finished: bool,
}

impl ResultProducer {
    /// Reserves buffer credit before constructing the next output view.
    /// Credit includes output still held by the producer, not just queued data.
    pub fn try_acquire(&mut self) -> Result<Admission> {
        self.core.tracker.check_cancelled()?;
        let mut state = self.core.lock();
        Core::ensure_open(&state)?;
        let slot = &state.producers[self.id];
        if slot.finished { return Err(VeinError::invalid_input("result producer is already finished")); }
        if slot.busy { return Err(VeinError::invalid_input("result producer already owns an output permit")); }
        if !self.core.can_acquire(&state, self.id) {
            return Ok(Admission::Blocked(ResultWait::new(&self.core, WaitKind::Capacity(self.id))));
        }
        state.producers[self.id].busy = true;
        state.reserved += self.core.config.max_batch_bytes.get();
        Ok(Admission::Ready(ResultPermit { core: Arc::clone(&self.core), producer: self.id, active: true }))
    }

    /// Marks this ordinal complete; queued batches remain available to consumers.
    pub fn finish(&mut self) -> Result<()> {
        if self.finished { return Ok(()); }
        {
            let mut state = self.core.lock();
            Core::ensure_open(&state)?;
            if state.producers[self.id].busy {
                return Err(VeinError::invalid_input("result producer cannot finish with pending output"));
            }
            state.producers[self.id].finished = true;
            self.core.advance(&mut state);
            self.finished = true;
        }
        self.core.notify_ready();
        Ok(())
    }

    /// Fails the stream, retaining the original error for its consumer.
    pub fn fail(&mut self, error: VeinError) {
        self.finished = true;
        self.core.terminate(Terminal::Failed, Some(error));
    }
}

impl Drop for ResultProducer {
    fn drop(&mut self) {
        if !self.finished {
            if self.core.query.get().is_some_and(|query| query.cancellation().is_cancelled()) {
                self.core.terminate(Terminal::Cancelled, None);
            } else {
                self.core.terminate(Terminal::Failed, Some(VeinError::internal("result producer dropped before completion")));
            }
        }
    }
}
