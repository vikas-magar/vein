use super::{OutputOrder, ResultBatch, ResultWait, core::{Core, Terminal}, wait::WaitKind};
use common::{Result, VeinError};
use std::sync::Arc;

/// Nonblocking result delivery; an empty view is a batch, not completion.
pub enum FetchResult {
    /// A view whose query memory remains leased while this handle is retained.
    Batch(ResultBatch),
    /// No visible batch yet; subscribe before waiting to avoid a lost wakeup.
    Pending(ResultWait),
    /// All producers completed and all published output has been fetched, or closed.
    Finished,
}

/// The single consuming side of a shared result buffer.
pub struct ResultConsumer {
    pub(super) core: Arc<Core>,
}

impl ResultConsumer {
    /// Fetches the next visible view, releasing its output-capacity credit.
    /// Query array/metadata leases stay with the returned ResultBatch.
    pub fn try_fetch(&mut self) -> Result<FetchResult> {
        let entry = {
            let mut state = self.core.lock();
            match state.terminal {
                Some(Terminal::Closed) => return Ok(FetchResult::Finished),
                Some(Terminal::Cancelled) => return Err(VeinError::cancelled()),
                Some(Terminal::Failed) => return Err(state.error.take()
                    .unwrap_or_else(|| VeinError::internal("result stream failed"))),
                None => {}
            }
            if !self.core.data_ready(&state) {
                return Ok(if self.core.complete(&state) { FetchResult::Finished } else { FetchResult::Pending(ResultWait::new(&self.core, WaitKind::Data)) });
            }
            let producer = match self.core.config.order {
                OutputOrder::Unordered => state.arrivals.pop_front().unwrap(),
                OutputOrder::ProducerOrder => state.frontier,
            };
            let entry = state.producers[producer].queue.pop_front().unwrap();
            entry.footprint.release(&mut state);
            state.queued -= 1;
            self.core.advance(&mut state);
            entry
        };
        self.core.notify_ready();
        Ok(FetchResult::Batch(entry.batch))
    }

    /// Stops result delivery and releases queued output; returned handles remain valid.
    pub fn close(&mut self) { self.core.terminate(Terminal::Closed, None); }
    /// Cancels delivery and wakes every producer/consumer subscription.
    pub fn cancel(&mut self) { self.core.terminate(Terminal::Cancelled, None); }
}

impl Drop for ResultConsumer {
    fn drop(&mut self) { self.close(); }
}
