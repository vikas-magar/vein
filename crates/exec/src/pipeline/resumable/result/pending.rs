use super::{OutputOrder, core::{Core, Entry}};
use common::Result;
use std::sync::Arc;

/// Accounted producer-owned output; drop it to abandon the batch safely.
pub struct PendingResult {
    pub(super) core: Arc<Core>,
    pub(super) producer: usize,
    pub(super) entry: Option<Entry>,
}

impl PendingResult {
    /// Transfers the same view to its output queue without copying Arrow buffers.
    pub fn publish(mut self) -> Result<()> {
        {
            let mut state = self.core.lock();
            Core::ensure_open(&state)?;
            let entry = self.entry.take().expect("pending result is published once");
            state.pending -= 1;
            state.queued += 1;
            state.producers[self.producer].busy = false;
            state.producers[self.producer].queue.push_back(entry);
            if self.core.config.order == OutputOrder::Unordered { state.arrivals.push_back(self.producer); }
        }
        self.core.notify_ready();
        Ok(())
    }
}

impl Drop for PendingResult {
    fn drop(&mut self) {
        if let Some(entry) = self.entry.take() {
            {
                let mut state = self.core.lock();
                entry.footprint.release(&mut state);
                state.pending -= 1;
                state.producers[self.producer].busy = false;
            }
            drop(entry);
            self.core.notify_ready();
        }
    }
}
