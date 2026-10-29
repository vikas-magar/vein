use super::{PendingResult, ResultWait, accounting::Footprint, core::{Core, Entry}};
use crate::pipeline::DataChunkView;
use common::{Result, VeinError};
use std::sync::Arc;

/// Either ownership of output capacity or readiness information for T06.
pub enum Admission {
    /// Credit is retained until publication/fetch or the permit is dropped.
    Ready(ResultPermit),
    /// Retry admission after registering this resource with the scheduler.
    Blocked(ResultWait),
}

/// Capacity for one pending output. Dropping unused credit wakes waiting producers.
pub struct ResultPermit {
    pub(super) core: Arc<Core>,
    pub(super) producer: usize,
    pub(super) active: bool,
}

impl ResultPermit {
    /// Accounts a completed view without publishing it or gathering column data.
    /// Oversized retained arrays and exhausted query budgets return errors, never
    /// an indefinite capacity wait. Decoder allocations preceding this call are
    /// outside this buffer's memory leases, as with existing operator accounting.
    pub fn prepare(mut self, view: DataChunkView) -> Result<PendingResult> {
        let footprint = Footprint::measure(&view)?;
        if footprint.total > self.core.config.max_batch_bytes.get() {
            return Err(VeinError::resource_exhausted("result batch retained footprint",
                                                     footprint.total as u128, self.core.config.max_batch_bytes.get() as u128));
        }
        let batch = footprint.acquire(view, &self.core.tracker)?;
        {
            let mut state = self.core.lock();
            Core::ensure_open(&state)?;
            state.reserved -= self.core.config.max_batch_bytes.get();
            footprint.retain(&mut state);
            state.pending += 1;
            self.active = false;
        }
        let pending = PendingResult { core: Arc::clone(&self.core), producer: self.producer, entry: Some(Entry { batch, footprint }) };
        self.core.notify_ready();
        Ok(pending)
    }
}

impl Drop for ResultPermit {
    fn drop(&mut self) {
        if self.active {
            {
                let mut state = self.core.lock();
                state.reserved -= self.core.config.max_batch_bytes.get();
                state.producers[self.producer].busy = false;
            }
            self.core.notify_ready();
        }
    }
}
