use super::{Lifecycle, QueryStatus};
use crate::operators::sink::WriteReceipt;
use common::{Result, VeinError, time::Instant};
use std::sync::Arc;

impl Lifecycle {
    pub fn claim_cursor(&self) -> Result<()> {
        let mut state = self.lock();
        if state.cursor_claimed {
            return Err(VeinError::invalid_input("query already has an owning result cursor"));
        }
        state.cursor_claimed = true;
        Ok(())
    }
    pub fn begin(&self) {
        let mut state = self.lock();
        if state.status == QueryStatus::Prepared {
            state.status = QueryStatus::Running;
            state.started = Some(Instant::now());
        }
    }
    pub fn terminate(&self, status: QueryStatus, error: Option<Arc<VeinError>>) {
        {
            let mut state = self.lock();
            if state.status.is_terminal() { return; }
            state.status = status;
            state.failure = error;
            Self::stop_clock(&mut state);
        }
        self.waker.wake();
    }
    pub fn fail(&self, error: VeinError) -> Arc<VeinError> {
        let error = error.into_shared();
        {
            let mut state = self.lock();
            if let Some(original) = &state.failure { return Arc::clone(original); }
            if !state.status.is_terminal() {
                state.status = if matches!(error.as_ref(), VeinError::Cancelled) {
                    QueryStatus::Cancelled
                } else { QueryStatus::Failed };
                state.failure = Some(Arc::clone(&error));
                Self::stop_clock(&mut state);
            }
        }
        self.waker.wake();
        error
    }
    pub fn finish(&self, receipt: Option<WriteReceipt>) {
        {
            let mut state = self.lock();
            // A confirmed commit is irreversible; a racing cancellation cannot
            // erase its receipt or claim that the write was rolled back.
            let committed_after_cancel = receipt.is_some() && state.status == QueryStatus::Cancelled;
            if !state.status.is_terminal() || committed_after_cancel {
                state.status = QueryStatus::Finished;
                state.failure = None;
                Self::stop_clock(&mut state);
            }
            if receipt.is_some() && state.receipt.is_none() {
                state.receipt = receipt.map(Arc::new);
            }
        }
        self.waker.wake();
    }
    pub fn delivered(&self, rows: usize) -> Result<()> {
        let mut state = self.lock();
        let rows = u64::try_from(rows).map_err(|_| VeinError::internal("result row count exceeds u64"))?;
        let next_rows = state.rows.checked_add(rows)
            .ok_or_else(|| VeinError::internal("delivered row count overflow"))?;
        let next_batches = state.batches.checked_add(1)
            .ok_or_else(|| VeinError::internal("delivered batch count overflow"))?;
        state.rows = next_rows;
        state.batches = next_batches;
        Ok(())
    }
}
