use super::{QueryStatus, super::QueryExecutionResources};
use std::time::Duration;

/// Observed query counters, not a guessed percentage of completion.
#[derive(Debug, Clone)]
pub struct QueryProgress {
    /// Current lifecycle state.
    pub status: QueryStatus,
    /// Rows transferred to the owning cursor's client.
    pub rows_delivered: u64,
    /// Batches transferred, including empty batches.
    pub batches_delivered: u64,
    /// Elapsed execution/drain time, frozen on termination.
    pub elapsed: Duration,
    /// Explicit retained reservations; not allocator or RSS usage.
    pub memory_used_bytes: usize,
    /// Peak explicit reservation across stages.
    pub memory_peak_bytes: usize,
    /// Current accounted spill bytes.
    pub spill_used_bytes: u64,
}

impl QueryExecutionResources {
    /// Takes a bounded counter snapshot without cloning statistics or data.
    pub fn query_progress(&self) -> QueryProgress {
        let state = self.lifecycle.lock();
        QueryProgress {
            status: state.status,
            rows_delivered: state.rows,
            batches_delivered: state.batches,
            elapsed: state.started.map_or(state.elapsed, |start| start.elapsed()),
            memory_used_bytes: self.memory.used(),
            memory_peak_bytes: self.memory.peak(),
            spill_used_bytes: self.memory.spill_bytes_used(),
        }
    }
}
