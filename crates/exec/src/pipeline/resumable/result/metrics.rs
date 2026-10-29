use super::{ResultConsumer, accounting::Footprint};
use crate::pipeline::DataChunkView;
use common::Result;

/// Buffer-owned credit and retained-footprint observations; these exclude client-held batches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResultBufferMetrics {
    /// Capacity reserved by producers that have not prepared output yet.
    pub reserved_bytes: usize,
    /// Unique array footprints plus per-view metadata for pending and queued output.
    pub retained_bytes: usize,
    /// Prepared batches still owned by their producers.
    pub pending_batches: usize,
    /// Published batches awaiting fetch, including ordered read-ahead.
    pub queued_batches: usize,
    /// Distinct retained ArrayRef identities, shared across all pending/queued entries.
    pub retained_arrays: usize,
}

/// Standalone retained estimate used by the per-batch admission limit.
/// Shared arrays across distinct batches are deduplicated after admission.
pub fn result_batch_footprint(view: &DataChunkView) -> Result<usize> { Ok(Footprint::measure(view)?.total) }

impl ResultConsumer {
    /// Captures bounded counters without reading or logging column values.
    pub fn metrics(&self) -> ResultBufferMetrics {
        let state = self.core.lock();
        ResultBufferMetrics {
            reserved_bytes: state.reserved,
            retained_bytes: state.retained,
            pending_batches: state.pending,
            queued_batches: state.queued,
            retained_arrays: state.arrays.len(),
        }
    }
}
