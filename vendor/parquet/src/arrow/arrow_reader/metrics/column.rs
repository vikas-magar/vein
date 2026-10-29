//! Optional leaf-reader attribution; no column values or buffers are retained.

use std::cell::RefCell;
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use web_time::Instant;

/// Elapsed leaf-reader work, summed across calls. Decompression is a subset of
/// read, skip or materialization time, not an additional exclusive duration.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnReaderMetrics {
    /// Parquet leaf-column ordinal.
    pub column_index: usize,
    /// Calls that decode records, including nested page decompression.
    pub read_ns: u64,
    /// Calls that advance over rejected records.
    pub skip_ns: u64,
    /// Calls that finish Arrow arrays.
    pub materialize_ns: u64,
    /// Time inside the compression codec itself.
    pub decompress_ns: u64,
    /// Successful decoded record count.
    pub rows_read: u64,
    /// Successful skipped record count.
    pub rows_skipped: u64,
    /// Compressed bytes submitted to codec calls, including failed calls.
    pub compressed_bytes: u64,
    /// Expected decoded bytes submitted to codec calls, including failed calls.
    pub uncompressed_bytes: u64,
    /// Completed calls by phase: read, skip, materialize, decompress.
    pub calls: [u64; 4],
}

#[derive(Clone, Copy)]
pub(crate) enum ReadPhase { Read, Skip, Materialize, Decompress }

#[derive(Debug, Default)]
pub(crate) struct ColumnMetrics {
    elapsed: [AtomicU64; 4],
    calls: [AtomicU64; 4],
    pub(crate) rows_read: AtomicU64,
    pub(crate) rows_skipped: AtomicU64,
    compressed_bytes: AtomicU64,
    uncompressed_bytes: AtomicU64,
}

thread_local! {
    // Set only for a synchronous leaf-reader call, never across an await.
    static ACTIVE: RefCell<Option<Arc<ColumnMetrics>>> = const { RefCell::new(None) };
}

impl ColumnMetrics {
    pub(crate) fn measure<T>(self: &Arc<Self>, phase: ReadPhase, operation: impl FnOnce() -> T) -> T {
        let previous = ACTIVE.with(|active| active.replace(Some(Arc::clone(self))));
        let _scope = ActiveScope(previous);
        let _timer = self.timer(phase);
        operation()
    }

    fn timer(self: &Arc<Self>, phase: ReadPhase) -> ColumnTimer {
        ColumnTimer { metrics: Arc::clone(self), phase, started: Instant::now() }
    }

    pub(crate) fn snapshot(&self, column_index: usize) -> ColumnReaderMetrics {
        let value = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        ColumnReaderMetrics {
            column_index,
            read_ns: value(&self.elapsed[0]),
            skip_ns: value(&self.elapsed[1]),
            materialize_ns: value(&self.elapsed[2]),
            decompress_ns: value(&self.elapsed[3]),
            rows_read: value(&self.rows_read),
            rows_skipped: value(&self.rows_skipped),
            compressed_bytes: value(&self.compressed_bytes),
            uncompressed_bytes: value(&self.uncompressed_bytes),
            calls: std::array::from_fn(|index| value(&self.calls[index])),
        }
    }
}

struct ActiveScope(Option<Arc<ColumnMetrics>>);
impl Drop for ActiveScope {
    fn drop(&mut self) { ACTIVE.with(|active| active.replace(self.0.take())); }
}

pub(crate) struct ColumnTimer {
    metrics: Arc<ColumnMetrics>,
    phase: ReadPhase,
    started: Instant,
}
impl Drop for ColumnTimer {
    fn drop(&mut self) {
        let index = self.phase as usize;
        self.metrics.elapsed[index].fetch_add(
            self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64, Ordering::Relaxed);
        self.metrics.calls[index].fetch_add(1, Ordering::Relaxed);
    }
}

/// The codec can be called by readers on different executor threads. Attribution
/// is scoped to the current synchronous leaf call and restored on errors/unwind.
pub(crate) fn decompression_timer(compressed: usize, uncompressed: usize) -> Option<ColumnTimer> {
    ACTIVE.with(|active| active.borrow().as_ref().map(|metrics| {
        metrics.compressed_bytes.fetch_add(compressed as u64, Ordering::Relaxed);
        metrics.uncompressed_bytes.fetch_add(uncompressed as u64, Ordering::Relaxed);
        metrics.timer(ReadPhase::Decompress)
    }))
}
