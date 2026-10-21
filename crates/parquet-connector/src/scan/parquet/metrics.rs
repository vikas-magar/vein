//! Low-cost scan predicate metrics used by optional detailed tracing.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[cfg(test)]
use arrow::array::{Array, BooleanArray};

/// Debug-only counters for the compiled predicate Arrow evaluates during a scan.
/// The counters are updated only when detailed scan telemetry is enabled, so
/// normal query execution does not pay for an extra pass over predicate output.
#[derive(Debug, Default)]
pub(super) struct ScanPredicateMetrics {
    pub(super) rows_before_filter: AtomicU64,
    pub(super) batches: AtomicU64,
    pub(super) rows_in: AtomicU64,
    pub(super) rows_kept: AtomicU64,
    pub(super) eval_wall_us: AtomicU64,
    pub(super) input_array_bytes: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ScanPredicateMetricsSnapshot {
    pub(super) rows_before_filter: u64,
    pub(super) batches: u64,
    pub(super) rows_in: u64,
    pub(super) rows_kept: u64,
    pub(super) eval_wall_us: u64,
    pub(super) input_array_bytes: u64,
}

impl ScanPredicateMetrics {
    pub(super) fn record_source_rows(&self, rows: usize) {
        self.rows_before_filter.fetch_add(rows as u64, Ordering::Relaxed);
    }

    pub(super) fn record_batch(
        &self,
        rows_in: usize,
        rows_kept: usize,
        elapsed: Duration,
        input_array_bytes: usize,
    ) {
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.rows_in.fetch_add(rows_in as u64, Ordering::Relaxed);
        self.rows_kept
            .fetch_add(rows_kept as u64, Ordering::Relaxed);
        self.eval_wall_us
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
        self.input_array_bytes
            .fetch_add(input_array_bytes as u64, Ordering::Relaxed);
    }

    pub(super) fn snapshot(&self) -> ScanPredicateMetricsSnapshot {
        ScanPredicateMetricsSnapshot {
            rows_before_filter: self.rows_before_filter.load(Ordering::Relaxed),
            batches: self.batches.load(Ordering::Relaxed),
            rows_in: self.rows_in.load(Ordering::Relaxed),
            rows_kept: self.rows_kept.load(Ordering::Relaxed),
            eval_wall_us: self.eval_wall_us.load(Ordering::Relaxed),
            input_array_bytes: self.input_array_bytes.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
pub(super) fn count_true_rows(values: &BooleanArray) -> usize {
    (0..values.len())
        .filter(|row| values.is_valid(*row) && values.value(*row))
        .count()
}
