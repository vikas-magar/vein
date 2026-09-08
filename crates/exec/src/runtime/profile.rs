//! Operator telemetry and lifecycle profiling.
//!
//! Provides [`OperatorProfile`], [`OperatorMetrics`], and the [`ProfileNode`] decorator
//! for non-intrusive monitoring of operator performance (rows in/out, chunk counts,
//! CPU duration, memory consumption, and disk spilling) across pipeline stages.

use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::daemon::pipeline::{DataChunk, PipelineNode, Transform};
use error::Result;

/// Telemetry metrics collected for a pipeline operator during execution.
pub trait OperatorProfile: Send + Sync {
    /// The canonical name of the operator (e.g. "FilterExecutor", "ProjectExecutor").
    fn name(&self) -> &'static str;

    /// Total number of logical rows received by this operator.
    fn input_rows(&self) -> usize;

    /// Total number of logical rows emitted downstream by this operator.
    fn output_rows(&self) -> usize;

    /// Total number of input [`DataChunk`]s received.
    fn input_chunks(&self) -> usize;

    /// Total number of output [`DataChunk`]s emitted.
    fn output_chunks(&self) -> usize;

    /// Cumulative CPU duration spent in this operator in nanoseconds.
    fn duration_ns(&self) -> u64;

    /// Active or peak memory allocated by this operator in bytes (for joins/aggs/sorts).
    fn memory_bytes(&self) -> usize {
        0
    }

    /// Number of times this operator spilled data to disk or external storage.
    fn spill_count(&self) -> usize {
        0
    }

    /// Total bytes spilled to disk or external storage.
    fn spilled_bytes(&self) -> usize {
        0
    }

    /// Selectivity ratio (output_rows / input_rows) clamped between 0.0 and 1.0.
    fn selectivity(&self) -> f64 {
        let in_rows = self.input_rows();
        if in_rows == 0 {
            1.0
        } else {
            (self.output_rows() as f64 / in_rows as f64).clamp(0.0, 1.0)
        }
    }

    /// Execution throughput in logical rows processed per second.
    fn throughput_rows_per_sec(&self) -> f64 {
        let nanos = self.duration_ns();
        if nanos == 0 {
            0.0
        } else {
            (self.input_rows() as f64) / (nanos as f64 / 1_000_000_000.0)
        }
    }
}

/// Thread-safe atomic counters tracking runtime metrics for an individual operator.
#[derive(Debug, Default)]
pub struct OperatorMetrics {
    name: &'static str,
    input_rows: AtomicUsize,
    output_rows: AtomicUsize,
    input_chunks: AtomicUsize,
    output_chunks: AtomicUsize,
    duration_ns: AtomicU64,
    memory_bytes: AtomicUsize,
    spill_count: AtomicUsize,
    spilled_bytes: AtomicUsize,
}

impl OperatorMetrics {
    /// Create a new metrics tracker for the given operator `name`.
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            input_rows: AtomicUsize::new(0),
            output_rows: AtomicUsize::new(0),
            input_chunks: AtomicUsize::new(0),
            output_chunks: AtomicUsize::new(0),
            duration_ns: AtomicU64::new(0),
            memory_bytes: AtomicUsize::new(0),
            spill_count: AtomicUsize::new(0),
            spilled_bytes: AtomicUsize::new(0),
        }
    }

    /// Record an incoming [`DataChunk`] with the given number of active rows.
    pub fn record_input_chunk(&self, rows: usize) {
        self.input_chunks.fetch_add(1, Ordering::Relaxed);
        self.input_rows.fetch_add(rows, Ordering::Relaxed);
    }

    /// Record an emitted [`DataChunk`] with the given number of active rows.
    pub fn record_output_chunk(&self, rows: usize) {
        self.output_chunks.fetch_add(1, Ordering::Relaxed);
        self.output_rows.fetch_add(rows, Ordering::Relaxed);
    }

    /// Record CPU duration in nanoseconds.
    pub fn record_duration(&self, nanos: u64) {
        self.duration_ns.fetch_add(nanos, Ordering::Relaxed);
    }

    /// Update current active or peak memory consumption in bytes.
    pub fn set_memory_bytes(&self, bytes: usize) {
        self.memory_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Record a disk spilling event with the spilled byte size.
    pub fn record_spill(&self, bytes: usize) {
        self.spill_count.fetch_add(1, Ordering::Relaxed);
        self.spilled_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Take an immutable point-in-time snapshot of the metrics.
    pub fn snapshot(&self) -> OperatorSnapshot {
        OperatorSnapshot {
            name: self.name,
            input_rows: self.input_rows.load(Ordering::Relaxed),
            output_rows: self.output_rows.load(Ordering::Relaxed),
            input_chunks: self.input_chunks.load(Ordering::Relaxed),
            output_chunks: self.output_chunks.load(Ordering::Relaxed),
            duration_ns: self.duration_ns.load(Ordering::Relaxed),
            memory_bytes: self.memory_bytes.load(Ordering::Relaxed),
            spill_count: self.spill_count.load(Ordering::Relaxed),
            spilled_bytes: self.spilled_bytes.load(Ordering::Relaxed),
        }
    }
}

impl OperatorProfile for OperatorMetrics {
    fn name(&self) -> &'static str {
        self.name
    }

    fn input_rows(&self) -> usize {
        self.input_rows.load(Ordering::Relaxed)
    }

    fn output_rows(&self) -> usize {
        self.output_rows.load(Ordering::Relaxed)
    }

    fn input_chunks(&self) -> usize {
        self.input_chunks.load(Ordering::Relaxed)
    }

    fn output_chunks(&self) -> usize {
        self.output_chunks.load(Ordering::Relaxed)
    }

    fn duration_ns(&self) -> u64 {
        self.duration_ns.load(Ordering::Relaxed)
    }

    fn memory_bytes(&self) -> usize {
        self.memory_bytes.load(Ordering::Relaxed)
    }

    fn spill_count(&self) -> usize {
        self.spill_count.load(Ordering::Relaxed)
    }

    fn spilled_bytes(&self) -> usize {
        self.spilled_bytes.load(Ordering::Relaxed)
    }
}

/// An immutable point-in-time snapshot of an operator's execution metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorSnapshot {
    /// Name of the operator.
    pub name: &'static str,
    /// Total logical rows received.
    pub input_rows: usize,
    /// Total logical rows emitted.
    pub output_rows: usize,
    /// Total input chunks received.
    pub input_chunks: usize,
    /// Total output chunks emitted.
    pub output_chunks: usize,
    /// Total CPU time in nanoseconds.
    pub duration_ns: u64,
    /// Memory consumed in bytes.
    pub memory_bytes: usize,
    /// Total spill events.
    pub spill_count: usize,
    /// Total bytes spilled.
    pub spilled_bytes: usize,
}

impl OperatorProfile for OperatorSnapshot {
    fn name(&self) -> &'static str {
        self.name
    }

    fn input_rows(&self) -> usize {
        self.input_rows
    }

    fn output_rows(&self) -> usize {
        self.output_rows
    }

    fn input_chunks(&self) -> usize {
        self.input_chunks
    }

    fn output_chunks(&self) -> usize {
        self.output_chunks
    }

    fn duration_ns(&self) -> u64 {
        self.duration_ns
    }

    fn memory_bytes(&self) -> usize {
        self.memory_bytes
    }

    fn spill_count(&self) -> usize {
        self.spill_count
    }

    fn spilled_bytes(&self) -> usize {
        self.spilled_bytes
    }
}

impl fmt::Display for OperatorSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let millis = self.duration_ns as f64 / 1_000_000.0;
        write!(
            f,
            "{}: in={}/{} (rows/chunks), out={}/{} (rows/chunks), time={:.2}ms",
            self.name, self.input_rows, self.input_chunks, self.output_rows, self.output_chunks, millis
        )?;
        if self.memory_bytes > 0 {
            let kb = self.memory_bytes as f64 / 1024.0;
            write!(f, ", mem={:.1}KB", kb)?;
        }
        if self.spill_count > 0 {
            write!(f, ", spills={}", self.spill_count)?;
        }
        Ok(())
    }
}

/// A decorator wrapping any [`PipelineNode`] to collect fine-grained telemetry
/// metrics (rows processed, chunk counts, CPU duration) with zero changes to operator logic.
pub struct ProfileNode<N> {
    inner: N,
    metrics: Arc<OperatorMetrics>,
}

impl<N> ProfileNode<N> {
    /// Wrap `inner` node with an [`Arc<OperatorMetrics>`] tracker.
    pub fn new(inner: N, metrics: Arc<OperatorMetrics>) -> Self {
        Self { inner, metrics }
    }

    /// Returns a reference to the shared metrics tracker.
    pub fn metrics(&self) -> &Arc<OperatorMetrics> {
        &self.metrics
    }

    /// Returns a reference to the wrapped inner node.
    pub fn inner(&self) -> &N {
        &self.inner
    }

    /// Returns a mutable reference to the wrapped inner node.
    pub fn inner_mut(&mut self) -> &mut N {
        &mut self.inner
    }

    /// Unwraps this decorator, returning the underlying inner node.
    pub fn into_inner(self) -> N {
        self.inner
    }
}

impl<N: PipelineNode<DataChunk>> PipelineNode<DataChunk> for ProfileNode<N> {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        let in_rows = chunk.num_rows();
        self.metrics.record_input_chunk(in_rows);

        let start = Instant::now();
        let result = self.inner.push(chunk);
        let elapsed = start.elapsed().as_nanos() as u64;
        self.metrics.record_duration(elapsed);

        result
    }

    fn flush(&mut self) -> Result<()> {
        let start = Instant::now();
        let result = self.inner.flush();
        let elapsed = start.elapsed().as_nanos() as u64;
        self.metrics.record_duration(elapsed);

        result
    }
}

impl<N: PipelineNode<DataChunk>> Transform<DataChunk> for ProfileNode<N> {}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use std::sync::Arc;

    struct DummySink {
        pushed_chunks: usize,
    }

    impl PipelineNode<DataChunk> for DummySink {
        fn push(&mut self, _chunk: DataChunk) -> Result<()> {
            self.pushed_chunks += 1;
            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_profile_node_records_metrics() {
        let metrics = Arc::new(OperatorMetrics::new("DummySink"));
        let sink = DummySink { pushed_chunks: 0 };
        let mut profiled = ProfileNode::new(sink, metrics.clone());

        let array = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as arrow::array::ArrayRef;
        let chunk = DataChunk::from_vectors(vec![array]);

        profiled.push(chunk).unwrap();
        profiled.flush().unwrap();

        assert_eq!(profiled.inner().pushed_chunks, 1);
        assert_eq!(metrics.name(), "DummySink");
        assert_eq!(metrics.input_rows(), 5);
        assert_eq!(metrics.input_chunks(), 1);

        metrics.record_output_chunk(3);
        assert_eq!(metrics.output_rows(), 3);
        assert_eq!(metrics.output_chunks(), 1);
        assert!((metrics.selectivity() - 0.6).abs() < 1e-6);

        metrics.set_memory_bytes(1024 * 64);
        assert_eq!(metrics.memory_bytes(), 1024 * 64);

        metrics.record_spill(2048);
        assert_eq!(metrics.spill_count(), 1);
        assert_eq!(metrics.spilled_bytes(), 2048);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.input_rows, 5);
        assert_eq!(snapshot.name, "DummySink");
        let display = format!("{}", snapshot);
        assert!(display.contains("DummySink: in=5/1 (rows/chunks)"));
        assert!(display.contains("mem=64.0KB"));
        assert!(display.contains("spills=1"));
    }
}
