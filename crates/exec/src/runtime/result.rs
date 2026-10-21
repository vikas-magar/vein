use crate::pipeline::DataChunkView;
use crate::operators::sink::WriteReceipt;

/// Per-task execution metrics, reported to the master on completion.
#[derive(Debug, Clone, Copy, Default)]
pub struct TaskMetrics {
    /// Bytes read from the source.
    pub bytes_read: u64,
    /// Wall-clock execution time in nanoseconds.
    pub duration_ns: u64,
}

/// The outcome of running a single task to completion.
#[derive(Debug, Clone, Default)]
pub struct TaskResult {
    /// Number of rows produced by the pipeline.
    pub row_count: u64,
    /// Captured output batches (populated by a [`Sink`] that retains results).
    pub output: Vec<DataChunkView>,
    /// Execution metrics (bytes read, duration).
    pub metrics: TaskMetrics,
    /// Present only after a storage sink has committed successfully.
    pub write_receipt: Option<WriteReceipt>,
}
