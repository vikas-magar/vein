use arrow::record_batch::RecordBatch;
use common::Result;
use crate::daemon::pipeline::{PipelineNode, Sink};
use tracing::info;

/// A simple sink that discards all data.
/// Useful for benchmarking, testing, or development.
pub struct Discard {
    rows_seen: usize,
}

impl Discard {
    /// Create a new Discard sink.
    pub fn new() -> Self {
        Self { rows_seen: 0 }
    }
}

impl PipelineNode for Discard {
    fn push(&mut self, batch: RecordBatch) -> Result<()> {
        self.rows_seen += batch.num_rows();
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        info!("Discard Sink finished, discarded {} total rows", self.rows_seen);
        Ok(())
    }
}

impl Sink for Discard {}
