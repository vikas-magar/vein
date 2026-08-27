use crate::daemon::pipeline::{DataChunk, PipelineNode, Sink};
use common::Result;
use tracing::info;

/// A simple sink that discards all data.
/// Useful for benchmarking, testing, or development.
pub struct DiscardExecutor {
    rows_seen: usize,
}

impl DiscardExecutor {
    /// Create a new DiscardExecutor sink.
    pub fn new() -> Self {
        Self { rows_seen: 0 }
    }
}

impl PipelineNode<DataChunk> for DiscardExecutor {
    fn push(&mut self, batch: DataChunk) -> Result<()> {
        self.rows_seen += batch.selection.size();
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        info!(
            "DiscardExecutor Sink finished, discarded {} total rows",
            self.rows_seen
        );
        Ok(())
    }
}

impl Sink<DataChunk> for DiscardExecutor {}
