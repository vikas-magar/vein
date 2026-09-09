//! Discard sink for benchmarking and development.

use crate::pipeline::{DataChunk, PipelineNode, Sink};
use common::Result;
use tracing::info;

/// A simple sink that discards all data.
/// Useful for benchmarking, testing, or development.
pub struct DiscardSink {
    rows_seen: usize,
}

impl DiscardSink {
    /// Create a new [`DiscardSink`].
    pub fn new() -> Self {
        Self { rows_seen: 0 }
    }
}

impl Default for DiscardSink {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineNode<DataChunk> for DiscardSink {
    fn push(&mut self, batch: DataChunk) -> Result<()> {
        self.rows_seen += batch.selection.size();
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        info!(
            "DiscardSink finished, discarded {} total rows",
            self.rows_seen
        );
        Ok(())
    }
}

impl Sink<DataChunk> for DiscardSink {}
