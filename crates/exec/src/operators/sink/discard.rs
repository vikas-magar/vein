//! Discard sink for benchmarking and development.

use crate::pipeline::{PipelineBatch, PipelineNode, Sink};
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

impl<B: PipelineBatch> PipelineNode<B> for DiscardSink {
    fn push(&mut self, batch: B) -> Result<()> {
        self.rows_seen += batch.num_rows();
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

impl<B: PipelineBatch> Sink<B> for DiscardSink {
    fn row_count(&self) -> u64 {
        self.rows_seen as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::{DataChunk, DataChunkView};
    use common::arrow::SelectionVector;

    #[test]
    fn discard_counts_chunk_and_view_rows_without_materialization() {
        let mut sink = DiscardSink::new();
        sink.push(DataChunk::new(vec![], SelectionVector::new_dense(2))).unwrap();
        sink.push(DataChunkView::from_chunk(
            DataChunk::new(vec![], SelectionVector::new_dense(3)),
        ).unwrap()).unwrap();
        assert_eq!(Sink::<DataChunkView>::row_count(&sink), 5);
        assert!(Sink::<DataChunkView>::collect(&mut sink).is_empty());
        PipelineNode::<DataChunkView>::flush(&mut sink).unwrap();
    }
}
