//! The limit operator.

use common::Result;
use vector::DataChunk;

use crate::pipeline::PipelineNode;

/// Applies an offset and a row limit across the whole stream.
///
/// The state is the count already skipped and already emitted, because a limit
/// is a property of the stream rather than of a chunk: a chunk that arrives
/// after the limit is satisfied is dropped without narrowing anything.
///
/// The window is applied to *active* rows in order, so a limit above a filter
/// bounds the surviving rows rather than the physical ones.
pub struct Limit {
    offset: u32,
    limit: u32,
    skipped: u32,
    produced: u32,
    next: Box<dyn PipelineNode<DataChunk>>,
}

impl Limit {
    /// Builds a limit feeding `next`.
    pub fn new(offset: u32, limit: u32, next: Box<dyn PipelineNode<DataChunk>>) -> Self {
        Self {
            offset,
            limit,
            skipped: 0,
            produced: 0,
            next,
        }
    }
}

impl PipelineNode<DataChunk> for Limit {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        if self.produced >= self.limit {
            return Ok(());
        }
        let row_count = chunk.row_count();
        let active = chunk.selection().active_count(row_count);

        // Skip only what this chunk can still contribute to the offset.
        let to_skip = self.offset.saturating_sub(self.skipped).min(active);
        self.skipped += to_skip;

        let remaining = self.limit - self.produced;
        let selection = chunk.selection().slice(row_count, to_skip, remaining);
        let taken = selection.active_count(row_count);
        if taken == 0 {
            return Ok(());
        }
        self.produced += taken;
        self.next.push(chunk.with_selection(selection))
    }

    fn flush(&mut self) -> Result<()> {
        self.next.flush()
    }
}
