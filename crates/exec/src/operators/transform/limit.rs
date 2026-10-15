//! Limit transform operator: truncates and offsets in-flight rows.

use crate::pipeline::{DataChunk, PipelineBatch, PipelineNode, Transform};
use common::Result;

/// A synchronous [`Transform`] that limits the number of rows produced by a pipeline.
pub struct LimitTransform<B = DataChunk> {
    offset: u64,
    count: u64,
    rows_seen: u64,
    rows_emitted: u64,
    next: Box<dyn PipelineNode<B>>,
}

impl<B: PipelineBatch> LimitTransform<B> {
    /// Create a new [`LimitTransform`] with the given offset and count.
    pub fn new(offset: u64, count: u64, next: Box<dyn PipelineNode<B>>) -> Self {
        Self {
            offset,
            count,
            rows_seen: 0,
            rows_emitted: 0,
            next,
        }
    }
}

impl<B: PipelineBatch> PipelineNode<B> for LimitTransform<B> {
    fn push(&mut self, chunk: B) -> Result<()> {
        if self.rows_emitted >= self.count {
            // Maximum rows already reached; drop further pushes
            return Ok(());
        }

        let chunk_rows = chunk.num_rows();
        if chunk_rows == 0 {
            return Ok(());
        }

        // Check how many rows of this chunk fall before `offset`
        if self.rows_seen + (chunk_rows as u64) <= self.offset {
            // Whole chunk is before offset
            self.rows_seen += chunk_rows as u64;
            return Ok(());
        }

        let start_in_chunk = if self.rows_seen < self.offset {
            (self.offset - self.rows_seen) as usize
        } else {
            0
        };

        let remaining_to_emit = (self.count - self.rows_emitted) as usize;
        let available = chunk_rows - start_in_chunk;
        let take_count = available.min(remaining_to_emit);

        self.rows_seen += chunk_rows as u64;
        self.rows_emitted += take_count as u64;

        if take_count == 0 {
            return Ok(());
        }

        let limited = chunk.slice_rows(start_in_chunk, take_count)?;
        self.next.push(limited)
    }

    fn flush(&mut self) -> Result<()> {
        self.next.flush()
    }

    /// Once the window is full, nothing further down can matter.
    fn finished(&self) -> bool {
        self.rows_emitted >= self.count
    }
}

impl<B: PipelineBatch> Transform<B> for LimitTransform<B> {}

#[cfg(test)]
mod tests;
