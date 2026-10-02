//! The filter operator.

use common::Result;
use vector::DataChunk;

use super::Predicate;
use crate::pipeline::PipelineNode;

/// Narrows a chunk's selection to the rows its predicate accepts.
///
/// The filter writes no data: it intersects the chunk's existing selection with
/// the predicate's mask, so narrowing costs a bitmap and the columns are never
/// touched (Rule 3). A chunk that keeps nothing is swallowed rather than
/// forwarded, so no downstream operator sees an empty chunk.
pub struct Filter<P> {
    predicate: P,
    next: Box<dyn PipelineNode<DataChunk>>,
}

impl<P: Predicate> Filter<P> {
    /// Builds a filter feeding `next`.
    pub fn new(predicate: P, next: Box<dyn PipelineNode<DataChunk>>) -> Self {
        Self { predicate, next }
    }
}

impl<P: Predicate> PipelineNode<DataChunk> for Filter<P> {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        let condition = self.predicate.evaluate(&chunk)?;
        // Only rows that are definitely true survive: the keep mask is the
        // condition's true bits intersected with its valid bits.
        let keep = condition.keep(chunk.row_count());
        let selection = chunk.selection().intersect_mask(chunk.row_count(), &keep);
        let chunk = chunk.with_selection(selection);
        if chunk.is_empty() {
            return Ok(());
        }
        self.next.push(chunk)
    }

    fn flush(&mut self) -> Result<()> {
        self.next.flush()
    }
}
