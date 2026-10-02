//! The projection operator.

use common::Result;
use error::VeinError;
use vector::DataChunk;

use crate::pipeline::PipelineNode;

/// Narrows a chunk to a subset of its columns, in the given order.
///
/// Projection copies handles, not data: the columns are shared with the input
/// chunk, so a projection materialises nothing (Rule 3). An out-of-range column
/// is a planning bug and fails loudly rather than silently dropping a column,
/// which would produce a result missing a field.
pub struct Project {
    columns: Vec<usize>,
    next: Box<dyn PipelineNode<DataChunk>>,
}

impl Project {
    /// Builds a projection feeding `next`.
    pub fn new(columns: Vec<usize>, next: Box<dyn PipelineNode<DataChunk>>) -> Self {
        Self { columns, next }
    }
}

impl PipelineNode<DataChunk> for Project {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        let projected = chunk.project(&self.columns).ok_or_else(|| {
            VeinError::internal(format!(
                "projection references a column outside the chunk of {} column(s)",
                chunk.num_columns()
            ))
        })?;
        self.next.push(projected)
    }

    fn flush(&mut self) -> Result<()> {
        self.next.flush()
    }
}
