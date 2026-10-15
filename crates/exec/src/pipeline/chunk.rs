//! In-flight columnar data chunk representation.

use arrow::array::{ArrayRef, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use common::arrow::SelectionVector;
use common::Result;
use error::VeinError;

/// A materialized Arrow batch used by operators that need ordinary arrays.
///
/// It stores Arrow columns plus a [`SelectionVector`] selecting the active
/// rows. Passing columns through or changing only the selection can retain
/// their Arrow buffers by reference. Pipelines that preserve arbitrary row
/// references across source batches use [`super::DataChunkView`].
/// Operators that compute values or gather rows may allocate new arrays;
/// [`DataChunk::materialize`] slices contiguous selections and uses `take` for
/// sparse selections.
#[derive(Debug, Clone)]
pub struct DataChunk {
    /// The columns in this chunk, each an Arc'd Arrow array.
    pub vectors: Vec<ArrayRef>,
    /// The selection vector indicating active rows.
    pub selection: SelectionVector,
}

impl DataChunk {
    /// Creates a new [`DataChunk`] from column arrays and an explicit selection vector.
    pub fn new(vectors: Vec<ArrayRef>, selection: SelectionVector) -> Self {
        Self {
            vectors,
            selection,
        }
    }

    /// Creates a [`DataChunk`] from column arrays with an all-valid (dense) selection vector.
    pub fn from_vectors(vectors: Vec<ArrayRef>) -> Self {
        let num_rows = vectors.first().map(|v| v.len()).unwrap_or(0);
        Self {
            vectors,
            selection: SelectionVector::new_dense(num_rows),
        }
    }

    /// Creates a [`DataChunk`] from an Arrow [`RecordBatch`] with an initially dense selection.
    ///
    /// Takes ownership of the batch's columns rather than copying them: the
    /// [`RecordBatch`] is decomposed with [`RecordBatch::into_parts`], so the
    /// vector of column references moves into the chunk for free.
    pub fn from_record_batch(batch: RecordBatch) -> Self {
        let (_, vectors, num_rows) = batch.into_parts();
        Self {
            vectors,
            selection: SelectionVector::new_dense(num_rows),
        }
    }

    /// Materializes the selected rows into an Arrow [`RecordBatch`] under `schema`.
    ///
    /// The selection vector is honoured: a contiguous physical range is
    /// `slice`d zero-copy, and only a sparse selection falls back to
    /// [`take`]. `schema` is supplied by the caller so it can be built once per
    /// query rather than once per batch.
    pub fn materialize(&self, schema: SchemaRef) -> Result<RecordBatch> {
        let selected = self.selection.size();
        let range = self.selection.contiguous_range();
        let indices = range.is_none().then(|| {
            let mut indices = UInt32Array::builder(selected);
            for row in 0..selected {
                indices.append_value(self.selection.get_index(row) as u32);
            }
            indices.finish()
        });

        let mut columns = Vec::with_capacity(self.vectors.len());
        for array in &self.vectors {
            let materialized = match (&range, &indices) {
                (Some(range), _) => array.slice(range.start, range.len()),
                (None, Some(indices)) => take(array.as_ref(), indices, None).map_err(|error| {
                    VeinError::internal(format!("materialize selection failed: {error}"))
                })?,
                (None, None) => {
                    return Err(VeinError::internal(
                        "materialize requires indices for a sparse selection",
                    ));
                }
            };
            columns.push(materialized);
        }

        let options = RecordBatchOptions::new().with_row_count(Some(selected));
        RecordBatch::try_new_with_options(schema, columns, &options)
            .map_err(|error| VeinError::internal(format!("materialize record batch failed: {error}")))
    }

    /// Total surviving rows in this chunk.
    pub fn num_rows(&self) -> usize {
        self.selection.size()
    }

    /// The physical length of the underlying column vectors in this chunk.
    pub fn physical_len(&self) -> usize {
        self.vectors.first().map(|v| v.len()).unwrap_or(0)
    }

    /// Number of column vectors in this chunk.
    pub fn num_columns(&self) -> usize {
        self.vectors.len()
    }

    /// Whether this chunk has zero surviving rows.
    pub fn is_empty(&self) -> bool {
        self.selection.is_empty()
    }

    /// Borrow the column array at `index`.
    pub fn column(&self, index: usize) -> &ArrayRef {
        &self.vectors[index]
    }
}

#[cfg(test)]
mod tests;
