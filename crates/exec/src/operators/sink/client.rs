//! Client collect sink materializing surviving rows into Arrow record batches.

use crate::pipeline::{DataChunk, PipelineNode, Sink};
use arrow::array::UInt32Array;
use arrow::compute::take;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use common::Result;
use error::VeinError;
use std::sync::Arc;

/// A [`Sink`] that captures the pipeline's output and returns it via
/// [`Sink::collect`].
///
/// Filters narrow a chunk's [`SelectionVector`](common::arrow::SelectionVector)
/// rather than compacting the underlying arrays, so this sink materializes the
/// selected rows via `arrow::compute::take` so the captured batches contain
/// exactly the surviving rows.
pub struct CollectSink {
    output: Vec<RecordBatch>,
    field_names: Vec<String>,
}

impl CollectSink {
    /// Create a new [`CollectSink`].
    pub fn new() -> Self {
        Self {
            output: Vec::new(),
            field_names: Vec::new(),
        }
    }

    /// Create a new [`CollectSink`] with predefined field names.
    pub fn with_field_names(field_names: Vec<String>) -> Self {
        Self {
            output: Vec::new(),
            field_names,
        }
    }
}

impl Default for CollectSink {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineNode<DataChunk> for CollectSink {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        let selected = chunk.selection.size();
        if selected == 0 {
            return Ok(());
        }

        // Build the index array selecting the surviving rows.
        let mut idx = UInt32Array::builder(selected);
        for i in 0..selected {
            idx.append_value(chunk.selection.get_index(i) as u32);
        }
        let indices = idx.finish();

        let mut fields = Vec::with_capacity(chunk.vectors.len());
        let mut columns = Vec::with_capacity(chunk.vectors.len());
        for (i, vec) in chunk.vectors.iter().enumerate() {
            let col_name = self
                .field_names
                .get(i)
                .cloned()
                .unwrap_or_else(|| format!("col_{i}"));
            fields.push(Field::new(
                col_name,
                vec.data_type().clone(),
                true,
            ));
            let array = take(vec.as_ref(), &indices, None)
                .map_err(|e| VeinError::internal(format!("client sink take failed: {e}")))?;
            columns.push(array);
        }

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| VeinError::internal(format!("client sink record batch failed: {e}")))?;
        self.output.push(batch);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Sink<DataChunk> for CollectSink {
    fn collect(&mut self) -> Vec<RecordBatch> {
        std::mem::take(&mut self.output)
    }
}
