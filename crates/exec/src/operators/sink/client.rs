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
    output_sender: Option<flume::Sender<RecordBatch>>,
    rows_emitted: u64,
}

impl CollectSink {
    /// Create a new [`CollectSink`].
    pub fn new() -> Self {
        Self {
            output: Vec::new(),
            field_names: Vec::new(),
            output_sender: None,
            rows_emitted: 0,
        }
    }

    /// Create a new [`CollectSink`] with predefined field names.
    pub fn with_field_names(field_names: Vec<String>) -> Self {
        Self {
            output: Vec::new(),
            field_names,
            output_sender: None,
            rows_emitted: 0,
        }
    }

    /// Create a client sink that forwards each materialized batch through a bounded channel.
    pub fn with_field_names_and_sender(
        field_names: Vec<String>,
        output_sender: flume::Sender<RecordBatch>,
    ) -> Self {
        Self {
            output: Vec::new(),
            field_names,
            output_sender: Some(output_sender),
            rows_emitted: 0,
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

        let contiguous = chunk.selection.contiguous_range();
        let indices = contiguous.is_none().then(|| {
            let mut indices = UInt32Array::builder(selected);
            for i in 0..selected {
                indices.append_value(chunk.selection.get_index(i) as u32);
            }
            indices.finish()
        });

        let mut fields = Vec::with_capacity(chunk.vectors.len());
        let mut columns = Vec::with_capacity(chunk.vectors.len());
        for (i, vec) in chunk.vectors.iter().enumerate() {
            let col_name = self
                .field_names
                .get(i)
                .cloned()
                .unwrap_or_else(|| format!("col_{i}"));
            fields.push(Field::new(col_name, vec.data_type().clone(), true));
            let array = match (&contiguous, &indices) {
                (Some(range), _) => vec.slice(range.start, range.len()),
                (None, Some(indices)) => take(vec.as_ref(), indices, None)
                    .map_err(|e| VeinError::internal(format!("client sink take failed: {e}")))?,
                (None, None) => {
                    return Err(VeinError::internal(
                        "client sink selection indices were not materialized",
                    ));
                }
            };
            columns.push(array);
        }

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| VeinError::internal(format!("client sink record batch failed: {e}")))?;
        self.rows_emitted += batch.num_rows() as u64;
        if let Some(sender) = &self.output_sender {
            sender
                .send(batch)
                .map_err(|e| VeinError::internal(format!("client result channel closed: {e}")))?;
        } else {
            self.output.push(batch);
        }
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

    fn row_count(&self) -> u64 {
        self.rows_emitted
    }
}
