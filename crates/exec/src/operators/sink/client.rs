//! Client collect sink materializing surviving rows into Arrow record batches.

use crate::pipeline::{DataChunk, PipelineNode, Sink};
use arrow::datatypes::{Field, Schema, SchemaRef};
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
    schema: Option<SchemaRef>,
}

impl CollectSink {
    /// Create a new [`CollectSink`].
    pub fn new() -> Self {
        Self {
            output: Vec::new(),
            field_names: Vec::new(),
            output_sender: None,
            rows_emitted: 0,
            schema: None,
        }
    }

    /// Create a new [`CollectSink`] with predefined field names.
    pub fn with_field_names(field_names: Vec<String>) -> Self {
        Self {
            output: Vec::new(),
            field_names,
            output_sender: None,
            rows_emitted: 0,
            schema: None,
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
            schema: None,
        }
    }

    /// Returns the output schema, building it once and reusing it for later batches.
    ///
    /// The pipeline's output schema is fixed for the life of the query; the
    /// fields are only rebuilt if a batch ever reports a different shape.
    fn schema_for(&mut self, chunk: &DataChunk) -> SchemaRef {
        if let Some(schema) = &self.schema {
            let same_shape = schema.fields().len() == chunk.vectors.len()
                && schema
                    .fields()
                    .iter()
                    .zip(&chunk.vectors)
                    .all(|(field, vector)| field.data_type() == vector.data_type());
            if same_shape {
                return schema.clone();
            }
        }

        let fields: Vec<Field> = chunk
            .vectors
            .iter()
            .enumerate()
            .map(|(i, vector)| {
                let name = self
                    .field_names
                    .get(i)
                    .cloned()
                    .unwrap_or_else(|| format!("col_{i}"));
                Field::new(name, vector.data_type().clone(), true)
            })
            .collect();
        let schema: SchemaRef = Arc::new(Schema::new(fields));
        self.schema = Some(schema.clone());
        schema
    }
}

impl Default for CollectSink {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineNode<DataChunk> for CollectSink {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        if chunk.selection.is_empty() {
            return Ok(());
        }

        let schema = self.schema_for(&chunk);
        let batch = chunk.materialize(schema)?;
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
