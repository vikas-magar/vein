//! Client collect sink materializing surviving rows into Arrow record batches.

use crate::pipeline::{DataChunk, PipelineNode, Sink};
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use common::Result;
use error::VeinError;
use std::sync::Arc;
use std::time::Instant;

/// A [`Sink`] that captures the pipeline's output and returns it via
/// [`Sink::collect`].
///
/// Filters narrow a chunk's [`SelectionVector`](common::arrow::SelectionVector)
/// rather than compacting the underlying arrays. This sink returns ordinary
/// `RecordBatch` values: contiguous selections are represented by zero-copy
/// Arrow slices, while sparse selections are gathered with `arrow::compute::take`.
pub struct CollectSink {
    output: Vec<RecordBatch>,
    field_names: Vec<String>,
    output_sender: Option<flume::Sender<RecordBatch>>,
    rows_emitted: u64,
    schema: Option<SchemaRef>,
    chunks_in: u64,
    chunks_skipped: u64,
    batches_out: u64,
    schema_build_us: u64,
    materialize_us: u64,
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
            chunks_in: 0,
            chunks_skipped: 0,
            batches_out: 0,
            schema_build_us: 0,
            materialize_us: 0,
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
            chunks_in: 0,
            chunks_skipped: 0,
            batches_out: 0,
            schema_build_us: 0,
            materialize_us: 0,
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
            chunks_in: 0,
            chunks_skipped: 0,
            batches_out: 0,
            schema_build_us: 0,
            materialize_us: 0,
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
        self.chunks_in += 1;
        if chunk.selection.is_empty() {
            self.chunks_skipped += 1;
            return Ok(());
        }

        let schema_started = Instant::now();
        let schema = self.schema_for(&chunk);
        self.schema_build_us += schema_started.elapsed().as_micros() as u64;

        let materialize_started = Instant::now();
        let batch = chunk.materialize(schema)?;
        self.materialize_us += materialize_started.elapsed().as_micros() as u64;

        self.rows_emitted += batch.num_rows() as u64;
        self.batches_out += 1;
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
        tracing::info!(
            target: "vein.audit",
            event = "client_sink_finished",
            chunks_in = self.chunks_in,
            chunks_skipped = self.chunks_skipped,
            batches_out = self.batches_out,
            rows = self.rows_emitted,
            schema_build_us = self.schema_build_us,
            materialize_us = self.materialize_us,
            "client_sink_finished"
        );
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

#[cfg(test)]
mod tests {
    use super::CollectSink;
    use crate::pipeline::{DataChunk, PipelineNode, Sink};
    use common::arrow::SelectionVector;

    #[test]
    fn client_sink_preserves_zero_column_rows() {
        let mut sink = CollectSink::new();
        sink.push(DataChunk::new(
            Vec::new(),
            SelectionVector::new_dense(2),
        ))
            .expect("push zero-column chunk");

        let batches = sink.collect();

        assert_eq!(sink.row_count(), 2);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_columns(), 0);
        assert_eq!(batches[0].num_rows(), 2);
    }
}
