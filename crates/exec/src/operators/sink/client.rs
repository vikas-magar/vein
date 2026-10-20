//! Client sink that preserves the canonical reference-backed batch.
use arrow::datatypes::SchemaRef;
use common::Result;
use error::VeinError;
use std::time::Instant;

use crate::pipeline::{DataChunkView, PipelineNode, Sink};

/// Collects or streams validated views without materializing their columns.
pub struct CollectSink {
    output: Vec<DataChunkView>,
    output_sender: Option<flume::Sender<DataChunkView>>,
    rows_emitted: u64,
    schema: SchemaRef,
    chunks_in: u64,
    chunks_skipped: u64,
    batches_out: u64,
    schema_validation_us: u64,
}

impl CollectSink {
    /// Creates a collector using the immutable planned result schema.
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            output: Vec::new(),
            output_sender: None,
            rows_emitted: 0,
            schema,
            chunks_in: 0,
            chunks_skipped: 0,
            batches_out: 0,
            schema_validation_us: 0,
        }
    }

    /// Streams views through a bounded channel without retaining them.
    pub fn with_sender(schema: SchemaRef, output_sender: flume::Sender<DataChunkView>) -> Self {
        Self {
            output_sender: Some(output_sender),
            ..Self::new(schema)
        }
    }
}

impl PipelineNode for CollectSink {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.chunks_in = self.chunks_in.saturating_add(1);
        let started = Instant::now();
        let view = view.with_schema(self.schema.clone())?;
        self.schema_validation_us = self
            .schema_validation_us
            .saturating_add(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));

        if view.is_empty() {
            self.chunks_skipped = self.chunks_skipped.saturating_add(1);
            return Ok(());
        }

        let rows = u64::try_from(view.num_rows())
            .map_err(|_| VeinError::internal("client row count exceeds u64"))?;
        let rows_emitted = self
            .rows_emitted
            .checked_add(rows)
            .ok_or_else(|| VeinError::internal("client row count overflow"))?;
        if let Some(sender) = &self.output_sender {
            sender
                .send(view)
                .map_err(|error| VeinError::internal(format!("client result channel closed: {error}")))?;
        } else {
            self.output.push(view);
        }
        self.rows_emitted = rows_emitted;
        self.batches_out = self.batches_out.saturating_add(1);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        tracing::info!(
            target: "vein.audit",
            event = "client_sink_finished",
            result_format = "data_chunk_view",
            chunks_in = self.chunks_in,
            chunks_skipped = self.chunks_skipped,
            batches_out = self.batches_out,
            rows = self.rows_emitted,
            schema_validation_us = self.schema_validation_us,
            payload_materialization = false,
            "client_sink_finished"
        );
        Ok(())
    }
}

impl Sink for CollectSink {
    fn collect(&mut self) -> Vec<DataChunkView> {
        std::mem::take(&mut self.output)
    }

    fn row_count(&self) -> u64 {
        self.rows_emitted
    }
}

#[cfg(test)]
#[path = "client/tests.rs"]
mod tests;
