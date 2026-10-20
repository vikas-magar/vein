//! Streaming storage sink and transactional storage interfaces.

use std::sync::{Arc, Mutex};

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use common::Result;
use common::protocol::StorageTarget;
use error::VeinError;
use std::time::Instant;
use tracing::info;

use crate::pipeline::{DataChunkView, MaterializationReport, PipelineNode, Sink};

/// Summary returned only after a storage transaction is committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteReceipt {
    /// Logical destination written by the query.
    pub destination: String,
    /// Number of rows durably committed.
    pub row_count: u64,
}

/// One staged, incremental table write.
pub trait WriteTransaction: Send {
    /// Append one selected Arrow batch to the staged write.
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()>;
    /// Atomically publish the staged write and return its committed receipt.
    fn commit(self: Box<Self>) -> Result<WriteReceipt>;
}

/// Storage backend used by `WRITE` sinks.
pub trait WriteStorage: Send + Sync {
    /// Validate a logical target and its user-supplied options without opening a transaction.
    ///
    /// Query preparation calls this before physical planning so backend-specific
    /// configuration errors are reported before a task can be created.
    fn validate_target(&self, destination_uri: &str, options: &[(String, String)]) -> Result<()>;
    /// Start a staged write for a logical target.
    fn begin_write(&self, target: &StorageTarget) -> Result<Box<dyn WriteTransaction>>;
}

/// Sink that streams batches into a backend transaction without retaining them.
pub struct WriteSink {
    expected_schema: arrow::datatypes::SchemaRef,
    transaction: Option<Mutex<Box<dyn WriteTransaction>>>,
    rows_written: u64,
    batches_written: u64,
    conversion_us: u64,
    materialization: MaterializationReport,
    receipt: Option<WriteReceipt>,
}

impl WriteSink {
    /// Construct the sink and begin its storage transaction.
    pub fn new(target: StorageTarget, storage: Arc<dyn WriteStorage>) -> Result<Self> {
        if target.schema_ipc.is_empty() {
            return Err(VeinError::planning("WRITE target is missing its Arrow schema", None));
        }
        let schema_reader = StreamReader::try_new(std::io::Cursor::new(target.schema_ipc.as_slice()), None)
            .map_err(|error| VeinError::planning(format!("invalid WRITE Arrow schema: {error}"), None))?;
        let expected_schema = schema_reader.schema();
        if expected_schema.fields().len() != target.schema.fields.len()
            || expected_schema.fields().iter().zip(&target.schema.fields).any(|(field, planned)| {
            field.name() != &planned.name
        })
        {
            return Err(VeinError::planning("WRITE schema metadata does not match its Arrow schema", None));
        }
        let transaction = storage.begin_write(&target)?;
        Ok(Self {
            expected_schema,
            transaction: Some(Mutex::new(transaction)),
            rows_written: 0,
            batches_written: 0,
            conversion_us: 0,
            materialization: MaterializationReport::default(),
            receipt: None,
        })
    }

    fn materialize(&self, view: &DataChunkView) -> Result<Option<(RecordBatch, MaterializationReport)>> {
        let selected = view.num_rows();
        if selected == 0 {
            return Ok(None);
        }
        if view.num_columns() != self.expected_schema.fields().len() {
            return Err(VeinError::internal(format!(
                "WRITE received {} columns, expected {}",
                view.num_columns(), self.expected_schema.fields().len()
            )));
        }
        view.validate_schema(&self.expected_schema)?;
        view.to_record_batch_with_report(self.expected_schema.clone())
            .map(Some)
    }
}

impl PipelineNode<DataChunkView> for WriteSink {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        let started = Instant::now();
        let Some((batch, report)) = self.materialize(&view)? else {
            return Ok(());
        };
        let rows_written = self.rows_written.checked_add(batch.num_rows() as u64)
            .ok_or_else(|| VeinError::internal("storage row count overflow"))?;
        self.transaction.as_ref()
            .ok_or_else(|| VeinError::internal("storage transaction already completed"))?
            .lock()
            .map_err(|_| VeinError::internal("storage transaction mutex poisoned"))?
            .write_batch(&batch)?;
        self.rows_written = rows_written;
        self.batches_written = self.batches_written.saturating_add(1);
        self.conversion_us = self
            .conversion_us
            .saturating_add(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));
        self.materialization.accumulate(report);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.receipt.is_none() {
            let transaction = self.transaction.take()
                .ok_or_else(|| VeinError::internal("storage transaction already completed"))?;
            let transaction = transaction.into_inner()
                .map_err(|_| VeinError::internal("storage transaction mutex poisoned"))?;
            self.receipt = Some(transaction.commit()?);
        }
        info!(
            target: "vein.audit",
            event = "storage_write_finished",
            rows = self.rows_written,
            batches = self.batches_written,
            shared_columns = self.materialization.shared_columns,
            gathered_columns = self.materialization.gathered_columns,
            empty_columns = self.materialization.empty_columns,
            gathered_output_buffer_bytes = self.materialization.gathered_output_buffer_bytes,
            conversion_us = self.conversion_us,
            "storage_write_finished"
        );
        Ok(())
    }
}

impl Sink<DataChunkView> for WriteSink {
    fn row_count(&self) -> u64 {
        self.rows_written
    }

    fn write_receipt(&self) -> Option<&WriteReceipt> {
        self.receipt.as_ref()
    }
}

#[cfg(test)]
#[path = "storage/tests.rs"]
mod tests;
