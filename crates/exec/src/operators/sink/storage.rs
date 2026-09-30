//! Streaming storage sink and transactional storage interfaces.

use std::sync::{Arc, Mutex};

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use common::Result;
use common::protocol::StorageTarget;
use error::VeinError;

use crate::pipeline::{DataChunk, PipelineNode, Sink};

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
    /// Start a staged write for a logical target.
    fn begin_write(&self, target: &StorageTarget) -> Result<Box<dyn WriteTransaction>>;
}

/// Sink that streams batches into a backend transaction without retaining them.
pub struct WriteSink {
    expected_schema: arrow::datatypes::SchemaRef,
    transaction: Option<Mutex<Box<dyn WriteTransaction>>>,
    rows_written: u64,
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
        if expected_schema.fields().len() != target.schema.len()
            || expected_schema.fields().iter().zip(&target.schema).any(|(field, planned)| {
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
            receipt: None,
        })
    }

    fn materialize(&self, chunk: &DataChunk) -> Result<Option<RecordBatch>> {
        let selected = chunk.selection.size();
        if selected == 0 {
            return Ok(None);
        }
        if chunk.vectors.len() != self.expected_schema.fields().len() {
            return Err(VeinError::internal(format!(
                "WRITE received {} columns, expected {}",
                chunk.vectors.len(), self.expected_schema.fields().len()
            )));
        }
        for (index, (array, field)) in chunk.vectors.iter().zip(self.expected_schema.fields()).enumerate() {
            if array.data_type() != field.data_type() {
                return Err(VeinError::internal(format!(
                    "WRITE column {index} ('{}') has type {:?}, expected {:?}",
                    field.name(), array.data_type(), field.data_type()
                )));
            }
            if !field.is_nullable() && array.null_count() != 0 {
                return Err(VeinError::internal(format!(
                    "WRITE column {index} ('{}') is non-nullable but contains {} nulls",
                    field.name(), array.null_count()
                )));
            }
        }
        chunk.materialize(self.expected_schema.clone()).map(Some)
    }
}

impl PipelineNode<DataChunk> for WriteSink {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        let Some(batch) = self.materialize(&chunk)? else {
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
        Ok(())
    }
}

impl Sink<DataChunk> for WriteSink {
    fn row_count(&self) -> u64 {
        self.rows_written
    }

    fn write_receipt(&self) -> Option<&WriteReceipt> {
        self.receipt.as_ref()
    }
}
