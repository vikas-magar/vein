//! Shared query spill resources and Arrow IPC run encoding.
//!
//! `SpillManager` owns run storage, cancellation, disk accounting, and cleanup.
//! Operators keep their typed run codecs and external algorithms local.

mod manager;
mod storage;

pub(crate) use manager::{PendingSpillRun, SpillManager, SpillRun, SpillWriteHandle};
pub use storage::{
    LocalSpillStorage, LocalSpillStorageFactory, SpillRunId, SpillStorage,
    SpillStorageFactory, SpillStream,
};

use std::io::{Seek, Write};
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use common::Result;
use error::VeinError;

use manager::SpillDiskLimitExceeded;

pub(crate) type SpillErrorResolver = Arc<dyn Fn() -> Option<VeinError> + Send + Sync>;

/// Output capability for the generic IPC writer, with optional query-specific
/// error translation for wrapped Arrow I/O errors.
pub(crate) trait IpcRunOutput: Write + Seek {
    fn spill_error_resolver(&self) -> Option<SpillErrorResolver> {
        None
    }
}

impl IpcRunOutput for SpillWriteHandle {
    fn spill_error_resolver(&self) -> Option<SpillErrorResolver> {
        Some(self.error_resolver())
    }
}

/// Incrementally writes batches to one Arrow IPC file.
pub(crate) struct IpcRunWriter<W: IpcRunOutput> {
    writer: FileWriter<W>,
    error_resolver: Option<SpillErrorResolver>,
}

impl<W: IpcRunOutput> IpcRunWriter<W> {
    /// Creates a writer for `schema` over the caller-owned output stream.
    pub(crate) fn new(output: W, schema: &Schema, operation: &'static str) -> Result<Self> {
        let error_resolver = output.spill_error_resolver();
        let writer = FileWriter::try_new(output, schema)
            .map_err(|error| resolve_spill_error(&error_resolver, operation, error))?;
        Ok(Self { writer, error_resolver })
    }

    /// Writes one already encoded batch to the run.
    pub(crate) fn write_batch(
        &mut self,
        batch: &RecordBatch,
        operation: &'static str,
    ) -> Result<()> {
        self.writer
            .write(batch)
            .map_err(|error| resolve_spill_error(&self.error_resolver, operation, error))
    }

    /// Writes the IPC footer and returns the finished output stream.
    pub(crate) fn finish(mut self, operation: &'static str) -> Result<W> {
        self.writer
            .finish()
            .map_err(|error| resolve_spill_error(&self.error_resolver, operation, error))?;
        self.writer
            .into_inner()
            .map_err(|error| resolve_spill_error(&self.error_resolver, operation, error))
    }
}

fn resolve_spill_error(
    resolver: &Option<SpillErrorResolver>,
    operation: &'static str,
    error: impl std::error::Error + 'static,
) -> VeinError {
    resolver
        .as_ref()
        .and_then(|resolve| resolve())
        .unwrap_or_else(|| spill_error(operation, error))
}

fn spill_error(operation: &'static str, error: impl std::error::Error + 'static) -> VeinError {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    while let Some(cause) = source {
        if let Some(limit) = cause.downcast_ref::<SpillDiskLimitExceeded>() {
            return VeinError::resource_exhausted(
                "query spill disk",
                usize::try_from(limit.requested).unwrap_or(usize::MAX),
                usize::try_from(limit.limit).unwrap_or(usize::MAX),
            );
        }
        source = cause.source();
    }
    VeinError::internal(format!("{operation}: {error}"))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    impl IpcRunOutput for Cursor<Vec<u8>> {}
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::reader::FileReader;
    use arrow::record_batch::RecordBatch;

    use super::IpcRunWriter;
    use super::IpcRunOutput;

    #[test]
    fn generic_ipc_run_writer_round_trips_multiple_batches() {
        let schema = Schema::new(vec![Field::new("key", DataType::Int64, false)]);
        let first = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
        )
            .expect("create first batch");
        let second = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int64Array::from(vec![3])) as ArrayRef],
        )
            .expect("create second batch");

        let mut writer = IpcRunWriter::new(Cursor::new(Vec::new()), &schema, "create test run")
            .expect("create generic IPC writer");
        writer
            .write_batch(&first, "write first test batch")
            .expect("write first batch");
        writer
            .write_batch(&second, "write second test batch")
            .expect("write second batch");
        let output = writer.finish("finish test run").expect("finish IPC run");

        let mut reader = FileReader::try_new(Cursor::new(output.into_inner()), None)
            .expect("open IPC run");
        assert_eq!(reader.next().expect("first record batch").unwrap(), first);
        assert_eq!(reader.next().expect("second record batch").unwrap(), second);
        assert!(reader.next().is_none());
    }
}
