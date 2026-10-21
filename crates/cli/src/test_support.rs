use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use common::protocol::{KeyValue, StorageTarget};
use exec::operators::sink::WriteStorage;
use exec::pipeline::DataChunkView;
use parquet_connector::catalog::ParquetCatalog;
use parquet_connector::writer::ParquetWriteStorage;
use plan::VeinSchema;
use storage::StorageLocation;
use storage::opendal::OpenDalReadStorageFactory;

pub(crate) fn parquet_catalog(root: impl Into<PathBuf>) -> ParquetCatalog {
    let factory = OpenDalReadStorageFactory;
    ParquetCatalog::new(StorageLocation::local(root), &factory)
        .expect("open local Parquet test catalog")
}

pub(crate) fn write_parquet_table(
    root: impl Into<PathBuf>,
    table: &str,
    mode: &str,
    schema: Arc<Schema>,
    batch: &RecordBatch,
) {
    let mut schema_ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut schema_ipc, schema.as_ref())
            .expect("create test Arrow schema stream");
        writer.finish().expect("finish test Arrow schema stream");
    }
    let logical_schema = Arc::new(VeinSchema::try_from(schema.as_ref()).expect("valid test schema"));
    let target = StorageTarget::new(
        table,
        vec![
            KeyValue::new("format", "parquet"),
            KeyValue::new("mode", mode),
        ],
        logical_schema,
    )
    .with_schema_ipc(schema_ipc);
    let storage = ParquetWriteStorage::new(root);
    let mut transaction = storage
        .begin_write(&target)
        .expect("begin test Parquet write");
    let view = DataChunkView::from_dense_arrays(batch.columns().to_vec())
        .expect("create reference-backed test view")
        .with_schema(schema)
        .expect("attach test schema");
    transaction
        .write_view(&view)
        .expect("write test view through connector");
    transaction.commit().expect("commit test table");
}
