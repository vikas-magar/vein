use std::path::Path;
use std::sync::Arc;

use crate::writer::ParquetWriteStorage;
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use common::protocol::{KeyValue, StorageTarget};
use exec::pipeline::DataChunkView;
use storage::StorageLocation;
use storage::opendal::OpenDalReadStorageFactory;

pub(super) fn target(mode: Option<&str>) -> StorageTarget {
    let mut options = vec![KeyValue::new("format", "parquet")];
    if let Some(mode) = mode { options.push(KeyValue::new("mode", mode)); }
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut schema_ipc = Vec::new();
    let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut schema_ipc, &schema)
        .expect("create schema IPC stream");
    writer.finish().expect("finish schema IPC stream");
    StorageTarget::new(
        "events", options,
        Arc::new(plan::VeinSchema::new(vec![Arc::new(plan::ResolvedColumn::new(
            plan::ColumnId(0), "id", None, plan::VeinDataType::Int64,
        ))])),
    ).with_schema_ipc(schema_ipc)
}

pub(super) fn chunk(values: Vec<i64>) -> DataChunkView {
    DataChunkView::from_dense_arrays(vec![Arc::new(Int64Array::from(values))])
        .expect("valid test chunk")
        .with_schema(Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])))
        .expect("valid test schema")
}

pub(super) fn storage(root: &Path) -> ParquetWriteStorage {
    ParquetWriteStorage::new(root)
}

pub(super) fn catalog(root: &Path) -> crate::catalog::ParquetCatalog {
    let factory = OpenDalReadStorageFactory;
    crate::catalog::ParquetCatalog::new(StorageLocation::local(root), &factory)
        .expect("open Parquet test catalog")
}
