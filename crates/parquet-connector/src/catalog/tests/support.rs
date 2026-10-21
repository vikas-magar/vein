use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use storage::StorageLocation;
use storage::opendal::OpenDalReadStorageFactory;

use crate::catalog::ParquetCatalog;

pub(super) fn catalog(root: &Path) -> ParquetCatalog {
    let factory = OpenDalReadStorageFactory;
    ParquetCatalog::new(StorageLocation::local(root), &factory)
        .expect("open local Parquet test catalog")
}

pub(super) fn write_parquet(path: &Path, field: &str) {
    let schema = Arc::new(Schema::new(vec![Field::new(field, DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
        .expect("valid test batch");
    let mut writer = ArrowWriter::try_new(File::create(path).expect("create test file"), schema, None)
        .expect("create Parquet writer");
    writer.write(&batch).expect("write test batch");
    writer.close().expect("finish test file");
}

pub(super) fn write_parquet_with_row_groups(path: &Path, values: Vec<Option<i64>>) {
    const ROW_GROUP_ROWS: usize = 2;
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))])
        .expect("valid test batch");
    let mut writer = ArrowWriter::try_new(File::create(path).expect("create test file"), schema, None)
        .expect("create Parquet writer");
    for offset in (0..batch.num_rows()).step_by(ROW_GROUP_ROWS) {
        writer
            .write(&batch.slice(offset, ROW_GROUP_ROWS.min(batch.num_rows() - offset)))
            .expect("write test row group");
        writer.flush().expect("flush test row group");
    }
    writer.close().expect("finish test file");
}
