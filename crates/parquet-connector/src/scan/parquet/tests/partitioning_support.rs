use std::path::{Path, PathBuf};
use std::sync::Arc;

use catalog::{DataFileFormatId, MetadataKey, MetadataStore};
use common::identifiers::TaskId;
use common::protocol::{DataSource, FileScanTask};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::ParquetMetaData;
use storage::{
    StorageFileMetadata, StorageLocation, StorageObjectIdentity, StorageObjectPath,
    StorageObjectVersion, StorageTimestamp,
};
use types::VeinSchema;

use crate::PARQUET_FOOTER;
use crate::scan::ParquetScanProvider;

/// Writes a small fixture with independently addressable row groups.
pub(super) fn write_row_groups(path: &Path, count: usize) -> Arc<ParquetMetaData> {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
    ]));
    let file = std::fs::File::create(path).expect("create Parquet file");
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).expect("create writer");
    for group in 0..count {
        let first = (group * 2) as i64;
        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arrow::array::Int64Array::from(vec![first, first + 1]))],
        )
            .expect("valid row-group batch");
        writer.write(&batch).expect("write row group");
        writer.flush().expect("finish row group");
    }
    writer.close().expect("close Parquet writer");
    ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(path).expect("open Parquet footer"),
    )
        .expect("read Parquet footer")
        .metadata()
        .clone()
}

/// Captures the storage identity used by the shared metadata cache.
pub(super) fn storage_snapshot(path: &Path) -> StorageFileMetadata {
    let file = std::fs::metadata(path).expect("read test file metadata");
    let name = path.file_name().expect("test file has a name");
    StorageFileMetadata {
        identity: StorageObjectIdentity::new(
            StorageLocation::local(path.parent().expect("file parent").to_path_buf()),
            StorageObjectPath::new(name.to_string_lossy().into_owned()),
        ),
        size_bytes: file.len(),
        version: StorageObjectVersion {
            last_modified: file
                .modified()
                .ok()
                .and_then(StorageTimestamp::from_system_time),
            ..StorageObjectVersion::default()
        },
    }
}

/// Builds a file scan task with the supplied schema projection.
pub(super) fn file_scan(
    files: Vec<PathBuf>,
    snapshots: Vec<StorageFileMetadata>,
    projected: Arc<VeinSchema>,
) -> FileScanTask {
    FileScanTask {
        files: files.iter().map(|path| path.display().to_string()).collect(),
        storage_metadata: snapshots,
        data_file_format: DataFileFormatId::new("parquet"),
        format_options: Vec::new(),
        byte_range: None,
        scan_unit_range: None,
        projected,
        column_encoding_hints: Vec::new(),
        pushdown: None,
        task_id: TaskId::new(),
        catalog_snapshot: None,
        runtime_statistics: None,
    }
}

/// Stores parsed footer metadata under its versioned cache key.
pub(super) fn cache_footer(
    metadata: &MetadataStore,
    snapshot: &StorageFileMetadata,
    footer: Arc<ParquetMetaData>,
) {
    let key = MetadataKey::from_storage(PARQUET_FOOTER, snapshot).expect("versioned footer key");
    metadata.insert(key, footer);
}

/// Plans scan units with the connector's registered provider implementation.
pub(super) fn plan(
    scan: &FileScanTask,
    metadata: &MetadataStore,
    parallelism: usize,
    filters: Option<&exec::runtime::filter::RuntimeFilterSet>,
) -> common::Result<Vec<DataSource>> {
    use exec::operators::source::scan::{ScanPlanningRequest, ScanProvider};

    ParquetScanProvider::new().plan_work_units(ScanPlanningRequest {
        scan,
        metadata_store: metadata,
        max_parallelism: parallelism,
        runtime_filters: filters,
    })
}
