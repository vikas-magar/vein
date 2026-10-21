use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use catalog::MetadataStore;
use parquet::arrow::ArrowWriter;
use storage::{
    StorageFileMetadata, StorageLocation, StorageObjectIdentity, StorageObjectPath,
    StorageObjectVersion,
};

use super::ParquetMetadataService;

fn write_footer(path: &Path) {
    let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))])
        .expect("test batch is valid");
    let file = File::create(path).expect("create Parquet fixture");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("create Parquet writer");
    writer.write(&batch).expect("write Parquet fixture");
    writer.close().expect("finish Parquet fixture");
}

fn versioned_source(size_bytes: u64) -> StorageFileMetadata {
    StorageFileMetadata {
        identity: StorageObjectIdentity::new(
            StorageLocation::local("dataset"),
            StorageObjectPath::new("part.parquet"),
        ),
        size_bytes,
        version: StorageObjectVersion {
            provider_version: Some(Arc::from("revision-1")),
            ..StorageObjectVersion::default()
        },
    }
}

#[test]
fn catalog_and_scan_service_handles_share_one_typed_footer_cache() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("source.parquet");
    write_footer(&path);
    let size = std::fs::metadata(&path).expect("stat Parquet fixture").len();
    let source = versioned_source(size);
    let store = MetadataStore::new();
    let catalog_service = ParquetMetadataService::new(store.clone());
    let scan_service = ParquetMetadataService::new(store);
    let source_label = path.display().to_string();
    let (catalog_footer, cache_hit) = catalog_service
        .load_footer_from_seekable(
            &source,
            File::open(&path).expect("open Parquet fixture"),
            &source_label,
        )
        .expect("catalog footer loads");
    let scan_footer = scan_service
        .cached_footer(&source)
        .expect("scan service sees catalog footer");
    let schema = catalog_service
        .arrow_schema(&catalog_footer, &source_label)
        .expect("Arrow schema derives from cached footer");

    assert!(!cache_hit);
    assert_eq!(schema.field(0).name(), "key");
    assert!(Arc::ptr_eq(&catalog_footer, &scan_footer));
}
