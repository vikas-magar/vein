use std::sync::Arc;

use catalog::DataFileFormatId;
use common::{identifiers::TaskId, protocol::FileScanTask};
use storage::{StorageFileMetadata, StorageLocation, StorageObjectIdentity, StorageObjectPath, StorageObjectVersion};

use super::{DefaultExecutorBuilder, paths};

fn scan(files: Vec<String>, storage_metadata: Vec<StorageFileMetadata>) -> FileScanTask {
    FileScanTask {
        files,
        storage_metadata,
        data_file_format: DataFileFormatId::new("test"),
        format_options: vec![],
        column_encoding_hints: vec![],
        byte_range: None,
        scan_unit_range: Some(1..2),
        projected: Arc::new(types::VeinSchema::empty()),
        pushdown: None,
        task_id: TaskId::new(),
        catalog_snapshot: None,
        runtime_statistics: None,
    }
}

fn metadata(root: &str, path: &str) -> StorageFileMetadata {
    StorageFileMetadata {
        identity: StorageObjectIdentity::new(StorageLocation::uri(root), StorageObjectPath::new(path)),
        size_bytes: 4,
        version: StorageObjectVersion::default(),
    }
}

#[test]
fn non_local_identity_never_enters_filesystem_resolution() {
    let task = scan(vec!["custom://namespace/table/file".into()], vec![metadata("custom://namespace", "table/file")]);
    let (location, files) = paths::resolve(&task).unwrap();
    assert_eq!(location, StorageLocation::uri("custom://namespace"));
    assert_eq!(files[0].relative, "table/file");
    assert!(files[0].absolute.as_os_str().is_empty());
    assert_eq!(files[0].scan_unit_range, Some(1..2));
    assert_eq!(files[0].storage_metadata.as_ref(), task.storage_metadata.first());
}

#[test]
fn missing_and_mixed_non_local_identities_are_rejected() {
    let missing = scan(vec!["custom://namespace/file".into()], vec![]);
    assert!(paths::resolve(&missing).unwrap_err().to_string().contains("storage identities"));
    let incomplete = scan(vec!["a".into(), "b".into()], vec![metadata("custom://one", "a")]);
    assert!(paths::resolve(&incomplete).is_err());
    let mixed = scan(vec!["a".into(), "b".into()], vec![metadata("custom://one", "a"), metadata("custom://two", "b")]);
    assert!(paths::resolve(&mixed).is_err());
}

#[test]
fn native_files_keep_canonical_paths_and_common_root() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("nested")).unwrap();
    let first = directory.path().join("a");
    let second = directory.path().join("nested/b");
    std::fs::write(&first, b"a").unwrap();
    std::fs::write(&second, b"b").unwrap();
    let task = scan(vec![first.to_string_lossy().into(), second.to_string_lossy().into()], vec![]);
    let (location, files) = paths::resolve(&task).unwrap();
    assert_eq!(location, StorageLocation::local(directory.path().canonicalize().unwrap()));
    assert_eq!(files[0].absolute, first.canonicalize().unwrap());
    assert_eq!(files[1].relative, "nested/b");
}

#[tokio::test]
async fn storage_handles_are_cached_by_full_namespace_identity() {
    let location = StorageLocation::uri("custom://namespace");
    let factory = storage::memory::MemoryReadStorageFactory::new(location.clone(), [
        (StorageObjectPath::new("table/file"), bytes::Bytes::from_static(b"data")),
    ]).unwrap();
    let builder = DefaultExecutorBuilder::new(Arc::new(functions::FunctionRegistry::with_builtins()))
        .with_read_storage_factory(Arc::new(factory));
    let first = builder.storage_for(&location).unwrap();
    let second = builder.storage_for(&location).unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(first.read_range(&StorageObjectPath::new("table/file"), 1..3).await.unwrap().as_ref(), b"at");
}
