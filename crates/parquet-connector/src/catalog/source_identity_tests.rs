use storage::{StorageObjectVersion, StorageTimestamp};

use super::statistics_source_identity;

#[test]
fn source_identity_changes_when_any_storage_version_fact_changes() {
    let first = StorageObjectVersion {
        etag: Some("etag-1".into()),
        last_modified: Some(StorageTimestamp::from_system_time(
            std::time::UNIX_EPOCH + std::time::Duration::from_secs(5),
        ).unwrap()),
        ..StorageObjectVersion::default()
    };
    let changed = StorageObjectVersion {
        etag: Some("etag-2".into()),
        ..first.clone()
    };
    let changed_provider_version = StorageObjectVersion {
        provider_version: Some("generation-2".into()),
        ..first.clone()
    };
    let changed_timestamp = StorageObjectVersion {
        last_modified: Some(StorageTimestamp::from_system_time(
            std::time::UNIX_EPOCH + std::time::Duration::from_secs(6),
        ).unwrap()),
        ..first.clone()
    };

    let first_identity = statistics_source_identity("part.parquet", 128, &first);
    let changed_identity = statistics_source_identity("part.parquet", 128, &changed);
    let provider_identity =
        statistics_source_identity("part.parquet", 128, &changed_provider_version);
    let timestamp_identity = statistics_source_identity("part.parquet", 128, &changed_timestamp);
    let resized_identity = statistics_source_identity("part.parquet", 129, &first);

    assert_eq!(first_identity.identifier, "part.parquet");
    assert_ne!(first_identity.version, changed_identity.version);
    assert_ne!(first_identity.version, provider_identity.version);
    assert_ne!(first_identity.version, timestamp_identity.version);
    assert_ne!(first_identity.version, resized_identity.version);
}
