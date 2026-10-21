use super::support::{catalog as open_catalog, chunk, storage, target};
use ::catalog::CatalogProvider;
use exec::operators::sink::WriteStorage;

#[tokio::test]
async fn create_append_and_overwrite_publish_consistent_manifests_and_statistics() {
    let root = tempfile::tempdir().expect("temporary data root");
    let storage = storage(root.path());
    let mut create = storage.begin_write(&target(None)).expect("begin create");
    create.write_view(&chunk(vec![1, 2])).expect("write create chunk");
    assert!(storage.read_manifest("events").expect("read catalog").is_none());
    let receipt = create.commit().expect("commit create");
    assert_eq!(receipt.row_count, 2);
    let created = storage.read_manifest("events").expect("read manifest").expect("created manifest");
    assert_eq!(created.revision, 1);
    assert_eq!(created.files.len(), 1);
    let created_stats = created.statistics.as_ref().expect("create statistics");
    assert_eq!(created_stats.source_revision, created.revision);
    assert_eq!(created_stats.statistics.num_rows, Some(2));
    assert_eq!(created_stats.statistics.column_statistics["id"].distinct_count, Some(2));
    assert_eq!(created_stats.statistics.column_statistics["id"].min_value, Some(plan::LogicalLiteral::Int64(1)));
    assert_eq!(created_stats.statistics.column_statistics["id"].max_value, Some(plan::LogicalLiteral::Int64(2)));

    let catalog = open_catalog(root.path());
    let created_table = catalog.get_table("events").await.expect("read created table");
    assert_eq!(created_table.files.len(), 1);
    assert_eq!(created_table.statistics.as_ref().unwrap().column_statistics["id"].distinct_count, Some(2));

    assert!(storage.begin_write(&target(None)).is_err());
    let mut append = storage.begin_write(&target(Some("append"))).expect("begin append");
    append.write_view(&chunk(vec![3])).expect("write append chunk");
    append.commit().expect("commit append");
    let appended = storage.read_manifest("events").expect("read append").expect("appended manifest");
    assert_eq!(appended.revision, 2);
    assert_eq!(appended.files.len(), 2);
    let appended_stats = appended.statistics.as_ref().expect("append statistics");
    assert_eq!(appended_stats.statistics.num_rows, Some(3));
    assert_eq!(appended_stats.statistics.column_statistics["id"].distinct_count, Some(3));
    assert_eq!(appended_stats.statistics.column_statistics["id"].min_value, Some(plan::LogicalLiteral::Int64(1)));
    assert_eq!(appended_stats.statistics.column_statistics["id"].max_value, Some(plan::LogicalLiteral::Int64(3)));
    assert_eq!(appended_stats.statistics.column_statistics["id"].numeric.as_ref().unwrap().mean, Some(2.0));
    assert_eq!(appended_stats.statistics.column_statistics["id"].numeric.as_ref().unwrap().m2, Some(2.0));
    assert!(appended_stats.source_identities.windows(2).all(|pair| pair[0].identifier <= pair[1].identifier));

    let mut overwrite = storage.begin_write(&target(Some("overwrite"))).expect("begin overwrite");
    overwrite.write_view(&chunk(vec![9])).expect("write overwrite chunk");
    overwrite.commit().expect("commit overwrite");
    let replaced = storage.read_manifest("events").expect("read overwrite").expect("overwritten manifest");
    assert_eq!(replaced.revision, 3);
    assert_eq!(replaced.files.len(), 1);
    let stats = replaced.statistics.as_ref().expect("overwrite statistics");
    assert_eq!(stats.source_revision, replaced.revision);
    assert_eq!(stats.statistics.num_rows, Some(1));
    assert_eq!(stats.statistics.column_statistics["id"].distinct_count, Some(1));
    assert_eq!(stats.statistics.column_statistics["id"].min_value, Some(plan::LogicalLiteral::Int64(9)));
    assert_eq!(stats.statistics.column_statistics["id"].numeric.as_ref().unwrap().mean, Some(9.0));
}

#[tokio::test]
async fn stale_source_statistics_are_discarded_when_appending() {
    let root = tempfile::tempdir().expect("temporary data root");
    let storage = storage(root.path());
    let mut create = storage.begin_write(&target(None)).expect("begin create");
    create.write_view(&chunk(vec![1, 2])).expect("write create chunk");
    create.commit().expect("commit create");

    let path = storage.manifest_path("events").expect("manifest path");
    let mut manifest = storage.read_manifest("events").expect("read manifest").expect("created table");
    manifest.statistics.as_mut().expect("analysis").source_identities[0].version.push_str("-stale");
    std::fs::write(path, serde_json::to_vec(&manifest).unwrap()).expect("persist stale identity");

    let mut append = storage.begin_write(&target(Some("append"))).expect("begin append");
    append.write_view(&chunk(vec![3])).expect("write append chunk");
    append.commit().expect("commit append");
    let manifest = storage.read_manifest("events").expect("read append").expect("appended table");
    assert!(manifest.statistics.is_none(), "stale whole-table statistics must not be published");
    let table = open_catalog(root.path()).get_table("events").await.expect("read table");
    let stats = table.statistics.as_ref().expect("footer row count remains available");
    assert_eq!(stats.num_rows, Some(3));
    assert_eq!(stats.column_statistics["id"].distinct_count, None);
}
