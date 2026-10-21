use super::{artifact_path, external_statistics_path, table_manifest_path};

#[test]
fn catalog_artifact_paths_have_one_canonical_owner() {
    assert_eq!(
        table_manifest_path("events").unwrap().as_str(),
        ".vein/catalog/events.json"
    );
    let statistics_path = external_statistics_path("events");
    assert!(statistics_path.as_str().starts_with(".vein/statistics/"));
    assert!(statistics_path.as_str().ends_with(".json"));
    assert_ne!(
        statistics_path,
        external_statistics_path("other_events")
    );
    for name in ["", ".", "..", "nested/table", r"nested\table", "C:table"] {
        assert!(table_manifest_path(name).is_err(), "accepted table name: {name}");
    }
}

#[test]
fn accepts_nested_root_relative_artifacts() {
    let path = artifact_path("table/part-000.parquet").expect("safe manifest path");
    assert_eq!(path.as_str(), "table/part-000.parquet");
}

#[test]
fn rejects_absolute_and_traversing_artifacts() {
    for path in ["", "/outside.parquet", "../outside.parquet", "table/../outside"] {
        assert!(artifact_path(path).is_err(), "accepted unsafe path: {path}");
    }
}

#[test]
fn rejects_nonportable_path_separators_and_drive_prefixes() {
    for path in [r"table\part.parquet", r"\outside.parquet", "C:/outside.parquet"] {
        assert!(artifact_path(path).is_err(), "accepted nonportable path: {path}");
    }
}
