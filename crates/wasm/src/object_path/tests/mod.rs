use storage::StorageObjectPath;

use super::valid_object_path;

#[test]
fn accepts_nested_root_relative_table_paths() {
    assert!(valid_object_path(&StorageObjectPath::new("lineitem/part-0.parquet")));
}

#[test]
fn rejects_paths_that_escape_or_do_not_name_an_object() {
    for value in ["", "/lineitem/a.parquet", "../a.parquet", "lineitem/../a.parquet", "lineitem//a.parquet"] {
        assert!(!valid_object_path(&StorageObjectPath::new(value)));
    }
}
