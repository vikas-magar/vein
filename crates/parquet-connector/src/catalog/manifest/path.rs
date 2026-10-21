use error::{Result, VeinError};
use storage::StorageObjectPath;

/// Returns the root-relative path of a managed table manifest.
pub fn table_manifest_path(table: &str) -> Result<StorageObjectPath> {
    if table.is_empty()
        || table == "."
        || table == ".."
        || table.contains(['/', '\\', ':'])
    {
        return Err(VeinError::planning(
            "managed table names must be a single portable path component",
            None,
        ));
    }
    artifact_path(&format!(".vein/catalog/{table}.json"))
}

/// Returns the root-relative path of an external table's analyzed statistics.
pub fn external_statistics_path(table: &str) -> StorageObjectPath {
    let identifier = common::statistics::stable_identifier_hash(table);
    StorageObjectPath::new(format!(".vein/statistics/{identifier:016x}.json"))
}

/// Validates a portable root-relative path stored in a Parquet manifest.
///
/// Manifest paths use `/` on every platform. Rejecting empty, dot, parent,
/// backslash, and drive-prefix components prevents traversal and avoids a path
/// changing meaning when a manifest moves between operating systems.
pub fn artifact_path(path: &str) -> Result<StorageObjectPath> {
    let components = path.split('/');
    let drive_prefix = path.as_bytes().get(1) == Some(&b':');
    if path.is_empty()
        || path.starts_with('/')
        || path.starts_with('\\')
        || path.contains('\\')
        || drive_prefix
        || components.clone().any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(VeinError::internal(
            "catalog manifest contains an unsafe artifact path",
        ));
    }

    Ok(StorageObjectPath::new(path.to_owned()))
}
