use catalog::AnalyzedTableStatistics;
use error::{Result, VeinError};
use futures::TryStreamExt;
use storage::{ReadStorage, StorageObjectPath};

use super::manifest::{ExternalStatisticsSidecar, TableManifest, artifact_path, external_statistics_path, table_manifest_path};

pub(super) struct CatalogFiles {
    pub(super) paths: Vec<StorageObjectPath>,
    pub(super) manifest: Option<TableManifest>,
}

pub(super) async fn discover(
    storage: &dyn ReadStorage,
    table: &str,
    table_path: &StorageObjectPath,
) -> Result<CatalogFiles> {
    if let Some(manifest) = read_manifest(storage, table).await? {
        let paths = manifest
            .files
            .iter()
            .map(|path| artifact_path(path))
            .collect::<Result<Vec<_>>>()?;
        if paths.is_empty() {
            return Err(VeinError::not_found(format!(
                "managed Parquet table '{table}' has no committed files"
            )));
        }
        return Ok(CatalogFiles { paths, manifest: Some(manifest) });
    }

    let mut paths = storage
        .list(table_path)
        .try_collect::<Vec<_>>()
        .await?;
    paths.retain(is_visible_parquet_file);
    paths.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    paths.dedup();
    if paths.is_empty() {
        return Err(VeinError::not_found(format!(
            "no Parquet files found for table '{table}'"
        )));
    }
    Ok(CatalogFiles { paths, manifest: None })
}

pub(super) async fn read_manifest(
    storage: &dyn ReadStorage,
    table: &str,
) -> Result<Option<TableManifest>> {
    let Some(bytes) = storage.read_optional(&table_manifest_path(table)?).await? else {
        return Ok(None);
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| VeinError::internal(format!("invalid table manifest for '{table}': {error}")))
}

pub(super) async fn read_external_statistics(
    storage: &dyn ReadStorage,
    table: &str,
) -> Result<Option<AnalyzedTableStatistics>> {
    let Some(bytes) = storage
        .read_optional(&external_statistics_path(table))
        .await?
    else {
        return Ok(None);
    };
    let sidecar: ExternalStatisticsSidecar = serde_json::from_slice(&bytes)
        .map_err(|error| VeinError::internal(format!("invalid statistics sidecar for '{table}': {error}")))?;
    Ok((sidecar.table_name == table).then_some(sidecar.statistics))
}

fn is_visible_parquet_file(path: &StorageObjectPath) -> bool {
    let value = path.as_str();
    if value
        .split('/')
        .any(|component| component.starts_with('.') || component.starts_with('_'))
    {
        return false;
    }
    value
        .rsplit('/')
        .next()
        .and_then(|name| name.rsplit_once('.'))
        .is_some_and(|(_, extension)| extension.eq_ignore_ascii_case("parquet"))
}

#[cfg(test)]
mod tests {
    use super::is_visible_parquet_file;
    use storage::StorageObjectPath;

    #[test]
    fn listing_filters_hidden_paths_and_non_parquet_files() {
        for path in ["table/_metadata.parquet", "table/.hidden/a.parquet", "table/a.csv"] {
            assert!(!is_visible_parquet_file(&StorageObjectPath::new(path)));
        }
        for path in ["table/a.parquet", "table/nested/b.PARQUET"] {
            assert!(is_visible_parquet_file(&StorageObjectPath::new(path)));
        }
    }
}
