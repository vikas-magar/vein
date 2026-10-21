use catalog::{CatalogSnapshotToken, StatisticsSourceIdentity};
use common::statistics::analysis::revision_from_identities;
use common::statistics::{runtime::table_statistics_key, schema_fingerprint, stable_identifier_hash};
use error::{Result, VeinError};
use plan::TableMetadata;
use storage::{StorageFileMetadata, StorageLocation};

use super::provider::ParquetCatalog;
use super::statistics_source_identity;

pub(super) fn source_identities(
    metadata: &TableMetadata,
    location: &StorageLocation,
    managed: bool,
) -> Vec<StatisticsSourceIdentity> {
    let mut identities = metadata
        .storage_metadata
        .iter()
        .map(|source: &StorageFileMetadata| {
            let path = source.identity.path();
            let identifier = if managed {
                path.as_str().to_owned()
            } else {
                super::metadata::absolute_path(location, path)
            };
            statistics_source_identity(identifier, source.size_bytes, &source.version)
        })
        .collect::<Vec<_>>();
    identities.sort_by(|left, right| left.identifier.cmp(&right.identifier));
    identities
}

pub(super) fn pin_source_snapshot(
    location: &StorageLocation,
    metadata: &mut TableMetadata,
) -> Result<()> {
    let identities = source_identities(metadata, location, false);
    let revision = revision_from_identities(&identities);
    let schema_version = stable_identifier_hash(&schema_fingerprint(&metadata.schema));
    metadata.catalog_snapshot = Some(CatalogSnapshotToken::new(
        metadata.name.clone(),
        format!("source:{revision:016x}"),
        format!("{revision:016x}"),
        schema_version,
    ));
    Ok(())
}

pub(super) fn apply_runtime_statistics(
    catalog: &ParquetCatalog,
    metadata: &mut TableMetadata,
) -> Result<()> {
    let key = metadata
        .catalog_snapshot
        .as_ref()
        .map(table_statistics_key)
        .ok_or_else(|| VeinError::internal("catalog source snapshot was not assigned"))?;
    if let Some(runtime) = catalog.runtime_statistics.complete_table_statistics(&key) {
        let statistics = metadata.statistics.get_or_insert_with(Default::default);
        statistics.num_rows = runtime.num_rows;
        statistics.column_statistics.extend(runtime.column_statistics.clone());
        if !runtime.column_groups.is_empty() {
            statistics.column_groups.clone_from(&runtime.column_groups);
        }
    }
    Ok(())
}
