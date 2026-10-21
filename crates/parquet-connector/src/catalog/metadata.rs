use std::sync::Arc;
use std::time::Instant;

use catalog::MetadataStore;
use common::Result;
use error::VeinError;
use futures::{StreamExt, TryStreamExt};
use plan::{TableMetadata, VeinSchema};
use storage::{ReadStorage, StorageLocation, StorageObjectPath};

use crate::parquet_statistics::{file_units, table_statistics};
use crate::scan::load_footer_from_storage;

pub(super) async fn inspect_files(
    name: &str,
    location: &StorageLocation,
    table_path: &StorageObjectPath,
    paths: &[StorageObjectPath],
    storage: &Arc<dyn ReadStorage>,
    metadata_store: &MetadataStore,
) -> Result<TableMetadata> {
    let started = Instant::now();
    let stat_started = Instant::now();
    let snapshots = futures::future::try_join_all(paths.iter().map(|path| storage.stat(path)))
        .await?;
    let stat_us = stat_started.elapsed().as_micros() as u64;

    let footer_started = Instant::now();
    let concurrency = std::thread::available_parallelism()
        .map_or(1, |value| value.get())
        .max(1);
    let storage = Arc::clone(storage);
    let metadata_store = metadata_store.clone();
    let footer_inputs = paths.iter().cloned().zip(snapshots.iter().cloned());
    let footers = futures::stream::iter(footer_inputs)
        .map(|(path, snapshot)| {
            let storage = Arc::clone(&storage);
            let metadata_store = metadata_store.clone();
            async move {
                let (schema, footer, cache_hit) =
                    load_footer_from_storage(&storage, &path, &snapshot, &metadata_store).await?;
                Ok::<_, VeinError>((schema, footer, cache_hit))
            }
        })
        .buffered(concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    let footer_us = footer_started.elapsed().as_micros() as u64;
    let expected_schema = footers
        .first()
        .map(|footer| Arc::clone(&footer.0))
        .ok_or_else(|| VeinError::not_found(format!("table '{name}' has no Parquet files")))?;
    for (path, (schema, _, _)) in paths.iter().zip(&footers).skip(1) {
        if schema.as_ref() != expected_schema.as_ref() {
            return Err(VeinError::internal(format!(
                "Parquet schema mismatch in '{}'; all files in one table must have identical schemas",
                path.as_str()
            )));
        }
    }

    let mut units = Vec::new();
    for (ordinal, ((schema, footer, _), snapshot)) in footers.iter().zip(&snapshots).enumerate() {
        let first_unit = units.len();
        let mut file_units = file_units(ordinal, snapshot.size_bytes, schema, footer);
        for unit in &mut file_units {
            if let Some(parent) = &mut unit.parent {
                *parent += first_unit;
            }
        }
        units.extend(file_units);
    }
    let statistics = table_statistics(units, expected_schema.as_ref());
    let cache_hits = footers.iter().filter(|(_, _, hit)| *hit).count();
    let file_names = paths
        .iter()
        .map(|path| absolute_path(location, path))
        .collect::<Vec<_>>();
    let schema = Arc::new(VeinSchema::try_from(expected_schema.as_ref())?);
    let metadata = TableMetadata::new(name, schema)
        .with_location(absolute_path(location, table_path))
        .with_file_format("parquet")
        .with_files(file_names)
        .with_storage_metadata(snapshots)
        .with_statistics(statistics);
    let statistics = metadata.statistics.as_ref().expect("just assigned statistics");
    tracing::info!(
        target: "vein.audit",
        event = "catalog_schema",
        table = %name,
        file_count = paths.len(),
        row_group_count = statistics.row_group_count.unwrap_or_default(),
        table_rows = statistics.num_rows.unwrap_or_default(),
        table_bytes = statistics.total_bytes.unwrap_or_default(),
        compressed_bytes = statistics.compressed_bytes.unwrap_or_default(),
        uncompressed_bytes = statistics.uncompressed_bytes.unwrap_or_default(),
        column_statistics_count = statistics.column_statistics.len(),
        footer_hits = cache_hits,
        footer_misses = footers.len() - cache_hits,
        stat_us,
        footer_us,
        total_us = started.elapsed().as_micros() as u64,
        "catalog_schema"
    );
    Ok(metadata)
}

pub(super) fn absolute_path(location: &StorageLocation, path: &StorageObjectPath) -> String {
    match location {
        StorageLocation::Local(root) => root.join(path.as_str()).to_string_lossy().into_owned(),
        StorageLocation::Uri(root) => {
            format!("{}/{}", root.trim_end_matches('/'), path.as_str())
        }
    }
}
