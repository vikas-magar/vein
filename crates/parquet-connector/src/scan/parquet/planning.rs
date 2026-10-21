use std::sync::Arc;

use catalog::{MetadataKey, MetadataStore};
use common::Result;
use common::protocol::{DataSource, FileScanTask};
use error::VeinError;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::file::metadata::ParquetMetaData;

use exec::operators::source::scan::{
    ScanPlanningRequest, coalesce_scan_units, file_scan_work_unit,
};
use exec::runtime::filter::RuntimeFilterSet;

use super::PARQUET_FOOTER;

pub(crate) fn plan_scan(request: ScanPlanningRequest<'_>) -> Result<Vec<DataSource>> {
    let ScanPlanningRequest {
        scan,
        metadata_store,
        max_parallelism,
        runtime_filters,
    } = request;
    partition_parquet_scan(scan, metadata_store, max_parallelism, runtime_filters)
}

fn partition_parquet_scan(
    scan: &FileScanTask,
    metadata_store: &MetadataStore,
    max_parallelism: usize,
    runtime_filters: Option<&RuntimeFilterSet>,
) -> Result<Vec<DataSource>> {
    let mut predicates = scan
        .pushdown
        .as_ref()
        .map_or_else(Vec::new, |p| p.statistics.clone());
    let static_predicate_count = predicates.len();
    let runtime_predicates = runtime_filters.map_or_else(Vec::new, |filters| {
        filters.statistics_predicates(scan.projected.as_ref())
    });
    let runtime_predicate_count = runtime_predicates.len();
    predicates.extend(runtime_predicates);
    let mut work = Vec::new();
    let mut row_group_files = Vec::new();
    let mut footer_hits = 0_usize;
    let mut footer_misses = 0_usize;
    let mut candidate_units = 0_usize;
    let mut total_row_groups = 0_usize;

    for file_index in 0..scan.files.len() {
        let key = scan
            .storage_metadata
            .get(file_index)
            .and_then(|metadata| MetadataKey::from_storage(PARQUET_FOOTER, metadata));
        let metadata = key
            .as_ref()
            .and_then(|key| metadata_store.get::<ParquetMetaData>(key));
        let Some(metadata) = metadata else {
            footer_misses += 1;
            work.push(file_scan_work_unit(scan, file_index, None));
            continue;
        };
        footer_hits += 1;
        total_row_groups = total_row_groups.saturating_add(metadata.num_row_groups());
        let arrow_metadata =
            ArrowReaderMetadata::try_new(Arc::clone(&metadata), ArrowReaderOptions::new())
                .map_err(|error| {
                    VeinError::internal(format!("Failed to derive Parquet scan schema: {error}"))
                })?;
        let candidates = crate::parquet_statistics::prune_row_groups(
            arrow_metadata.schema().as_ref(),
            &metadata,
            &predicates,
        );
        candidate_units = candidate_units.saturating_add(candidates.selected.len());
        if candidates.selected.is_empty() {
            continue;
        }
        let weights = projected_row_group_bytes(scan, &metadata, &candidates.selected);
        row_group_files.push((file_index, candidates.selected, weights));
    }

    let total_weight = row_group_files
        .iter()
        .flat_map(|(_, _, weights)| weights)
        .fold(0_u64, |sum, weight| sum.saturating_add(*weight));
    let target_bytes = (max_parallelism > 1).then(|| {
        total_weight
            .div_ceil(max_parallelism.saturating_mul(2).max(1) as u64)
            .max(1)
    });
    for (file_index, groups, weights) in row_group_files {
        let weighted_groups = groups.into_iter().zip(weights);
        for range in coalesce_scan_units(weighted_groups, target_bytes) {
            work.push(file_scan_work_unit(scan, file_index, Some(range)));
        }
    }

    trace_partition(
        scan,
        runtime_filters,
        max_parallelism,
        static_predicate_count,
        runtime_predicate_count,
        footer_hits,
        footer_misses,
        total_row_groups,
        candidate_units,
        work.len(),
    );
    Ok(work)
}

fn projected_row_group_bytes(
    scan: &FileScanTask,
    metadata: &ParquetMetaData,
    groups: &[usize],
) -> Vec<u64> {
    groups
        .iter()
        .map(|index| {
            metadata
                .row_group(*index)
                .columns()
                .iter()
                .filter(|column| {
                    scan.projected.fields.is_empty()
                        || scan
                        .projected
                        .fields
                        .iter()
                        .any(|field| column.column_path().parts().first() == Some(&field.name))
                })
                .fold(0_u64, |sum, column| {
                    sum.saturating_add(column.compressed_size().max(1) as u64)
                })
                .max(1)
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn trace_partition(
    scan: &FileScanTask,
    runtime_filters: Option<&RuntimeFilterSet>,
    max_parallelism: usize,
    static_predicates: usize,
    runtime_predicates: usize,
    footer_hits: usize,
    footer_misses: usize,
    total_row_groups: usize,
    candidate_units: usize,
    work_units: usize,
) {
    tracing::debug!(target: "vein.audit", event = "scan_partitioned", format = "parquet",
        file_count = scan.files.len(), footer_hits, footer_misses,
        static_statistics_predicates = static_predicates,
        runtime_statistics_predicates = runtime_predicates,
        runtime_filter_names = ?runtime_filters.map_or_else(Vec::new, RuntimeFilterSet::names),
        total_row_groups, candidate_row_groups = candidate_units,
        pruned_row_groups = total_row_groups.saturating_sub(candidate_units),
        max_parallelism, scheduled_work_units = work_units,
        "Parquet scan decomposed into byte-balanced row-group ranges");
}
