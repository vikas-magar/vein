//! Loads eligible column-chunk Bloom filters and prunes candidate row groups.

use std::collections::HashMap;
use common::time::Instant;

use parquet::arrow::arrow_reader::RowGroupSelection;

use crate::parquet_statistics::{ParquetBloomFilter, equality_predicate_columns};

use super::ParquetScanBuilder;
use super::predicates::PredicateInputs;
use super::{PruningContext, PruningSummary};

pub(super) struct BloomOutcome {
    pub(super) selections: Vec<RowGroupSelection>,
}

pub(super) async fn apply(
    builder: &mut ParquetScanBuilder,
    context: &PruningContext<'_>,
    inputs: &PredicateInputs,
    selections: Vec<RowGroupSelection>,
    summary: &mut PruningSummary,
) -> BloomOutcome {
    let static_predicates = context
        .pushdown
        .map_or(&[][..], |predicate| predicate.statistics.as_slice());
    let static_columns = equality_predicate_columns(static_predicates);
    let runtime_columns =
        equality_predicate_columns(&inputs.statistics[inputs.static_count..]);
    let row_groups_considered = selections.len() as u64;
    let mut available = 0_u64;
    let mut loaded = 0_u64;
    let mut read_errors = 0_u64;
    let mut chunks_checked = 0_u64;
    let mut chunks_without_metadata = 0_u64;
    let mut column_metadata_missing = 0_u64;
    let mut bytes = 0_u64;
    let load_started = Instant::now();
    let mut filters_by_row_group = HashMap::new();

    for selection in &selections {
        let row_group_index = selection.row_group_index();
        let mut filters = HashMap::with_capacity(inputs.bloom_columns.len());
        for column in &inputs.bloom_columns {
            let row_group = builder.metadata().row_group(row_group_index);
            let Some(metadata) = row_group.columns().get(column.parquet_column_index) else {
                column_metadata_missing = column_metadata_missing.saturating_add(1);
                continue;
            };
            chunks_checked = chunks_checked.saturating_add(1);
            let bloom_filter_length = metadata
                .bloom_filter_length()
                .and_then(|length| u64::try_from(length).ok());
            if metadata.bloom_filter_offset().is_none() {
                chunks_without_metadata = chunks_without_metadata.saturating_add(1);
                continue;
            }
            available = available.saturating_add(1);
            match builder
                .get_row_group_column_bloom_filter(row_group_index, column.parquet_column_index)
                .await
            {
                Ok(Some(filter)) => {
                    loaded = loaded.saturating_add(1);
                    let decoded_bytes = u64::try_from(filter.num_blocks())
                        .unwrap_or(u64::MAX)
                        .saturating_mul(32);
                    bytes = bytes.saturating_add(bloom_filter_length.unwrap_or(decoded_bytes));
                    filters.insert(
                        column.name.clone(),
                        ParquetBloomFilter::new(column.clone(), filter),
                    );
                }
                Ok(None) => {}
                Err(error) => {
                    read_errors = read_errors.saturating_add(1);
                    tracing::debug!(
                        target: "vein.audit",
                        event = "parquet_bloom_filter_unavailable",
                        source_id = %common::logging::LogLocation::from(context.path),
                        row_group = row_group_index,
                        column = %column.name,
                        error = %error,
                        "ignoring unavailable Parquet Bloom filter"
                    );
                }
            }
        }
        if !filters.is_empty() {
            filters_by_row_group.insert(row_group_index, filters);
        }
    }
    summary.bloom_filter_load_us = load_started.elapsed().as_micros() as u64;
    summary.bloom_filters_available = available;
    summary.bloom_filters_loaded = loaded;
    summary.bloom_filter_read_errors = read_errors;
    summary.bloom_filter_bytes = bytes;
    log_eligibility(
        context.path,
        &static_columns,
        &runtime_columns,
        inputs.bloom_columns.len(),
        row_groups_considered,
        chunks_checked,
        chunks_without_metadata,
        column_metadata_missing,
        available,
        loaded,
        read_errors,
    );

    let pruning_started = Instant::now();
    let pruning = crate::parquet_statistics::prune_row_groups_with_bloom(
        builder.metadata(),
        &inputs.statistics,
        selections,
        &filters_by_row_group,
    );
    summary.row_groups_selected_after_bloom = pruning.selections.len();
    summary.row_groups_skipped_by_bloom = pruning.row_groups_skipped;
    summary.rows_skipped_by_bloom = pruning.rows_skipped;
    summary.estimated_compressed_bytes_skipped_by_bloom =
        pruning.estimated_compressed_bytes_skipped;
    summary.bloom_predicate_probes = pruning.probes;
    summary.bloom_pruning_us = pruning_started.elapsed().as_micros() as u64;
    BloomOutcome {
        selections: pruning.selections,
    }
}

#[allow(clippy::too_many_arguments)]
fn log_eligibility(
    path: &str,
    static_columns: &[String],
    runtime_columns: &[String],
    mapped_columns: usize,
    row_groups_considered: u64,
    chunks_checked: u64,
    chunks_without_metadata: u64,
    column_metadata_missing: u64,
    available: u64,
    loaded: u64,
    read_errors: u64,
) {
    let reason = if static_columns.is_empty() && runtime_columns.is_empty() {
        "no_equality_predicate_columns"
    } else if mapped_columns == 0 {
        "equality_columns_not_mappable_to_parquet_schema"
    } else if available == 0 {
        "no_column_chunk_bloom_metadata"
    } else if loaded == 0 {
        "column_chunk_bloom_filters_unavailable_or_unreadable"
    } else {
        "column_chunk_bloom_filters_loaded"
    };
    tracing::info!(
        target: "vein.parquet_bloom",
        event = "parquet_bloom_filter_eligibility",
        source_id = %common::logging::LogLocation::from(path),
        static_equality_columns = ?static_columns,
        runtime_equality_columns = ?runtime_columns,
        mapped_columns,
        row_groups_considered,
        chunks_checked,
        chunks_without_bloom_metadata = chunks_without_metadata,
        column_metadata_missing,
        filters_available = available,
        filters_loaded = loaded,
        read_errors,
        eligibility_reason = reason,
        "why Parquet column-chunk Bloom pruning is or is not available"
    );
}
