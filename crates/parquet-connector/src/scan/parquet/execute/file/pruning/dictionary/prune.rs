//! Applies dictionary evidence and attaches page byte locations to selections.

use std::time::Instant;

use parquet::arrow::arrow_reader::RowGroupSelection;
use parquet::file::metadata::ParquetMetaData;

use super::super::super::super::super::page_pruning::{
    attach_header_page_locations, page_payload_pruning_metrics,
};
use super::super::metadata::MetadataOutcome;
use super::super::predicates::PredicateInputs;
use super::super::{PruningContext, PruningSummary};
use super::DictionaryOutcome;
use super::load::DictionaryLoad;

pub(super) fn apply(
    parquet_metadata: &ParquetMetaData,
    context: &PruningContext<'_>,
    inputs: &PredicateInputs,
    metadata: &MetadataOutcome,
    loaded: DictionaryLoad,
    selections: Vec<RowGroupSelection>,
    summary: &mut PruningSummary,
) -> DictionaryOutcome {
    let prune_started = Instant::now();
    let row_group_pruning = crate::parquet_statistics::prune_row_groups_with_dictionary(
        parquet_metadata,
        selections,
        &loaded.row_group_membership,
    );
    let dictionary_page_pruning =
        crate::parquet_statistics::prune_dictionary_encoded_pages(
            row_group_pruning.selections,
            &metadata.page_header_result.pages,
            &inputs.dictionary_predicates,
            &loaded.page_membership,
        );
    let final_pruning = attach_header_page_locations(
        dictionary_page_pruning.selections,
        &metadata.page_header_result.page_locations,
    );
    let payload_metrics =
        page_payload_pruning_metrics(&final_pruning, &metadata.page_header_result.page_locations);
    summary.dictionary_predicate_probes = row_group_pruning.probes;
    summary.row_groups_skipped_by_dictionary = row_group_pruning.row_groups_skipped;
    summary.rows_skipped_by_dictionary = row_group_pruning.rows_skipped;
    summary.estimated_compressed_bytes_skipped_by_dictionary =
        row_group_pruning.estimated_compressed_bytes_skipped;
    summary.row_groups_selected_after_dictionary = final_pruning.len();
    summary.dictionary_pruning_us = prune_started.elapsed().as_micros() as u64;

    tracing::info!(
        target: "vein.parquet_dictionary",
        event = "parquet_dictionary_data_page_pruning",
        path = %context.path,
        chunks_fully_dictionary_encoded = loaded.metrics.fully_encoded_chunks,
        chunks_page_filtered = loaded.metrics.page_filtered_chunks,
        predicate_page_checks = dictionary_page_pruning.predicate_page_checks,
        dictionary_pages_proven_absent = dictionary_page_pruning.dictionary_pages_proven_absent,
        rows_skipped = dictionary_page_pruning.rows_skipped,
        row_groups_emptied = dictionary_page_pruning.row_groups_emptied,
        estimated_payload_bytes_skipped = payload_metrics.skipped_payload_bytes,
        "dictionary membership applied to dictionary-encoded data pages"
    );
    tracing::debug!(
        target: "vein.audit",
        event = "parquet_page_payload_pruning",
        path = %context.path,
        columns_with_locations = payload_metrics.columns_with_locations,
        data_pages_available = payload_metrics.data_pages_available,
        data_pages_selected = payload_metrics.data_pages_selected,
        data_pages_skipped = payload_metrics.data_pages_skipped,
        selected_payload_bytes_estimate = payload_metrics.selected_payload_bytes,
        skipped_payload_bytes_estimate = payload_metrics.skipped_payload_bytes,
        "estimated Parquet page payload omitted by row selection"
    );
    tracing::debug!(
        target: "vein.audit",
        event = "parquet_dictionary_pruning",
        path = %context.path,
        predicates = inputs.dictionary_predicates.len(),
        columns = inputs.dictionary_columns.len(),
        chunks_considered = loaded.metrics.chunks_considered,
        chunks_fully_dictionary_encoded = loaded.metrics.chunks_eligible,
        pages_loaded = loaded.metrics.pages_loaded,
        page_bytes = loaded.metrics.page_bytes,
        unsupported_or_missing = loaded.metrics.unsupported_chunks,
        read_errors = loaded.metrics.read_errors,
        predicate_probes = summary.dictionary_predicate_probes,
        row_groups_skipped = summary.row_groups_skipped_by_dictionary,
        rows_skipped = summary.rows_skipped_by_dictionary,
        estimated_compressed_bytes_skipped =
            summary.estimated_compressed_bytes_skipped_by_dictionary,
        load_us = summary.dictionary_load_us,
        prune_us = summary.dictionary_pruning_us,
        "Parquet dictionary page pruning decisions"
    );

    DictionaryOutcome {
        selections: final_pruning,
        rows_skipped: dictionary_page_pruning.rows_skipped,
    }
}
