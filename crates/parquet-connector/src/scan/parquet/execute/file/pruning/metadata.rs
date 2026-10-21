//! Footer, page-index, and page-header pruning for one assigned file range.

use std::time::Instant;

use arrow::datatypes::Schema;
use parquet::arrow::arrow_reader::RowGroupSelection;
use parquet::file::metadata::ParquetMetaData;

use super::super::super::super::page_header_reader::{
    PageHeaderReadRequest, PageHeaderReadResult, read_page_header_statistics,
};
use super::super::super::super::page_pruning::page_header_audit_enabled;
use super::{PruningContext, PruningSummary};
use crate::parquet_statistics::BloomFilterColumn;

#[path = "metadata_audit.rs"]
mod audit;

pub(super) struct MetadataOutcome {
    pub(super) page_selections: Vec<RowGroupSelection>,
    pub(super) page_header_result: PageHeaderReadResult,
}

pub(super) async fn apply(
    metadata: &ParquetMetaData,
    arrow_schema: &Schema,
    context: &PruningContext<'_>,
    predicates: &[catalog::StatisticsPredicate],
    dictionary_columns: &[BloomFilterColumn],
    summary: &mut PruningSummary,
) -> MetadataOutcome {
    let row_group_pruning_started = Instant::now();
    let row_groups_total = metadata.num_row_groups();
    let assigned_range = context
        .file
        .scan_unit_range
        .clone()
        .unwrap_or(0..row_groups_total);
    summary.row_groups_total = row_groups_total;
    summary.row_groups_assigned = assigned_range
        .end
        .min(row_groups_total)
        .saturating_sub(assigned_range.start.min(row_groups_total));

    let row_group_pruning = crate::parquet_statistics::prune_row_groups_in_range(
        arrow_schema,
        metadata,
        predicates,
        assigned_range,
    );
    summary.row_groups_selected = row_group_pruning.selected.len();
    summary.row_groups_skipped = summary
        .row_groups_assigned
        .saturating_sub(summary.row_groups_selected);
    summary.row_groups_skipped_rows = row_group_pruning.skipped_rows;
    summary.estimated_compressed_bytes_skipped =
        row_group_pruning.estimated_compressed_bytes_skipped;
    tracing::debug!(
        target: "vein.audit",
        event = "parquet_predicate_pruning",
        source_id = %common::logging::LogLocation::from(context.path),
        stage = "row_group_footer",
        predicate_count = row_group_pruning.predicate_diagnostics.predicates.len(),
        "per-predicate Parquet footer pruning decisions"
    );

    let page_pruning_started = Instant::now();
    summary.page_index_available = metadata.page_index().is_some();
    let page_index_pruning = crate::parquet_statistics::prune_data_pages(
        arrow_schema,
        metadata,
        predicates,
        &row_group_pruning.selected,
    );
    summary.row_groups_selected_after_page_index = page_index_pruning.selections.len();
    summary.row_groups_skipped_by_page_index = page_index_pruning.row_groups_skipped;
    summary.page_index_columns = page_index_pruning.indexed_columns;
    summary.page_bounds_complete = page_index_pruning.page_bounds_complete;
    summary.page_bounds_incomplete = page_index_pruning.page_bounds_incomplete;
    summary.page_ranges_considered = page_index_pruning.row_ranges_considered;
    summary.page_ranges_skipped = page_index_pruning.row_ranges_skipped;
    summary.page_rows_skipped = page_index_pruning.rows_skipped;
    tracing::debug!(
        target: "vein.audit",
        event = "parquet_predicate_pruning",
        source_id = %common::logging::LogLocation::from(context.path),
        stage = "page_index",
        predicate_count = page_index_pruning.predicate_diagnostics.predicates.len(),
        "per-predicate Parquet page-index pruning decisions"
    );

    let header_row_groups = page_index_pruning
        .selections
        .iter()
        .map(RowGroupSelection::row_group_index)
        .collect::<Vec<_>>();
    let audit_page_headers = page_header_audit_enabled();
    let mut page_header_plan = crate::parquet_statistics::page_header_requests_with_audit(
        predicates,
        &header_row_groups,
        summary.page_index_available,
        &page_index_pruning,
        audit_page_headers,
    );
    page_header_plan.merge(crate::parquet_statistics::dictionary_page_header_requests(
        metadata,
        &header_row_groups,
        dictionary_columns.iter(),
    ));

    let page_header_started = Instant::now();
    let page_header_result = if page_header_plan.is_empty() {
        PageHeaderReadResult::default()
    } else {
        read_page_header_statistics(PageHeaderReadRequest {
            storage: context.storage,
            path: context.path,
            absolute_path: &context.file.absolute,
            backend: context.io_backend,
            file_size_bytes: context.file_size_bytes,
            bytes_read: context.bytes_read,
            io: context.io,
            arrow_schema,
            metadata,
            plan: &page_header_plan,
        })
            .await
    };
    record_header_metrics(summary, &page_header_result);
    let mut page_pruning = if page_header_result.pages.is_empty() {
        page_index_pruning
    } else {
        crate::parquet_statistics::prune_data_pages_with_headers(
            arrow_schema,
            metadata,
            predicates,
            &row_group_pruning.selected,
            &page_header_result.pages,
        )
    };
    summary.row_groups_selected_after_page_header = page_pruning.selections.len();
    summary.row_groups_skipped_by_page_header = summary
        .row_groups_selected_after_page_index
        .saturating_sub(summary.row_groups_selected_after_page_header);
    summary.page_header_rows_skipped =
        page_pruning.rows_skipped.saturating_sub(summary.page_rows_skipped);
    audit::log_page_header_decisions(
        context.path,
        audit_page_headers,
        &page_header_result,
        &page_pruning,
        summary,
    );
    summary.page_pruning_us = page_pruning_started.elapsed().as_micros() as u64;
    summary.page_header_pruning_us = page_header_started.elapsed().as_micros() as u64;
    summary.header_scan_us = page_header_result.metrics.elapsed_us;
    summary.row_group_pruning_us = row_group_pruning_started.elapsed().as_micros() as u64;

    MetadataOutcome {
        page_selections: std::mem::take(&mut page_pruning.selections),
        page_header_result,
    }
}

fn record_header_metrics(summary: &mut PruningSummary, result: &PageHeaderReadResult) {
    let metrics = &result.metrics;
    summary.page_header_columns = metrics.columns_requested;
    summary.page_header_column_row_group_pairs = metrics.column_row_group_pairs_requested;
    summary.header_chunks_scanned = metrics.chunks_scanned;
    summary.header_chunks_failed = metrics.failed_chunks;
    summary.header_chunks_unsupported_row_mapping = metrics.unsupported_row_mapping_chunks;
    summary.header_pages_loaded = metrics.pages_scanned;
    summary.header_pages_with_statistics = metrics.pages_with_statistics;
    summary.header_pages_with_complete_bounds = metrics.pages_with_complete_bounds;
    summary.header_bytes_read = metrics.probe_bytes;
    summary.header_bytes_parsed = metrics.header_bytes;
}
