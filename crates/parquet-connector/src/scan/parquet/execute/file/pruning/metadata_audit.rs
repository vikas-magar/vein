//! Bounded diagnostic events for page-header pruning evidence.

use crate::parquet_statistics::PagePruning;

use super::super::super::super::super::page_header_reader::PageHeaderReadResult;
use super::super::summary::PruningSummary;

pub(super) fn log_page_header_decisions(
    path: &str,
    audit_enabled: bool,
    result: &PageHeaderReadResult,
    pruning: &PagePruning,
    summary: &PruningSummary,
) {
    let metrics = &result.metrics;
    if audit_enabled {
        tracing::info!(
            target: "vein.parquet_audit",
            event = "parquet_page_header_audit_summary",
            source_id = %common::logging::LogLocation::from(path),
            predicate_count = pruning.predicate_diagnostics.predicates.len(),
            page_index_columns = summary.page_index_columns,
            page_bounds_complete = summary.page_bounds_complete,
            page_bounds_incomplete = summary.page_bounds_incomplete,
            columns_requested = metrics.columns_requested,
            column_row_group_pairs_requested = metrics.column_row_group_pairs_requested,
            chunks_scanned = metrics.chunks_scanned,
            chunks_failed = metrics.failed_chunks,
            chunks_unsupported_row_mapping = metrics.unsupported_row_mapping_chunks,
            pages_scanned = metrics.pages_scanned,
            pages_with_statistics = metrics.pages_with_statistics,
            pages_with_complete_bounds = metrics.pages_with_complete_bounds,
            probe_bytes_read = metrics.probe_bytes,
            header_bytes_parsed = metrics.header_bytes,
            pages_skipped_rows = summary.page_header_rows_skipped,
            row_groups_skipped = summary.row_groups_skipped_by_page_header,
            "opt-in Parquet page-header audit summary"
        );
    }
    tracing::debug!(
        target: "vein.audit",
        event = "parquet_predicate_pruning",
        source_id = %common::logging::LogLocation::from(path),
        stage = "page_header",
        audit_forced = audit_enabled,
        columns_requested = metrics.columns_requested,
        column_row_group_pairs_requested = metrics.column_row_group_pairs_requested,
        chunks_scanned = metrics.chunks_scanned,
        chunks_failed = metrics.failed_chunks,
        chunks_unsupported_row_mapping = metrics.unsupported_row_mapping_chunks,
        pages_scanned = metrics.pages_scanned,
        pages_with_statistics = metrics.pages_with_statistics,
        pages_with_complete_bounds = metrics.pages_with_complete_bounds,
        probe_bytes_read = metrics.probe_bytes,
        header_bytes_parsed = metrics.header_bytes,
        probe_overread_bytes = metrics.probe_bytes.saturating_sub(metrics.header_bytes),
        pages_skipped_rows = summary.page_header_rows_skipped,
        row_groups_skipped = summary.row_groups_skipped_by_page_header,
        predicate_count = pruning.predicate_diagnostics.predicates.len(),
        "Parquet data-page header pruning decisions"
    );
}
