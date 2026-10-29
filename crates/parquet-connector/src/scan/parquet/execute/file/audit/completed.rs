use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use common::time::Instant;

use common::protocol::ScanPredicate;
use parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;

use crate::scan::parquet::execute::file::pruning::PruningSummary;
use crate::scan::parquet::metrics::ScanPredicateMetrics;
use crate::scan::parquet::predicate::{AdaptiveFilterOrder, PredicateStageMetrics};
use crate::scan::parquet::reader::IoStats;
use crate::scan::parquet::stream::ScanStreamMetrics;

pub(in crate::scan::parquet::execute::file) struct CompletedScan<'a> {
    pub(in crate::scan::parquet::execute::file) path: &'a str,
    pub(in crate::scan::parquet::execute::file) scan_started: Instant,
    pub(in crate::scan::parquet::execute::file) bytes_before_file: u64,
    pub(in crate::scan::parquet::execute::file) bytes_read: &'a AtomicU64,
    pub(in crate::scan::parquet::execute::file) file_size_bytes: u64,
    pub(in crate::scan::parquet::execute::file) io: &'a IoStats,
    pub(in crate::scan::parquet::execute::file) stream: &'a ScanStreamMetrics,
    pub(in crate::scan::parquet::execute::file) predicate_metrics: &'a ScanPredicateMetrics,
    pub(in crate::scan::parquet::execute::file) reader_metrics: &'a ArrowReaderMetrics,
    pub(in crate::scan::parquet::execute::file) stage_metrics: &'a [PredicateStageMetrics],
    pub(in crate::scan::parquet::execute::file) adaptive_order: Option<&'a Arc<AdaptiveFilterOrder>>,
    pub(in crate::scan::parquet::execute::file) summary: &'a PruningSummary,
    pub(in crate::scan::parquet::execute::file) pushdown: Option<&'a ScanPredicate>,
    pub(in crate::scan::parquet::execute::file) pushed_filter_columns: &'a [&'a str],
    pub(in crate::scan::parquet::execute::file) runtime_statistics_enabled: bool,
    pub(in crate::scan::parquet::execute::file) detailed_metrics_enabled: bool,
    pub(in crate::scan::parquet::execute::file) footer_hit: bool,
    pub(in crate::scan::parquet::execute::file) stat_us: u64,
    pub(in crate::scan::parquet::execute::file) metadata_us: u64,
    pub(in crate::scan::parquet::execute::file) build_us: u64,
    pub(in crate::scan::parquet::execute::file) file_rows: i64,
}

pub(in crate::scan::parquet::execute::file) fn log_completed(scan: CompletedScan<'_>) {
    let elapsed = scan.scan_started.elapsed();
    let bytes_read_total = scan
        .bytes_read
        .load(std::sync::atomic::Ordering::Relaxed)
        .saturating_sub(scan.bytes_before_file);
    let io_ranges = scan.io.ranges.load(std::sync::atomic::Ordering::Relaxed);
    let io_calls = scan.io.calls.load(std::sync::atomic::Ordering::Relaxed);
    let io_read_us = scan.io.read_us.load(std::sync::atomic::Ordering::Relaxed);
    let io_max_range_us = scan.io.max_range_us.load(std::sync::atomic::Ordering::Relaxed);
    let io_wall_us = scan.io.fetch_wall_us.load(std::sync::atomic::Ordering::Relaxed);
    let io_avg_bytes = bytes_read_total.checked_div(io_ranges).unwrap_or(0);
    let first_poll_offcpu = scan
        .stream
        .first_poll_time
        .saturating_sub(scan.stream.first_poll_thread_cpu);
    let predicate_metrics = scan.predicate_metrics.snapshot();
    let adaptive_filter_snapshot = scan.adaptive_order.map(|policy| policy.snapshot());
    if let Some(snapshot) = &adaptive_filter_snapshot {
        snapshot.log(scan.path);
    }
    if tracing::enabled!(target: "vein.audit", tracing::Level::DEBUG) {
        for (stage_index, predicate_count, input_columns, metrics) in scan.stage_metrics {
            let metrics = metrics.snapshot();
            tracing::debug!(target: "vein.audit", event = "scan_predicate_stage_completed",
                source_id = %common::logging::LogLocation::from(scan.path), stage_index, predicate_count, input_columns = ?input_columns,
                batches = metrics.batches, rows_in = metrics.rows_in, rows_kept = metrics.rows_kept,
                eval_wall_us = metrics.eval_wall_us,
                input_array_bytes_seen = metrics.input_array_bytes,
                "scan predicate stage completed");
        }
        tracing::debug!(target: "vein.audit", event = "parquet_column_decode_metrics",
            source_id = %common::logging::LogLocation::from(scan.path), columns = ?scan.reader_metrics.column_metrics(),
            records_decoded = ?scan.reader_metrics.records_read_from_inner(),
            records_reused = ?scan.reader_metrics.records_read_from_cache(),
            timing_scope = "sum_of_leaf_reader_call_elapsed_times;decompression_is_nested",
            "Parquet column decoding completed");
    }
    let summary = scan.summary;
    tracing::debug!(
        target: "vein.statistics",
        event = "scan_completed",
        source_id = %common::logging::LogLocation::from(scan.path),
        format = "parquet",
        footer_hit = scan.footer_hit,
        rows = scan.stream.total_rows,
        rows_decoded = scan.stream.physical_rows_read,
        file_rows = scan.file_rows,
        batches = scan.stream.batches,
        row_groups_total = summary.row_groups_total,
        row_groups_assigned = summary.row_groups_assigned,
        row_groups_selected = summary.row_groups_selected,
        row_groups_skipped = summary.row_groups_skipped,
        row_groups_selected_after_page_index = summary.row_groups_selected_after_page_index,
        row_groups_skipped_by_page_index = summary.row_groups_skipped_by_page_index,
        row_groups_selected_after_page_header = summary.row_groups_selected_after_page_header,
        row_groups_skipped_by_page_header = summary.row_groups_skipped_by_page_header,
        row_groups_skipped_rows = summary.row_groups_skipped_rows,
        estimated_compressed_bytes_skipped = summary.estimated_compressed_bytes_skipped,
        page_index_available = summary.page_index_available,
        page_index_columns = summary.page_index_columns,
        page_bounds_complete = summary.page_bounds_complete,
        page_bounds_incomplete = summary.page_bounds_incomplete,
        page_ranges_considered = summary.page_ranges_considered,
        page_ranges_skipped = summary.page_ranges_skipped,
        page_rows_skipped = summary.page_rows_skipped,
        page_header_columns = summary.page_header_columns,
        page_header_column_row_group_pairs = summary.page_header_column_row_group_pairs,
        header_chunks_scanned = summary.header_chunks_scanned,
        header_chunks_failed = summary.header_chunks_failed,
        header_chunks_unsupported_row_mapping = summary.header_chunks_unsupported_row_mapping,
        header_pages_loaded = summary.header_pages_loaded,
        header_pages_with_statistics = summary.header_pages_with_statistics,
        header_pages_with_complete_bounds = summary.header_pages_with_complete_bounds,
        header_probe_bytes = summary.header_bytes_read,
        header_bytes_parsed = summary.header_bytes_parsed,
        header_probe_overread_bytes = summary.header_bytes_read.saturating_sub(summary.header_bytes_parsed),
        page_header_rows_skipped = summary.page_header_rows_skipped,
        bloom_filter_columns = summary.bloom_filter_columns,
        bloom_filters_available = summary.bloom_filters_available,
        bloom_filters_loaded = summary.bloom_filters_loaded,
        bloom_filter_read_errors = summary.bloom_filter_read_errors,
        bloom_filter_bytes = summary.bloom_filter_bytes,
        bloom_filter_load_us = summary.bloom_filter_load_us,
        bloom_predicate_probes = summary.bloom_predicate_probes,
        row_groups_skipped_by_bloom = summary.row_groups_skipped_by_bloom,
        rows_skipped_by_bloom = summary.rows_skipped_by_bloom,
        estimated_compressed_bytes_skipped_by_bloom = summary.estimated_compressed_bytes_skipped_by_bloom,
        row_groups_selected_after_bloom = summary.row_groups_selected_after_bloom,
        dictionary_predicates = summary.dictionary_predicates,
        dictionary_columns = summary.dictionary_columns,
        dictionary_chunks_considered = summary.dictionary_chunks_considered,
        dictionary_chunks_eligible = summary.dictionary_chunks_eligible,
        dictionary_pages_loaded = summary.dictionary_pages_loaded,
        dictionary_page_bytes = summary.dictionary_page_bytes,
        dictionary_unsupported_chunks = summary.dictionary_unsupported_chunks,
        dictionary_page_read_errors = summary.dictionary_page_read_errors,
        dictionary_predicate_probes = summary.dictionary_predicate_probes,
        row_groups_skipped_by_dictionary = summary.row_groups_skipped_by_dictionary,
        rows_skipped_by_dictionary = summary.rows_skipped_by_dictionary,
        estimated_compressed_bytes_skipped_by_dictionary = summary.estimated_compressed_bytes_skipped_by_dictionary,
        row_groups_selected_after_dictionary = summary.row_groups_selected_after_dictionary,
        row_group_pruning_us = summary.row_group_pruning_us,
        page_pruning_us = summary.page_pruning_us,
        page_header_pruning_us = summary.page_header_pruning_us,
        header_scan_us = summary.header_scan_us,
        bloom_pruning_us = summary.bloom_pruning_us,
        dictionary_load_us = summary.dictionary_load_us,
        dictionary_pruning_us = summary.dictionary_pruning_us,
        pushed_filter_count = scan.pushdown.map_or(0, |predicate| predicate.predicate_count),
        pushed_filter_stage_count = scan.pushdown.map_or(0, |predicate| predicate.stages.len()),
        adaptive_filter_active = adaptive_filter_snapshot.is_some(),
        adaptive_filter_stage_count = adaptive_filter_snapshot.as_ref().map_or(0, |snapshot| snapshot.stage_count),
        adaptive_filter_observations = adaptive_filter_snapshot.as_ref().map_or(0, |snapshot| snapshot.observations),
        adaptive_filter_swap_attempts = adaptive_filter_snapshot.as_ref().map_or(0, |snapshot| snapshot.swap_attempts),
        adaptive_filter_swaps_accepted = adaptive_filter_snapshot.as_ref().map_or(0, |snapshot| snapshot.accepted_swaps),
        adaptive_filter_swaps_reverted = adaptive_filter_snapshot.as_ref().map_or(0, |snapshot| snapshot.reverted_swaps),
        adaptive_filter_warmup = adaptive_filter_snapshot.as_ref().is_some_and(|snapshot| snapshot.warmup),
        adaptive_filter_order = ?adaptive_filter_snapshot.as_ref().map(|snapshot| &snapshot.permutation),
        pushed_filter_columns = ?scan.pushed_filter_columns,
        predicate_metrics_enabled = scan.detailed_metrics_enabled,
        predicate_rows_before_filter = predicate_metrics.rows_before_filter,
        predicate_batches = predicate_metrics.batches,
        predicate_rows_in = predicate_metrics.rows_in,
        predicate_rows_kept = predicate_metrics.rows_kept,
        predicate_eval_wall_us = predicate_metrics.eval_wall_us,
        predicate_input_array_bytes = predicate_metrics.input_array_bytes,
        output_source_array_count = scan.stream.output_source_array_count,
        output_source_array_bytes = scan.stream.output_source_array_bytes,
        runtime_statistics_enabled = scan.runtime_statistics_enabled,
        runtime_statistics_enqueue_us = scan.stream.statistics_enqueue_us,
        runtime_statistics_batches_queued = scan.stream.statistics_batches_queued,
        runtime_statistics_batches_queue_full = scan.stream.statistics_batches_queue_full,
        runtime_statistics_batches_too_large = scan.stream.statistics_batches_too_large,
        runtime_statistics_batches_worker_unavailable = scan.stream.statistics_batches_worker_unavailable,
        bytes_read = bytes_read_total,
        file_size_bytes = scan.file_size_bytes,
        stat_us = scan.stat_us,
        metadata_us = scan.metadata_us,
        build_us = scan.build_us,
        first_batch_us = scan.stream.first_batch_us,
        parquet_read_us = scan.stream.parquet_read_time.as_micros() as u64,
        first_poll_us = scan.stream.first_poll_time.as_micros() as u64,
        first_poll_io_us = scan.stream.first_poll_io_us,
        first_poll_thread_cpu_us = scan.stream.first_poll_thread_cpu.as_micros() as u64,
        first_poll_offcpu_us = first_poll_offcpu.as_micros() as u64,
        rest_poll_us = scan.stream.rest_poll_time.as_micros() as u64,
        poll_thread_cpu_us = scan.stream.poll_thread_cpu.as_micros() as u64,
        poll_offcpu_us = scan.stream.parquet_read_time.saturating_sub(scan.stream.poll_thread_cpu).as_micros() as u64,
        poll_count = scan.stream.poll_count,
        first_poll_polls = scan.stream.first_poll_polls,
        io_calls,
        io_ranges,
        io_read_us,
        io_wall_us,
        io_max_range_us,
        io_avg_bytes,
        downstream_us = scan.stream.downstream_time.as_micros() as u64,
        elapsed_us = elapsed.as_micros() as u64,
        rows_per_sec = (scan.stream.total_rows as f64 / elapsed.as_secs_f64().max(f64::MIN_POSITIVE)) as u64,
        "scan_completed"
    );
}
