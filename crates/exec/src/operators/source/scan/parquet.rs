use crate::pipeline::PipelineNode;
use crate::expression::eval::ExpressionKernelProvider;
use crate::runtime::filter::RuntimeFilterSet;
use crate::udf::FunctionRegistry;
use common::Result;
use common::protocol::RuntimeStatisticsScan;
use common::protocol::ScanPredicate;
use common::statistics::RuntimeStatisticsStore;
#[cfg(test)]
use common::statistics::runtime::{RUNTIME_STATISTIC_METRICS, runtime_metric_availability};
use error::VeinError;
use futures::Stream;
use opendal::Operator;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use std::collections::HashMap;
use std::future::poll_fn;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use types::VeinSchema;

use super::ScanFile;
use catalog::{MetadataKind, MetadataStore};

mod file;
mod batch_size;
mod metrics;
mod page_header_reader;
mod page_pruning;
mod predicate;
mod projection;
mod reader;

use batch_size::{RowGroupBatchSizePolicy, ScanBatchSizePolicy};
use file::prepare_parquet_file;
use metrics::ScanPredicateMetrics;
use page_header_reader::{
    PageHeaderReadRequest, PageHeaderReadResult, read_page_header_statistics,
};
use page_pruning::{
    attach_header_page_locations, page_header_audit_enabled, page_payload_pruning_metrics,
};
use predicate::{AdaptiveFilterOrder, ScanPredicateContext, build_scan_predicates};
use projection::{dictionary_schema_hint, scan_projection};
use reader::{IoStats, ParquetFileReader, ParquetIoBackend, thread_cpu_time};

/// Metadata kind under which this format publishes parsed Parquet footers.
///
/// The catalog that plans a scan and the scan itself must both use this
/// constant, so a footer parsed once is found by the other side. It moves to a
/// dedicated Parquet catalog crate together with the rest of this format.
pub const PARQUET_FOOTER: MetadataKind = MetadataKind::new("parquet.footer");

/// Borrowed inputs for one Parquet scan invocation.
pub struct ParquetScanRequest<'a> {
    /// Storage operator used to read the scan files.
    pub operator: &'a Operator,
    /// Files and optional row-group ranges assigned to this scan.
    pub files: &'a [ScanFile],
    /// Columns requested by the query plan.
    pub projected: &'a VeinSchema,
    /// Projected columns that may retain dictionary encoding.
    pub dictionary_columns: &'a [u32],
    /// Compiled predicates the reader may evaluate during scanning.
    pub pushdown: Option<&'a ScanPredicate>,
    /// Shared cache for format metadata such as Parquet footers.
    pub metadata_store: &'a MetadataStore,
    /// Functions available to compiled scan predicates.
    pub registry: &'a Arc<FunctionRegistry>,
    /// Shared typed kernels used by source predicates.
    pub expression_kernels: &'a Arc<dyn ExpressionKernelProvider>,
    /// Shared byte counter for storage reads performed by the scan.
    pub bytes_read: &'a Arc<AtomicU64>,
    /// Query-local filters produced by runtime operators.
    pub runtime_filters: &'a RuntimeFilterSet,
    /// Background collector for lazily computed runtime statistics.
    pub runtime_statistics: &'a RuntimeStatisticsStore,
    /// Optional scan identity and coverage information for statistics.
    pub statistics_scan: Option<&'a RuntimeStatisticsScan>,
    /// Shared statistics context for this query, when available.
    pub query_statistics: Option<&'a Arc<common::statistics::QueryStatisticsContext>>,
}

/// Executes a scan over a list of Parquet files
pub(super) async fn execute_parquet_scan(
    request: ParquetScanRequest<'_>,
    next: &mut dyn PipelineNode,
) -> Result<()> {
    let ParquetScanRequest {
        operator,
        files,
        projected,
        dictionary_columns,
        pushdown,
        metadata_store,
        registry,
        expression_kernels,
        bytes_read,
        runtime_filters,
        runtime_statistics,
        statistics_scan,
        query_statistics,
    } = request;
    let mut adaptive_order: Option<Arc<AdaptiveFilterOrder>> = None;
    let io_backend = ParquetIoBackend::configured();
    let batch_size_policy = RowGroupBatchSizePolicy::default();
    let batch_rows_override = std::env::var("VEIN_SCAN_BATCH_ROWS").ok();
    'files: for file in files {
        let path = file.relative.as_str();
        let scan_started = Instant::now();
        let detailed_metrics_enabled =
            tracing::enabled!(target: "vein.audit", tracing::Level::DEBUG);
        let predicate_metrics = Arc::new(ScanPredicateMetrics::default());
        let pushed_filter_columns = pushdown
            .map(|predicate| {
                let mut columns = Vec::new();
                for stage in &predicate.stages {
                    for column in &stage.input.fields {
                        if !columns.contains(&column.name.as_str()) {
                            columns.push(column.name.as_str());
                        }
                    }
                }
                columns
            })
            .unwrap_or_default();
        let runtime_filter_names = runtime_filters.names();
        tracing::debug!(
            target: "vein.audit",
            event = "scan_started",
            path = %path,
            format = "parquet",
            io_backend = io_backend.name(),
            pushed_filter_count = pushdown.map_or(0, |predicate| predicate.predicate_count),
            pushed_filter_stage_count = pushdown.map_or(0, |predicate| predicate.stages.len()),
            pushed_filter_columns = ?pushed_filter_columns,
            runtime_filter_count = runtime_filter_names.len(),
            runtime_filter_names = ?runtime_filter_names,
            detailed_metrics_enabled,
            "scan_started"
        );

        let bytes_before_file = bytes_read.load(Ordering::Relaxed);
        let io = Arc::new(IoStats::default());

        let prepared_file = prepare_parquet_file(
            operator,
            file,
            io_backend,
            bytes_read,
            Arc::clone(&io),
            metadata_store,
        )
            .await?;
        let reader = prepared_file.reader;
        let parquet_metadata = prepared_file.metadata;
        let file_size_bytes = prepared_file.file_size_bytes;
        let stat_us = prepared_file.stat_us;
        let metadata_started = prepared_file.metadata_started;
        let footer_hit = prepared_file.footer_hit;
        let options = if dictionary_columns.is_empty() {
            ArrowReaderOptions::new()
        } else {
            let default_metadata = ArrowReaderMetadata::try_new(
                Arc::clone(&parquet_metadata),
                ArrowReaderOptions::new(),
            )
                .map_err(|error| {
                    VeinError::internal(format!(
                        "Failed to infer Parquet schema for '{path}': {error}"
                    ))
                })?;
            let schema =
                dictionary_schema_hint(default_metadata.schema(), projected, dictionary_columns)?;
            ArrowReaderOptions::new().with_schema(schema)
        };
        let arrow_metadata =
            ArrowReaderMetadata::try_new(parquet_metadata, options).map_err(|error| {
                VeinError::internal(format!(
                    "Failed to configure Parquet reader for '{path}': {error}"
                ))
            })?;
        let mut builder =
            ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arrow_metadata);
        let reader_metrics = if detailed_metrics_enabled {
            ArrowReaderMetrics::enabled()
        } else {
            ArrowReaderMetrics::disabled()
        };
        builder = builder.with_metrics(reader_metrics.clone());

        let scan_projection =
            scan_projection(builder.schema(), projected, pushdown, runtime_filters, path)?;
        let output_mask = ProjectionMask::roots(
            builder.parquet_schema(),
            scan_projection.output_roots.clone(),
        );
        builder = builder.with_projection(output_mask);

        // Prune from this file's footer before Arrow applies staged row filters
        // and decodes projected output columns only for rows that pass.
        let row_groups_total = builder.metadata().num_row_groups();
        let row_group_pruning_started = Instant::now();
        let assigned_range = file.scan_unit_range.clone().unwrap_or(0..row_groups_total);
        let row_groups_assigned = assigned_range
            .end
            .min(row_groups_total)
            .saturating_sub(assigned_range.start.min(row_groups_total));
        let assigned_row_group_rows = (assigned_range.start.min(row_groups_total)
            ..assigned_range.end.min(row_groups_total))
            .map(|index| builder.metadata().row_group(index).num_rows());
        let batch_size =
            batch_size_policy.select(assigned_row_group_rows, batch_rows_override.as_deref());
        builder = builder.with_batch_size(batch_size.rows);
        if file
            .scan_unit_range
            .as_ref()
            .is_none_or(|range| range.start == 0)
        {
            tracing::debug!(
                target: "vein.scan_batch_size",
                event = "parquet_batch_size_selected",
                path = %path,
                batch_rows = batch_size.rows,
                source = batch_size.source.as_str(),
                assigned_row_groups = row_groups_assigned,
                largest_assigned_row_group_rows = batch_size.largest_assigned_row_group.unwrap_or(0),
                invalid_override = batch_size.invalid_override,
                "selected Parquet scan batch size for first range in file"
            );
        }
        let static_predicates =
            pushdown.map_or(&[][..], |predicate| predicate.statistics.as_slice());
        let mut statistics_predicates = static_predicates.to_vec();
        let static_statistics_predicate_count = static_predicates.len();
        let (runtime_filter_predicates, runtime_filter_diagnostics) =
            runtime_filters.statistics_predicates_with_diagnostics(projected);
        let runtime_statistics_predicates = runtime_filter_predicates.len();
        if !runtime_filter_diagnostics.is_empty() {
            tracing::info!(
                target: "vein.parquet_runtime_filter",
                event = "parquet_runtime_filter_pruning_path",
                path = %path,
                filters = ?runtime_filter_diagnostics,
                metadata_predicate_count = runtime_statistics_predicates,
                row_filter_names = ?runtime_filters.row_filter_names(),
                metadata_stage = "footer_and_page_index",
                row_filter_stage = "arrow_row_filter_after_metadata_pruning",
                "runtime filter capabilities at the Parquet scan boundary"
            );
        }
        statistics_predicates.extend(runtime_filter_predicates);
        let bloom_columns = crate::parquet_statistics::bloom_filter_columns(
            builder.schema().as_ref(),
            builder.metadata(),
            &statistics_predicates,
        );
        let dictionary_predicates =
            crate::parquet_statistics::dictionary_predicates(&statistics_predicates);
        let dictionary_columns = bloom_columns
            .iter()
            .filter(|column| {
                dictionary_predicates
                    .iter()
                    .any(|predicate| predicate.column == column.name)
            })
            .collect::<Vec<_>>();
        let row_group_pruning = crate::parquet_statistics::prune_row_groups_in_range(
            builder.schema().as_ref(),
            builder.metadata(),
            &statistics_predicates,
            assigned_range,
        );
        tracing::debug!(
            target: "vein.audit",
            event = "parquet_predicate_pruning",
            path = %path,
            stage = "row_group_footer",
            predicates = ?row_group_pruning.predicate_diagnostics.predicates,
            "per-predicate Parquet footer pruning decisions"
        );
        let row_groups_selected = row_group_pruning.selected.len();
        let row_groups_skipped = row_groups_assigned.saturating_sub(row_groups_selected);
        let row_groups_skipped_rows = row_group_pruning.skipped_rows;
        let estimated_compressed_bytes_skipped =
            row_group_pruning.estimated_compressed_bytes_skipped;
        let page_pruning_started = Instant::now();
        let page_index_available = builder.metadata().page_index().is_some();
        let page_index_pruning = crate::parquet_statistics::prune_data_pages(
            builder.schema().as_ref(),
            builder.metadata(),
            &statistics_predicates,
            &row_group_pruning.selected,
        );
        tracing::debug!(
            target: "vein.audit",
            event = "parquet_predicate_pruning",
            path = %path,
            stage = "page_index",
            predicates = ?page_index_pruning.predicate_diagnostics.predicates,
            "per-predicate Parquet page-index pruning decisions"
        );
        let row_groups_selected_after_page_index = page_index_pruning.selections.len();
        let row_groups_skipped_by_page_index = page_index_pruning.row_groups_skipped;
        let page_index_columns = page_index_pruning.indexed_columns;
        let page_bounds_complete = page_index_pruning.page_bounds_complete;
        let page_bounds_incomplete = page_index_pruning.page_bounds_incomplete;
        let page_ranges_considered = page_index_pruning.row_ranges_considered;
        let page_ranges_skipped = page_index_pruning.row_ranges_skipped;
        let page_rows_skipped = page_index_pruning.rows_skipped;

        let header_row_groups = page_index_pruning
            .selections
            .iter()
            .map(|selection| selection.row_group_index())
            .collect::<Vec<_>>();
        let audit_page_headers = page_header_audit_enabled();
        let mut page_header_plan = crate::parquet_statistics::page_header_requests_with_audit(
            &statistics_predicates,
            &header_row_groups,
            page_index_available,
            &page_index_pruning,
            audit_page_headers,
        );
        page_header_plan.merge(crate::parquet_statistics::dictionary_page_header_requests(
            builder.metadata(),
            &header_row_groups,
            dictionary_columns.iter().copied(),
        ));
        let page_header_started = Instant::now();
        let page_header_result = if page_header_plan.is_empty() {
            PageHeaderReadResult::default()
        } else {
            read_page_header_statistics(PageHeaderReadRequest {
                operator,
                path,
                absolute_path: &file.absolute,
                backend: io_backend,
                file_size_bytes,
                bytes_read,
                io: &io,
                arrow_schema: builder.schema().as_ref(),
                metadata: builder.metadata(),
                plan: &page_header_plan,
            })
                .await
        };
        let header_pages_loaded = page_header_result.metrics.pages_scanned;
        let header_pages_with_statistics = page_header_result.metrics.pages_with_statistics;
        let header_pages_with_complete_bounds =
            page_header_result.metrics.pages_with_complete_bounds;
        let header_bytes_read = page_header_result.metrics.probe_bytes;
        let header_bytes_parsed = page_header_result.metrics.header_bytes;
        let header_chunks_scanned = page_header_result.metrics.chunks_scanned;
        let header_chunks_failed = page_header_result.metrics.failed_chunks;
        let header_chunks_unsupported_row_mapping =
            page_header_result.metrics.unsupported_row_mapping_chunks;
        let header_scan_us = page_header_result.metrics.elapsed_us;
        let mut page_pruning = if page_header_result.pages.is_empty() {
            page_index_pruning
        } else {
            crate::parquet_statistics::prune_data_pages_with_headers(
                builder.schema().as_ref(),
                builder.metadata(),
                &statistics_predicates,
                &row_group_pruning.selected,
                &page_header_result.pages,
            )
        };
        let row_groups_selected_after_page_header = page_pruning.selections.len();
        let row_groups_skipped_by_page_header = row_groups_selected_after_page_index
            .saturating_sub(row_groups_selected_after_page_header);
        let page_header_rows_skipped = page_pruning.rows_skipped.saturating_sub(page_rows_skipped);
        if audit_page_headers {
            tracing::info!(
                target: "vein.parquet_audit",
                event = "parquet_page_header_audit_summary",
                path = %path,
                predicates = ?page_pruning.predicate_diagnostics.predicates,
                page_index_columns = page_index_columns,
                page_bounds_complete,
                page_bounds_incomplete,
                columns_requested = page_header_result.metrics.columns_requested,
                column_row_group_pairs_requested = page_header_result.metrics.column_row_group_pairs_requested,
                chunks_scanned = header_chunks_scanned,
                chunks_failed = header_chunks_failed,
                chunks_unsupported_row_mapping = header_chunks_unsupported_row_mapping,
                pages_scanned = header_pages_loaded,
                pages_with_statistics = header_pages_with_statistics,
                pages_with_complete_bounds = header_pages_with_complete_bounds,
                probe_bytes_read = header_bytes_read,
                header_bytes_parsed = header_bytes_parsed,
                pages_skipped_rows = page_header_rows_skipped,
                row_groups_skipped = row_groups_skipped_by_page_header,
                "opt-in Parquet page-header audit summary"
            );
        }
        tracing::debug!(
            target: "vein.audit",
            event = "parquet_predicate_pruning",
            path = %path,
            stage = "page_header",
            audit_forced = audit_page_headers,
            columns_requested = page_header_result.metrics.columns_requested,
            column_row_group_pairs_requested = page_header_result.metrics.column_row_group_pairs_requested,
            chunks_scanned = header_chunks_scanned,
            chunks_failed = header_chunks_failed,
            chunks_unsupported_row_mapping = header_chunks_unsupported_row_mapping,
            pages_scanned = header_pages_loaded,
            pages_with_statistics = header_pages_with_statistics,
            pages_with_complete_bounds = header_pages_with_complete_bounds,
            probe_bytes_read = header_bytes_read,
            header_bytes_parsed = header_bytes_parsed,
            probe_overread_bytes = header_bytes_read.saturating_sub(header_bytes_parsed),
            pages_skipped_rows = page_header_rows_skipped,
            row_groups_skipped = row_groups_skipped_by_page_header,
            predicates = ?page_pruning.predicate_diagnostics.predicates,
            "Parquet data-page header pruning decisions"
        );
        let page_pruning_us = page_pruning_started.elapsed().as_micros() as u64;
        let page_header_pruning_us = page_header_started.elapsed().as_micros() as u64;

        // Load filters only for groups that survived footer and page-index
        // pruning. Bloom filters are optional metadata: absent or unreadable
        // filters leave the affected group selected.
        let static_bloom_equality_columns =
            crate::parquet_statistics::equality_predicate_columns(static_predicates);
        let runtime_bloom_equality_columns = crate::parquet_statistics::equality_predicate_columns(
            &statistics_predicates[static_statistics_predicate_count..],
        );
        let bloom_row_groups_considered = page_pruning.selections.len() as u64;
        let bloom_filter_columns = bloom_columns.len();
        let mut bloom_filters_available = 0_u64;
        let mut bloom_filters_loaded = 0_u64;
        let mut bloom_filter_read_errors = 0_u64;
        let mut bloom_filter_chunks_checked = 0_u64;
        let mut bloom_filter_chunks_without_metadata = 0_u64;
        let mut bloom_filter_column_metadata_missing = 0_u64;
        let mut bloom_filter_bytes = 0_u64;
        let bloom_filter_load_started = Instant::now();
        let mut row_group_bloom_filters = HashMap::new();
        for selection in &page_pruning.selections {
            let row_group_index = selection.row_group_index();
            let mut filters = HashMap::with_capacity(bloom_columns.len());
            for column in &bloom_columns {
                let row_group = builder.metadata().row_group(row_group_index);
                let Some(metadata) = row_group.columns().get(column.parquet_column_index) else {
                    bloom_filter_column_metadata_missing =
                        bloom_filter_column_metadata_missing.saturating_add(1);
                    continue;
                };
                bloom_filter_chunks_checked = bloom_filter_chunks_checked.saturating_add(1);
                let bloom_filter_length = metadata
                    .bloom_filter_length()
                    .and_then(|length| u64::try_from(length).ok());
                if metadata.bloom_filter_offset().is_none() {
                    bloom_filter_chunks_without_metadata =
                        bloom_filter_chunks_without_metadata.saturating_add(1);
                    continue;
                }
                bloom_filters_available = bloom_filters_available.saturating_add(1);
                match builder
                    .get_row_group_column_bloom_filter(row_group_index, column.parquet_column_index)
                    .await
                {
                    Ok(Some(filter)) => {
                        bloom_filters_loaded = bloom_filters_loaded.saturating_add(1);
                        let decoded_bytes = u64::try_from(filter.num_blocks())
                            .unwrap_or(u64::MAX)
                            .saturating_mul(32);
                        let serialized_bytes = bloom_filter_length.unwrap_or(decoded_bytes);
                        bloom_filter_bytes = bloom_filter_bytes.saturating_add(serialized_bytes);
                        filters.insert(
                            column.name.clone(),
                            crate::parquet_statistics::ParquetBloomFilter::new(
                                column.clone(),
                                filter,
                            ),
                        );
                    }
                    Ok(None) => {}
                    Err(error) => {
                        bloom_filter_read_errors = bloom_filter_read_errors.saturating_add(1);
                        tracing::debug!(
                            target: "vein.audit",
                            event = "parquet_bloom_filter_unavailable",
                            path = %path,
                            row_group = row_group_index,
                            column = %column.name,
                            error = %error,
                            "ignoring unavailable Parquet Bloom filter"
                        );
                    }
                }
            }
            if !filters.is_empty() {
                row_group_bloom_filters.insert(row_group_index, filters);
            }
        }
        let bloom_filter_load_us = bloom_filter_load_started.elapsed().as_micros() as u64;
        let bloom_filter_eligibility_reason = if static_bloom_equality_columns.is_empty()
            && runtime_bloom_equality_columns.is_empty()
        {
            "no_equality_predicate_columns"
        } else if bloom_filter_columns == 0 {
            "equality_columns_not_mappable_to_parquet_schema"
        } else if bloom_filters_available == 0 {
            "no_column_chunk_bloom_metadata"
        } else if bloom_filters_loaded == 0 {
            "column_chunk_bloom_filters_unavailable_or_unreadable"
        } else {
            "column_chunk_bloom_filters_loaded"
        };
        tracing::info!(
            target: "vein.parquet_bloom",
            event = "parquet_bloom_filter_eligibility",
            path = %path,
            static_equality_columns = ?static_bloom_equality_columns,
            runtime_equality_columns = ?runtime_bloom_equality_columns,
            mapped_columns = bloom_filter_columns,
            row_groups_considered = bloom_row_groups_considered,
            chunks_checked = bloom_filter_chunks_checked,
            chunks_without_bloom_metadata = bloom_filter_chunks_without_metadata,
            column_metadata_missing = bloom_filter_column_metadata_missing,
            filters_available = bloom_filters_available,
            filters_loaded = bloom_filters_loaded,
            read_errors = bloom_filter_read_errors,
            eligibility_reason = bloom_filter_eligibility_reason,
            "why Parquet column-chunk Bloom pruning is or is not available"
        );
        let bloom_pruning_started = Instant::now();
        let mut bloom_pruning = crate::parquet_statistics::prune_row_groups_with_bloom(
            builder.metadata(),
            &statistics_predicates,
            std::mem::take(&mut page_pruning.selections),
            &row_group_bloom_filters,
        );
        let row_groups_selected_after_bloom = bloom_pruning.selections.len();
        let row_groups_skipped_by_bloom = bloom_pruning.row_groups_skipped;
        let rows_skipped_by_bloom = bloom_pruning.rows_skipped;
        let estimated_compressed_bytes_skipped_by_bloom =
            bloom_pruning.estimated_compressed_bytes_skipped;
        let bloom_predicate_probes = bloom_pruning.probes;
        let bloom_pruning_us = bloom_pruning_started.elapsed().as_micros() as u64;

        // Decode each dictionary once. Whole-row-group elimination requires all
        // data pages to use it; mixed chunks use the result only on matching pages.
        let mut dictionary_chunks_considered = 0_u64;
        let mut dictionary_chunks_eligible = 0_u64;
        let mut dictionary_pages_loaded = 0_u64;
        let mut dictionary_page_read_errors = 0_u64;
        let mut dictionary_unsupported_chunks = 0_u64;
        let mut dictionary_page_bytes = 0_u64;
        let mut dictionary_chunks_fully_encoded = 0_u64;
        let mut dictionary_chunks_page_filtered = 0_u64;
        let dictionary_load_started = Instant::now();
        let mut dictionary_reader = if dictionary_predicates.is_empty()
            || bloom_pruning.selections.is_empty()
        {
            None
        } else {
            match ParquetFileReader::new(
                operator,
                path,
                &file.absolute,
                io_backend,
                file_size_bytes,
                Arc::clone(bytes_read),
                Arc::clone(&io),
            )
                .await
            {
                Ok(reader) => Some(reader),
                Err(error) => {
                    dictionary_page_read_errors = dictionary_page_read_errors.saturating_add(1);
                    tracing::debug!(
                        target: "vein.audit",
                        event = "parquet_dictionary_reader_unavailable",
                        path = %path,
                        backend = io_backend.name(),
                        error = %error,
                        "dictionary pruning unavailable; retaining candidate rows"
                    );
                    None
                }
            }
        };
        let mut dictionary_membership = HashMap::new();
        let mut dictionary_page_membership = HashMap::new();
        for selection in &bloom_pruning.selections {
            let row_group_index = selection.row_group_index();
            let row_group = builder.metadata().row_group(row_group_index);
            let mut outcomes = vec![None; dictionary_predicates.len()];
            let mut page_outcomes = vec![None; dictionary_predicates.len()];
            for column in &dictionary_columns {
                let column_predicates = dictionary_predicates
                    .iter()
                    .filter(|predicate| predicate.column == column.name)
                    .cloned()
                    .collect::<Vec<_>>();
                if column_predicates.is_empty() {
                    continue;
                }
                dictionary_chunks_considered = dictionary_chunks_considered.saturating_add(1);
                let Some(chunk) = row_group.columns().get(column.parquet_column_index) else {
                    dictionary_unsupported_chunks = dictionary_unsupported_chunks.saturating_add(1);
                    continue;
                };
                let fully_dictionary_encoded =
                    crate::parquet_statistics::is_fully_dictionary_encoded(chunk);
                if !fully_dictionary_encoded
                    && !crate::parquet_statistics::has_dictionary_encoded_page(
                    &page_header_result.pages,
                    row_group_index,
                    &column.name,
                )
                {
                    continue;
                }
                let Some(range) =
                    crate::parquet_statistics::dictionary_page_range(chunk, file_size_bytes)
                else {
                    dictionary_unsupported_chunks = dictionary_unsupported_chunks.saturating_add(1);
                    continue;
                };
                dictionary_chunks_eligible = dictionary_chunks_eligible.saturating_add(1);
                if fully_dictionary_encoded {
                    dictionary_chunks_fully_encoded =
                        dictionary_chunks_fully_encoded.saturating_add(1);
                } else {
                    dictionary_chunks_page_filtered =
                        dictionary_chunks_page_filtered.saturating_add(1);
                }
                let Some(dictionary_reader) = dictionary_reader.as_mut() else {
                    continue;
                };
                let page_bytes = match dictionary_reader.get_bytes(range.clone()).await {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        dictionary_page_read_errors = dictionary_page_read_errors.saturating_add(1);
                        tracing::debug!(
                            target: "vein.audit",
                            event = "parquet_dictionary_page_unavailable",
                            path = %path,
                            row_group = row_group_index,
                            column = %column.name,
                            range_start = range.start,
                            range_end = range.end,
                            error = %error,
                            "retaining row group because dictionary page could not be read"
                        );
                        continue;
                    }
                };
                dictionary_pages_loaded = dictionary_pages_loaded.saturating_add(1);
                dictionary_page_bytes = dictionary_page_bytes
                    .saturating_add(u64::try_from(page_bytes.len()).unwrap_or(u64::MAX));
                let row_count = usize::try_from(row_group.num_rows()).unwrap_or(0);
                let matches = crate::parquet_statistics::dictionary_page_predicate_matches(
                    page_bytes,
                    file_size_bytes,
                    row_count,
                    chunk,
                    column,
                    &column_predicates,
                );
                for (predicate, outcome) in column_predicates.iter().zip(matches) {
                    page_outcomes[predicate.index] = outcome;
                    if fully_dictionary_encoded {
                        outcomes[predicate.index] = outcome;
                    }
                    if outcome.is_none() {
                        dictionary_unsupported_chunks =
                            dictionary_unsupported_chunks.saturating_add(1);
                    }
                }
            }
            dictionary_membership.insert(row_group_index, outcomes);
            dictionary_page_membership.insert(row_group_index, page_outcomes);
        }
        let dictionary_load_us = dictionary_load_started.elapsed().as_micros() as u64;
        let dictionary_pruning_started = Instant::now();
        let mut dictionary_pruning = crate::parquet_statistics::prune_row_groups_with_dictionary(
            builder.metadata(),
            std::mem::take(&mut bloom_pruning.selections),
            &dictionary_membership,
        );
        let mut dictionary_page_pruning = crate::parquet_statistics::prune_dictionary_encoded_pages(
            std::mem::take(&mut dictionary_pruning.selections),
            &page_header_result.pages,
            &dictionary_predicates,
            &dictionary_page_membership,
        );
        dictionary_page_pruning.selections = attach_header_page_locations(
            std::mem::take(&mut dictionary_page_pruning.selections),
            &page_header_result.page_locations,
        );
        let page_payload_metrics = page_payload_pruning_metrics(
            &dictionary_page_pruning.selections,
            &page_header_result.page_locations,
        );
        tracing::info!(
            target: "vein.parquet_dictionary",
            event = "parquet_dictionary_data_page_pruning",
            path = %path,
            chunks_fully_dictionary_encoded = dictionary_chunks_fully_encoded,
            chunks_page_filtered = dictionary_chunks_page_filtered,
            predicate_page_checks = dictionary_page_pruning.predicate_page_checks,
            dictionary_pages_proven_absent = dictionary_page_pruning.dictionary_pages_proven_absent,
            rows_skipped = dictionary_page_pruning.rows_skipped,
            row_groups_emptied = dictionary_page_pruning.row_groups_emptied,
            estimated_payload_bytes_skipped = page_payload_metrics.skipped_payload_bytes,
            "dictionary membership applied to dictionary-encoded data pages"
        );
        tracing::debug!(
            target: "vein.audit",
            event = "parquet_page_payload_pruning",
            path = %path,
            columns_with_locations = page_payload_metrics.columns_with_locations,
            data_pages_available = page_payload_metrics.data_pages_available,
            data_pages_selected = page_payload_metrics.data_pages_selected,
            data_pages_skipped = page_payload_metrics.data_pages_skipped,
            selected_payload_bytes_estimate = page_payload_metrics.selected_payload_bytes,
            skipped_payload_bytes_estimate = page_payload_metrics.skipped_payload_bytes,
            "estimated Parquet page payload omitted by row selection"
        );
        let dictionary_predicate_probes = dictionary_pruning.probes;
        let row_groups_skipped_by_dictionary = dictionary_pruning.row_groups_skipped;
        let rows_skipped_by_dictionary = dictionary_pruning.rows_skipped;
        let estimated_compressed_bytes_skipped_by_dictionary =
            dictionary_pruning.estimated_compressed_bytes_skipped;
        let dictionary_pruning_us = dictionary_pruning_started.elapsed().as_micros() as u64;
        tracing::debug!(
            target: "vein.audit",
            event = "parquet_dictionary_pruning",
            path = %path,
            predicates = dictionary_predicates.len(),
            columns = dictionary_columns.len(),
            chunks_considered = dictionary_chunks_considered,
            chunks_fully_dictionary_encoded = dictionary_chunks_eligible,
            pages_loaded = dictionary_pages_loaded,
            page_bytes = dictionary_page_bytes,
            unsupported_or_missing = dictionary_unsupported_chunks,
            read_errors = dictionary_page_read_errors,
            predicate_probes = dictionary_predicate_probes,
            row_groups_skipped = row_groups_skipped_by_dictionary,
            rows_skipped = rows_skipped_by_dictionary,
            estimated_compressed_bytes_skipped =
                estimated_compressed_bytes_skipped_by_dictionary,
            load_us = dictionary_load_us,
            prune_us = dictionary_pruning_us,
            "Parquet dictionary page pruning decisions"
        );
        let row_groups_selected_after_dictionary = dictionary_page_pruning.selections.len();
        let has_scan_selection = file.scan_unit_range.is_some()
            || row_groups_selected != row_groups_total
            || page_rows_skipped != 0
            || row_groups_skipped_by_bloom != 0
            || row_groups_skipped_by_dictionary != 0
            || dictionary_page_pruning.rows_skipped != 0
            // Preserve header-derived page locations even when metadata cannot
            // prune rows yet; a runtime filter may produce a selection before
            // later filter stages or projected columns are fetched.
            || !page_header_result.page_locations.is_empty();
        if has_scan_selection {
            if dictionary_page_pruning.selections.is_empty() {
                builder = builder.with_row_groups(Vec::new());
            } else {
                builder = builder.with_row_group_selections(dictionary_page_pruning.selections);
            }
        }
        let row_group_pruning_us = row_group_pruning_started.elapsed().as_micros() as u64;

        // Parquet evaluates narrow predicates first, then decodes only selected
        // rows into the final output projection.
        let mut predicate_pipeline = build_scan_predicates(ScanPredicateContext {
            arrow_schema: builder.schema(),
            parquet_schema: builder.parquet_schema(),
            path,
            projected,
            pushdown,
            registry,
            expression_kernels,
            detailed_metrics_enabled,
            predicate_metrics: &predicate_metrics,
            runtime_filters,
            adaptive_order: adaptive_order.clone(),
        })?;
        adaptive_order = predicate_pipeline.adaptive_order.clone();
        let stage_metrics = predicate_pipeline.stage_metrics.clone();
        let has_row_filter = predicate_pipeline.has_row_filter;
        if let Some(row_filter) = predicate_pipeline.row_filter.take() {
            builder = builder.with_row_filter(row_filter);
        }
        let metadata_us = metadata_started.elapsed().as_micros() as u64;
        let file_rows = builder.metadata().file_metadata().num_rows();

        let build_started = Instant::now();
        let mut stream = builder
            .build()
            .map_err(|e| VeinError::internal(format!("Failed to build parquet stream: {}", e)))?;
        let build_us = build_started.elapsed().as_micros() as u64;

        tracing::info!(
            target: "vein.parquet_audit",
            event = "scan_ready",
            path = %path,
            format = "parquet",
            read_projection_roots = ?scan_projection.read_roots,
            output_projection_positions = ?scan_projection.output_positions,
            predicate_handoff = "parquet_row_filter_late_materialization",
            late_materialization = true,
            post_decode_row_gather = false,
            footer_hit,
            file_size_bytes,
            row_groups_total,
            row_groups_assigned,
            row_groups_selected,
            row_groups_skipped,
            row_groups_skipped_rows,
            static_statistics_predicates = static_statistics_predicate_count,
            runtime_statistics_predicates,
            estimated_compressed_bytes_skipped,
            page_index_available,
            page_index_columns,
            page_bounds_complete,
            page_bounds_incomplete,
            page_ranges_considered,
            page_ranges_skipped,
            page_rows_skipped,
            row_groups_skipped_by_page_index,
            row_groups_selected_after_page_index,
            bloom_filter_columns,
            bloom_filters_available,
            bloom_filters_loaded,
            bloom_filter_read_errors,
            bloom_filter_bytes,
            bloom_filter_load_us,
            bloom_predicate_probes,
            row_groups_skipped_by_bloom,
            rows_skipped_by_bloom,
            estimated_compressed_bytes_skipped_by_bloom,
            row_groups_selected_after_bloom,
            dictionary_predicates = dictionary_predicates.len(),
            dictionary_columns = dictionary_columns.len(),
            dictionary_chunks_considered,
            dictionary_chunks_eligible,
            dictionary_pages_loaded,
            dictionary_page_bytes,
            dictionary_unsupported_chunks,
            dictionary_page_read_errors,
            dictionary_predicate_probes,
            row_groups_skipped_by_dictionary,
            rows_skipped_by_dictionary,
            estimated_compressed_bytes_skipped_by_dictionary,
            row_groups_selected_after_dictionary,
            row_group_pruning_us,
            page_pruning_us,
            bloom_pruning_us,
            dictionary_load_us,
            dictionary_pruning_us,
            file_rows,
            pushed_filter_count = pushdown.map_or(0, |predicate| predicate.predicate_count),
            pushed_filter_stage_count = pushdown.map_or(0, |predicate| predicate.stages.len()),
            pushed_filter_columns = ?pushed_filter_columns,
            runtime_filter_count = runtime_filter_names.len(),
            runtime_filter_names = ?runtime_filter_names,
            runtime_filter_bytes = runtime_filters.memory_bytes(),
            detailed_metrics_enabled,
            stat_us,
            metadata_us,
            build_us,
            "scan_ready"
        );

        let mut total_rows = 0usize;
        let mut physical_rows_read = 0usize;
        let mut batches = 0u64;
        let mut output_source_array_count = 0u64;
        let mut output_source_array_bytes = 0u64;
        let mut parquet_read_time = Duration::ZERO;
        let mut first_poll_time = Duration::ZERO;
        let mut rest_poll_time = Duration::ZERO;
        let mut downstream_time = Duration::ZERO;
        let mut first_batch_us = 0u64;
        let mut poll_thread_cpu = Duration::ZERO;
        let mut poll_count = 0u64;
        let mut first_poll_io_us = 0u64;
        let mut first_poll_thread_cpu = Duration::ZERO;
        let mut first_poll_polls = 0u64;
        let mut statistics_enqueue_us = 0u64;
        let mut statistics_batches_queued = 0u64;
        let mut statistics_batches_queue_full = 0u64;
        let mut statistics_batches_too_large = 0u64;
        let mut statistics_batches_worker_unavailable = 0u64;
        let io_wall_before_first_poll = io.fetch_wall_us.load(Ordering::Relaxed);
        let mut last_progress = Instant::now();
        loop {
            // CPU burned inside synchronous polls of the stream. Sampling inside
            // the closure keeps each reading on the thread that did the work: the
            // task may be rescheduled between polls, so a pair around the await
            // would subtract two different threads' clocks.
            let mut poll_cpu = Duration::ZERO;
            let mut polls = 0u64;
            let read_started = Instant::now();
            let next_batch = poll_fn(|cx| {
                let cpu_started = thread_cpu_time();
                let polled = Pin::new(&mut stream).poll_next(cx);
                poll_cpu += thread_cpu_time().saturating_sub(cpu_started);
                polls += 1;
                polled
            })
                .await;
            let poll_time = read_started.elapsed();
            poll_thread_cpu += poll_cpu;
            poll_count += polls;
            parquet_read_time += poll_time;
            if batches == 0 {
                first_poll_time = poll_time;
                first_poll_thread_cpu = poll_cpu;
                first_poll_polls = polls;
                first_poll_io_us = io
                    .fetch_wall_us
                    .load(Ordering::Relaxed)
                    .saturating_sub(io_wall_before_first_poll);
            } else {
                rest_poll_time += poll_time;
            }
            let Some(batch_result) = next_batch else {
                break;
            };
            let batch = batch_result
                .map_err(|e| VeinError::internal(format!("Parquet read error: {}", e)))?;
            let view = crate::pipeline::arrow_adapter::from_record_batch(batch)?;
            if batches == 0 {
                first_batch_us = scan_started.elapsed().as_micros() as u64;
            }
            total_rows = total_rows.saturating_add(view.num_rows());
            if !has_row_filter {
                physical_rows_read = physical_rows_read.saturating_add(view.num_rows());
            }
            batches += 1;

            if let Some(scan) = statistics_scan {
                let statistics_started = Instant::now();
                let statistics_view = view.project(&scan_projection.output_positions)?;
                let statistics_schema = statistics_view
                    .schema()
                    .cloned()
                    .ok_or_else(|| VeinError::internal("Parquet statistics view is missing its schema"))?;
                let retained_bytes = statistics_view
                    .source_array_memory_sizes()?
                    .into_iter()
                    .try_fold(0usize, |total, (_, bytes)| {
                        total.checked_add(bytes).ok_or_else(|| {
                            VeinError::internal("runtime statistics retained-byte count overflow")
                        })
                    })?;
                let outcome = runtime_statistics.enqueue_input(
                    scan.scan_id,
                    &scan.table_key,
                    scan.expected_rows,
                    scan.partition_count,
                    scan.row_coverage_candidate,
                    statistics_schema,
                    statistics_view,
                    retained_bytes,
                );
                match outcome {
                    common::statistics::runtime::RuntimeStatisticsEnqueue::Queued => {
                        statistics_batches_queued += 1;
                    }
                    common::statistics::runtime::RuntimeStatisticsEnqueue::QueueFull => {
                        statistics_batches_queue_full += 1;
                    }
                    common::statistics::runtime::RuntimeStatisticsEnqueue::BatchTooLarge => {
                        statistics_batches_too_large += 1;
                    }
                    common::statistics::runtime::RuntimeStatisticsEnqueue::WorkerUnavailable => {
                        statistics_batches_worker_unavailable += 1;
                    }
                }
                statistics_enqueue_us = statistics_enqueue_us
                    .saturating_add(statistics_started.elapsed().as_micros() as u64);
            }

            if view.num_rows() != 0 {
                let chunk = view.project(&scan_projection.output_positions)?;
                tracing::trace!(
                    rows = chunk.num_rows(),
                    "Pushing selected batch down pipeline"
                );
                if detailed_metrics_enabled {
                    let (array_count, array_bytes) = chunk.source_array_footprint();
                    output_source_array_count =
                        output_source_array_count.saturating_add(array_count as u64);
                    output_source_array_bytes =
                        output_source_array_bytes.saturating_add(array_bytes as u64);
                    tracing::trace!(
                        target: "vein.audit",
                        event = "scan_view_batch_emitted",
                        rows = chunk.num_rows(),
                        columns = chunk.num_columns(),
                        source_array_count = array_count,
                        source_array_bytes = array_bytes,
                        representation = "DataChunkView",
                        arrow_array_handles_moved = true,
                        "scan_view_batch_emitted"
                    );
                }
                let downstream_started = Instant::now();
                next.push(chunk)?;
                downstream_time += downstream_started.elapsed();

                // A limit below the pipeline has every row it will emit.
                if next.finished() {
                    break;
                }
            }

            if last_progress.elapsed() >= Duration::from_secs(10) {
                tracing::info!(
                    target: "vein.audit",
                    event = "parquet_scan_progress",
                    path = %path,
                    rows = total_rows,
                    batches,
                    bytes_read = bytes_read
                        .load(Ordering::Relaxed)
                        .saturating_sub(bytes_before_file),
                    file_size_bytes,
                    parquet_read_us = parquet_read_time.as_micros() as u64,
                    downstream_us = downstream_time.as_micros() as u64,
                    elapsed_us = scan_started.elapsed().as_micros() as u64,
                    "parquet_scan_progress"
                );
                last_progress = Instant::now();
            }
        }

        if has_row_filter {
            physical_rows_read =
                usize::try_from(predicate_metrics.rows_before_filter.load(Ordering::Relaxed))
                    .unwrap_or(usize::MAX);
        }
        let elapsed = scan_started.elapsed();
        let bytes_read_total = bytes_read
            .load(Ordering::Relaxed)
            .saturating_sub(bytes_before_file);
        let io_ranges = io.ranges.load(Ordering::Relaxed);
        let io_calls = io.calls.load(Ordering::Relaxed);
        let io_read_us = io.read_us.load(Ordering::Relaxed);
        let io_max_range_us = io.max_range_us.load(Ordering::Relaxed);
        let io_wall_us = io.fetch_wall_us.load(Ordering::Relaxed);
        let io_avg_bytes = bytes_read_total.checked_div(io_ranges).unwrap_or(0);
        let first_poll_offcpu = first_poll_time.saturating_sub(first_poll_thread_cpu);
        let predicate_metrics = predicate_metrics.snapshot();
        let adaptive_filter_snapshot = adaptive_order.as_ref().map(|policy| policy.snapshot());
        if let Some(snapshot) = &adaptive_filter_snapshot {
            snapshot.log(&path);
        }
        if detailed_metrics_enabled {
            for (stage_index, predicate_count, input_columns, metrics) in stage_metrics {
                let metrics = metrics.snapshot();
                tracing::debug!(target: "vein.audit", event = "scan_predicate_stage_completed",
                    path = %path, stage_index, predicate_count, input_columns = ?input_columns,
                    batches = metrics.batches, rows_in = metrics.rows_in, rows_kept = metrics.rows_kept,
                    eval_wall_us = metrics.eval_wall_us,
                    input_array_bytes_seen = metrics.input_array_bytes,
                    "scan predicate stage completed");
            }
            tracing::debug!(target: "vein.audit", event = "parquet_column_decode_metrics",
                path = %path, columns = ?reader_metrics.column_metrics(),
                records_decoded = ?reader_metrics.records_read_from_inner(),
                records_reused = ?reader_metrics.records_read_from_cache(),
                timing_scope = "sum_of_leaf_reader_call_elapsed_times;decompression_is_nested",
                "Parquet column decoding completed");
        }
        tracing::debug!(
            target: "vein.statistics",
            event = "scan_completed",
            path = %path,
            format = "parquet",
            footer_hit,
            rows = total_rows,
            rows_decoded = physical_rows_read,
            file_rows,
            batches,
            row_groups_total,
            row_groups_assigned,
            row_groups_selected,
            row_groups_skipped,
            row_groups_selected_after_page_index,
            row_groups_skipped_by_page_index,
            row_groups_selected_after_page_header,
            row_groups_skipped_by_page_header,
            row_groups_skipped_rows,
            estimated_compressed_bytes_skipped,
            page_index_available,
            page_index_columns,
            page_bounds_complete,
            page_bounds_incomplete,
            page_ranges_considered,
            page_ranges_skipped,
            page_rows_skipped,
            page_header_columns = page_header_result.metrics.columns_requested,
            page_header_column_row_group_pairs = page_header_result.metrics.column_row_group_pairs_requested,
            header_chunks_scanned,
            header_chunks_failed,
            header_chunks_unsupported_row_mapping,
            header_pages_loaded,
            header_pages_with_statistics,
            header_pages_with_complete_bounds,
            header_probe_bytes = header_bytes_read,
            header_bytes_parsed,
            header_probe_overread_bytes = header_bytes_read.saturating_sub(header_bytes_parsed),
            page_header_rows_skipped,
            bloom_filter_columns,
            bloom_filters_available,
            bloom_filters_loaded,
            bloom_filter_read_errors,
            bloom_filter_bytes,
            bloom_filter_load_us,
            bloom_predicate_probes,
            row_groups_skipped_by_bloom,
            rows_skipped_by_bloom,
            estimated_compressed_bytes_skipped_by_bloom,
            row_groups_selected_after_bloom,
            dictionary_predicates = dictionary_predicates.len(),
            dictionary_columns = dictionary_columns.len(),
            dictionary_chunks_considered,
            dictionary_chunks_eligible,
            dictionary_pages_loaded,
            dictionary_page_bytes,
            dictionary_unsupported_chunks,
            dictionary_page_read_errors,
            dictionary_predicate_probes,
            row_groups_skipped_by_dictionary,
            rows_skipped_by_dictionary,
            estimated_compressed_bytes_skipped_by_dictionary,
            row_groups_selected_after_dictionary,
            row_group_pruning_us,
            page_pruning_us,
            page_header_pruning_us,
            header_scan_us,
            bloom_pruning_us,
            dictionary_load_us,
            dictionary_pruning_us,
            pushed_filter_count = pushdown.map_or(0, |predicate| predicate.predicate_count),
            pushed_filter_stage_count = pushdown.map_or(0, |predicate| predicate.stages.len()),
            adaptive_filter_active = adaptive_filter_snapshot.is_some(),
            adaptive_filter_stage_count = adaptive_filter_snapshot.as_ref().map_or(0, |snapshot| snapshot.stage_count),
            adaptive_filter_observations = adaptive_filter_snapshot.as_ref().map_or(0, |snapshot| snapshot.observations),
            adaptive_filter_swap_attempts = adaptive_filter_snapshot.as_ref().map_or(0, |snapshot| snapshot.swap_attempts),
            adaptive_filter_swaps_accepted = adaptive_filter_snapshot.as_ref().map_or(0, |snapshot| snapshot.accepted_swaps),
            adaptive_filter_swaps_reverted = adaptive_filter_snapshot.as_ref().map_or(0, |snapshot| snapshot.reverted_swaps),
            adaptive_filter_warmup = adaptive_filter_snapshot.as_ref().is_some_and(|snapshot| snapshot.warmup),
            adaptive_filter_order = ?adaptive_filter_snapshot.as_ref().map(|snapshot| &snapshot.permutation),
            pushed_filter_columns = ?pushed_filter_columns,
            predicate_metrics_enabled = detailed_metrics_enabled,
            predicate_rows_before_filter = predicate_metrics.rows_before_filter,
            predicate_batches = predicate_metrics.batches,
            predicate_rows_in = predicate_metrics.rows_in,
            predicate_rows_kept = predicate_metrics.rows_kept,
            predicate_eval_wall_us = predicate_metrics.eval_wall_us,
            predicate_input_array_bytes = predicate_metrics.input_array_bytes,
            output_source_array_count,
            output_source_array_bytes,
            runtime_statistics_enabled = statistics_scan.is_some(),
            runtime_statistics_enqueue_us = statistics_enqueue_us,
            runtime_statistics_batches_queued = statistics_batches_queued,
            runtime_statistics_batches_queue_full = statistics_batches_queue_full,
            runtime_statistics_batches_too_large = statistics_batches_too_large,
            runtime_statistics_batches_worker_unavailable = statistics_batches_worker_unavailable,
            bytes_read = bytes_read_total,
            file_size_bytes,
            stat_us,
            metadata_us,
            build_us,
            first_batch_us,
            parquet_read_us = parquet_read_time.as_micros() as u64,
            first_poll_us = first_poll_time.as_micros() as u64,
            first_poll_io_us,
            first_poll_thread_cpu_us = first_poll_thread_cpu.as_micros() as u64,
            first_poll_offcpu_us = first_poll_offcpu.as_micros() as u64,
            rest_poll_us = rest_poll_time.as_micros() as u64,
            poll_thread_cpu_us = poll_thread_cpu.as_micros() as u64,
            poll_offcpu_us = parquet_read_time.saturating_sub(poll_thread_cpu).as_micros() as u64,
            poll_count,
            first_poll_polls,
            io_calls,
            io_ranges,
            io_read_us,
            io_wall_us,
            io_max_range_us,
            io_avg_bytes,
            downstream_us = downstream_time.as_micros() as u64,
            elapsed_us = elapsed.as_micros() as u64,
            rows_per_sec = (total_rows as f64 / elapsed.as_secs_f64().max(f64::MIN_POSITIVE))
                as u64,
            "scan_completed"
        );

        if next.finished() {
            break 'files;
        }
    }

    if let Some(scan) = statistics_scan {
        let queued = runtime_statistics.enqueue_partition_completion(
            common::statistics::runtime::RuntimeStatisticsPartitionCompletion {
                scan_id: scan.scan_id,
                table_key: &scan.table_key,
                expected_rows: scan.expected_rows,
                expected_partitions: scan.partition_count,
                table_scan_candidate: scan.row_coverage_candidate,
                partition_index: scan.partition_index,
                query_context: query_statistics.cloned(),
            },
        );
        if !queued {
            tracing::debug!(
                target: "vein.audit",
                event = "runtime_statistics_completion_unavailable",
                scan_id = scan.scan_id,
                "runtime statistics completion notification was unavailable"
            );
        }
    }

    Ok(())
}

#[cfg(test)]
#[path = "reader_metrics_tests.rs"]
mod reader_metrics_tests;

#[cfg(test)]
#[path = "parquet/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "parquet/runtime_statistics_tests.rs"]
mod runtime_statistics_tests;

#[cfg(test)]
#[path = "parquet/selection_tests.rs"]
mod selection_tests;
