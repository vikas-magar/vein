use crate::pipeline::{DataChunk, DataChunkView, PipelineNode};
use crate::runtime_filter::RuntimeFilterSet;
use crate::udf::FunctionRegistry;
use crate::vm::VectorVmExecutor;
use arrow::array::{Array, BooleanArray};
use arrow::datatypes::{DataType as ArrowDataType, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use common::Result;
use common::protocol::ScanPredicate;
use error::VeinError;
use futures::future::BoxFuture;
use futures::{Stream, StreamExt, TryStreamExt};
use opendal::Operator;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowPredicate, ArrowPredicateFn, RowFilter};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use std::collections::HashMap;
use std::future::poll_fn;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use types::ResolvedColumnRef;
use types::{VeinDataType, VeinSchema};

use super::ScanFile;
use catalog::{MetadataKey, MetadataKind, MetadataStore};

const MAX_CONCURRENT_RANGE_READS: usize = 8;

/// Debug-only counters for the compiled predicate Arrow evaluates during a scan.
/// The counters are updated only when detailed scan telemetry is enabled, so
/// normal query execution does not pay for an extra pass over predicate output.
#[derive(Debug, Default)]
struct ScanPredicateMetrics {
    batches: AtomicU64,
    rows_in: AtomicU64,
    rows_kept: AtomicU64,
    eval_wall_us: AtomicU64,
    input_array_bytes: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default)]
struct ScanPredicateMetricsSnapshot {
    batches: u64,
    rows_in: u64,
    rows_kept: u64,
    eval_wall_us: u64,
    input_array_bytes: u64,
}

impl ScanPredicateMetrics {
    fn record_batch(
        &self,
        rows_in: usize,
        rows_kept: usize,
        elapsed: Duration,
        input_array_bytes: usize,
    ) {
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.rows_in.fetch_add(rows_in as u64, Ordering::Relaxed);
        self.rows_kept
            .fetch_add(rows_kept as u64, Ordering::Relaxed);
        self.eval_wall_us
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
        self.input_array_bytes
            .fetch_add(input_array_bytes as u64, Ordering::Relaxed);
    }

    fn snapshot(&self) -> ScanPredicateMetricsSnapshot {
        ScanPredicateMetricsSnapshot {
            batches: self.batches.load(Ordering::Relaxed),
            rows_in: self.rows_in.load(Ordering::Relaxed),
            rows_kept: self.rows_kept.load(Ordering::Relaxed),
            eval_wall_us: self.eval_wall_us.load(Ordering::Relaxed),
            input_array_bytes: self.input_array_bytes.load(Ordering::Relaxed),
        }
    }
}

fn count_true_rows(values: &BooleanArray) -> usize {
    (0..values.len())
        .filter(|row| values.is_valid(*row) && values.value(*row))
        .count()
}

/// Builds the full Arrow schema hint required by Parquet while changing only
/// the selected projected Utf8 columns to dictionary arrays.
fn dictionary_schema_hint(
    parquet_schema: &SchemaRef,
    projected: &VeinSchema,
    dictionary_columns: &[u32],
) -> Result<SchemaRef> {
    let mut dictionary_names = Vec::with_capacity(dictionary_columns.len());
    for &index in dictionary_columns {
        let column = projected.fields.get(index as usize).ok_or_else(|| {
            VeinError::internal(format!(
                "dictionary column index {index} is outside the projected schema"
            ))
        })?;
        if column.data_type != VeinDataType::Utf8 {
            return Err(VeinError::internal(format!(
                "dictionary encoding is only supported for Utf8 grouping columns, got {:?}",
                column.data_type
            )));
        }
        dictionary_names.push(column.name.as_str());
    }

    let dictionary_type = ArrowDataType::Dictionary(
        Box::new(ArrowDataType::Int32),
        Box::new(ArrowDataType::Utf8),
    );
    let fields = parquet_schema
        .fields()
        .iter()
        .map(|field| {
            if dictionary_names.contains(&field.name().as_str()) {
                Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(dictionary_type.clone()),
                )
            } else {
                Arc::clone(field)
            }
        })
        .collect::<Vec<_>>();
    for name in dictionary_names {
        if !parquet_schema
            .fields()
            .iter()
            .any(|field| field.name() == name)
        {
            return Err(VeinError::internal(format!(
                "dictionary projected column '{name}' is missing from the Parquet schema"
            )));
        }
    }
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        parquet_schema.metadata().clone(),
    )))
}

/// Maps `fields` onto their Parquet root column indices, in source order.
///
/// Both the output projection and a push-down predicate address columns the same
/// way, so both go through here: a column the file does not carry, one that is
/// ambiguous, or a set that is not in source order is a planning bug, not a
/// partial read.
fn root_indices(
    arrow_schema: &arrow::datatypes::Schema,
    fields: &[ResolvedColumnRef],
    path: &str,
) -> Result<Vec<usize>> {
    let mut root_indices = Vec::with_capacity(fields.len());
    for field in fields {
        let mut matches = arrow_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, candidate)| candidate.name() == &field.name);
        let Some((index, _)) = matches.next() else {
            return Err(VeinError::internal(format!(
                "projected Parquet column '{}' is missing from '{}'",
                field.name, path
            )));
        };
        if matches.next().is_some() {
            return Err(VeinError::internal(format!(
                "projected Parquet column '{}' is ambiguous in '{}'",
                field.name, path
            )));
        }
        root_indices.push(index);
    }
    if root_indices.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(VeinError::internal(format!(
            "projected columns are not in source schema order for '{}'",
            path
        )));
    }
    Ok(root_indices)
}

/// Metadata kind under which this format publishes parsed Parquet footers.
///
/// The catalog that plans a scan and the scan itself must both use this
/// constant, so a footer parsed once is found by the other side. It moves to a
/// dedicated Parquet catalog crate together with the rest of this format.
pub const PARQUET_FOOTER: MetadataKind = MetadataKind::new("parquet.footer");

/// An adapter to use OpenDAL's Operator as a Parquet AsyncFileReader
struct OpendalParquetReader {
    operator: Operator,
    path: String,
    size_bytes: u64,
    bytes_read: Arc<AtomicU64>,
    io: Arc<IoStats>,
}

/// Counters describing how a scan's bytes were fetched from storage.
///
/// Separating the time spent inside storage calls from the poll time that
/// contains them distinguishes a slow byte path from per-read overhead: if a
/// handful of ranges account for the read time, the cost is bytes or latency;
/// if thousands do, the cost is per-call overhead.
#[derive(Debug, Default)]
struct IoStats {
    calls: AtomicU64,
    ranges: AtomicU64,
    read_us: AtomicU64,
    max_range_us: AtomicU64,
    fetch_wall_us: AtomicU64,
}

impl IoStats {
    fn record_call(&self) {
        self.calls.fetch_add(1, Ordering::Relaxed);
    }

    /// Wall time of a whole `get_bytes`/`get_byte_ranges` invocation.
    ///
    /// Compared against the summed per-range time, this is the observed
    /// concurrency factor: `read_us / fetch_wall_us` is ~1 when ranges run one
    /// after another and ~N when they overlap.
    fn record_fetch(&self, elapsed: Duration) {
        self.fetch_wall_us
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
    }

    fn record(&self, elapsed: Duration) {
        let micros = elapsed.as_micros() as u64;
        self.ranges.fetch_add(1, Ordering::Relaxed);
        self.read_us.fetch_add(micros, Ordering::Relaxed);
        self.max_range_us.fetch_max(micros, Ordering::Relaxed);
    }
}

/// CPU time consumed by the calling thread so far.
///
/// Sample this immediately before and after a synchronous `poll_next` call. A
/// task can be rescheduled onto a different worker between polls, so a pair
/// spanning an `await` subtracts two threads' clocks and charges another
/// thread's CPU to this scan; sampling inside the poll stays on one thread.
/// Wall time minus this delta is time the thread did not run at all.
#[cfg(unix)]
fn thread_cpu_time() -> Duration {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid, properly aligned `timespec` that outlives the call.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    if rc != 0 {
        return Duration::ZERO;
    }
    Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
}

/// Reports zero where the platform has no portable per-thread CPU clock.
#[cfg(not(unix))]
fn thread_cpu_time() -> Duration {
    Duration::ZERO
}

impl OpendalParquetReader {
    /// Wraps `operator` as a Parquet reader for `path`, which holds `size_bytes` bytes.
    ///
    /// The length is passed in rather than fetched: whoever builds the reader
    /// already knows the file's identity, so a second stat of the same file
    /// would only repeat work the caller has done.
    fn new(
        operator: &Operator,
        path: &str,
        size_bytes: u64,
        bytes_read: Arc<AtomicU64>,
        io: Arc<IoStats>,
    ) -> Self {
        Self {
            operator: operator.clone(),
            path: path.to_string(),
            size_bytes,
            bytes_read,
            io,
        }
    }
}

impl AsyncFileReader for OpendalParquetReader {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<u64>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        Box::pin(async move {
            self.io.record_call();
            let fetch_started = Instant::now();
            let read_started = Instant::now();
            let data = self
                .operator
                .read_with(&self.path)
                .range(range.start..range.end)
                .await
                .map_err(|e| parquet::errors::ParquetError::General(e.to_string()))?;
            self.io.record(read_started.elapsed());
            self.bytes_read
                .fetch_add(data.len() as u64, Ordering::Relaxed);
            self.io.record_fetch(fetch_started.elapsed());
            Ok(data.to_bytes())
        })
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        self.io.record_call();
        let io = self.io.clone();
        let reads = ranges.into_iter().map(|range| {
            let operator = self.operator.clone();
            let path = self.path.clone();
            let bytes_read = self.bytes_read.clone();
            let io = self.io.clone();
            async move {
                let read_started = Instant::now();
                let data = operator
                    .read_with(&path)
                    .range(range.start..range.end)
                    .await
                    .map_err(|error| parquet::errors::ParquetError::General(error.to_string()))?;
                io.record(read_started.elapsed());
                bytes_read.fetch_add(data.len() as u64, Ordering::Relaxed);
                Ok(data.to_bytes())
            }
        });

        Box::pin(async move {
            let fetch_started = Instant::now();
            let result = futures::stream::iter(reads)
                .buffered(MAX_CONCURRENT_RANGE_READS)
                .try_collect::<Vec<_>>()
                .await;
            io.record_fetch(fetch_started.elapsed());
            result
        })
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a parquet::arrow::arrow_reader::ArrowReaderOptions>,
    ) -> BoxFuture<
        'a,
        parquet::errors::Result<std::sync::Arc<parquet::file::metadata::ParquetMetaData>>,
    > {
        Box::pin(async move {
            let size = self.size_bytes;
            // Page indexes are separate ranges outside the footer. Loading them
            // optionally makes page pruning available while remaining compatible
            // with files that were written without indexes.
            let reader =
                ParquetMetaDataReader::new().with_page_index_policy(PageIndexPolicy::Optional);
            let meta = reader.load_and_finish(self, size).await?;
            Ok(std::sync::Arc::new(meta))
        })
    }
}

const INITIAL_PAGE_HEADER_PROBE_BYTES: u64 = 32;
const MAX_PAGE_HEADER_PROBE_BYTES: u64 = 64 * 1024;

/// Enables an opt-in audit that reads data-page headers even when the page
/// index already covers every pushed predicate. This intentionally adds a
/// pre-read and is meant for comparing metadata sources, not normal scans.
fn page_header_audit_enabled() -> bool {
    std::env::var("VEIN_PARQUET_PAGE_HEADER_AUDIT")
        .ok()
        .is_some_and(|value| {
            let value = value.trim();
            value == "1"
                || value.eq_ignore_ascii_case("true")
                || value.eq_ignore_ascii_case("yes")
                || value.eq_ignore_ascii_case("on")
        })
}

#[derive(Debug, Default)]
struct PageHeaderReadMetrics {
    columns_requested: u64,
    chunks_scanned: u64,
    pages_scanned: u64,
    pages_with_statistics: u64,
    pages_with_complete_bounds: u64,
    unsupported_row_mapping_chunks: u64,
    header_bytes: u64,
    probe_bytes: u64,
    failed_chunks: u64,
    elapsed_us: u64,
}

#[derive(Debug, Default)]
struct PageHeaderReadResult {
    pages: crate::parquet_statistics::PageHeaderStatistics,
    /// Complete data-page byte maps keyed by row group and Parquet leaf ordinal.
    /// They let the push decoder fetch the surviving pages without relying on
    /// a footer OffsetIndex.
    page_locations: HeaderPageLocations,
    metrics: PageHeaderReadMetrics,
}

type HeaderPageLocations = HashMap<
    usize,
    HashMap<usize, Vec<parquet::file::page_index::offset_index::PageLocation>>,
>;

#[derive(Debug, Default)]
struct PagePayloadPruningMetrics {
    columns_with_locations: u64,
    data_pages_available: u64,
    data_pages_selected: u64,
    data_pages_skipped: u64,
    selected_payload_bytes: u64,
    skipped_payload_bytes: u64,
}

fn attach_header_page_locations(
    selections: Vec<parquet::arrow::arrow_reader::RowGroupSelection>,
    locations_by_row_group: &HeaderPageLocations,
) -> Vec<parquet::arrow::arrow_reader::RowGroupSelection> {
    selections
        .into_iter()
        .map(|mut selection| {
            if let Some(columns) = locations_by_row_group.get(&selection.row_group_index()) {
                for (column_index, locations) in columns {
                    selection = selection.with_page_locations(*column_index, locations.clone());
                }
            }
            selection
        })
        .collect()
}

fn page_payload_pruning_metrics(
    selections: &[parquet::arrow::arrow_reader::RowGroupSelection],
    locations_by_row_group: &HeaderPageLocations,
) -> PagePayloadPruningMetrics {
    let mut metrics = PagePayloadPruningMetrics::default();
    for selection in selections {
        let Some(columns) = locations_by_row_group.get(&selection.row_group_index()) else {
            continue;
        };
        for locations in columns.values() {
            if locations.is_empty() {
                continue;
            }
            metrics.columns_with_locations = metrics.columns_with_locations.saturating_add(1);
            metrics.data_pages_available = metrics
                .data_pages_available
                .saturating_add(u64::try_from(locations.len()).unwrap_or(u64::MAX));
            let total_bytes = locations.iter().fold(0_u64, |total, page| {
                total.saturating_add(u64::try_from(page.compressed_page_size).unwrap_or(0))
            });
            let selected_ranges = selection
                .selection()
                .map(|rows| rows.scan_ranges(locations));
            let selected_pages = locations
                .iter()
                .filter(|page| {
                    selected_ranges.as_ref().is_none_or(|ranges| {
                        ranges.iter().any(|range| {
                            u64::try_from(page.offset).ok().is_some_and(|offset| {
                                offset >= range.start
                                    && offset
                                    .checked_add(
                                        u64::try_from(page.compressed_page_size).unwrap_or(0),
                                    )
                                    .is_some_and(|end| end <= range.end)
                            })
                        })
                    })
                })
                .collect::<Vec<_>>();
            let selected_bytes = selected_pages.iter().fold(0_u64, |total, page| {
                total.saturating_add(u64::try_from(page.compressed_page_size).unwrap_or(0))
            });
            metrics.data_pages_selected = metrics
                .data_pages_selected
                .saturating_add(u64::try_from(selected_pages.len()).unwrap_or(u64::MAX));
            metrics.data_pages_skipped = metrics
                .data_pages_skipped
                .saturating_add(u64::try_from(locations.len() - selected_pages.len()).unwrap_or(u64::MAX));
            metrics.selected_payload_bytes = metrics
                .selected_payload_bytes
                .saturating_add(selected_bytes.min(total_bytes));
            metrics.skipped_payload_bytes = metrics
                .skipped_payload_bytes
                .saturating_add(total_bytes.saturating_sub(selected_bytes));
        }
    }
    metrics
}

/// Reads data-page headers for predicate columns when page-index coverage is
/// missing or incomplete. Unsupported mappings or malformed chunks remain
/// conservative and are left for Arrow's normal row filter.
async fn read_page_header_statistics(
    operator: &Operator,
    path: &str,
    file_size_bytes: u64,
    bytes_read: Arc<AtomicU64>,
    io: Arc<IoStats>,
    arrow_schema: &Schema,
    metadata: &ParquetMetaData,
    needed_columns: &[String],
    row_groups: &[usize],
) -> PageHeaderReadResult {
    let started = Instant::now();
    let mut result = PageHeaderReadResult::default();
    let parquet_schema = metadata.file_metadata().schema_descr();
    let columns = needed_columns
        .iter()
        .filter_map(|name| {
            let (_, field) = arrow_schema.column_with_name(name)?;
            let converter =
                parquet::arrow::arrow_reader::statistics::StatisticsConverter::try_new(
                    name,
                    arrow_schema,
                    parquet_schema,
                )
                    .ok()?;
            let ordinal = converter.parquet_column_index()?;
            let descriptor = parquet_schema.column(ordinal);
            Some((
                name.clone(),
                ordinal,
                field.data_type().clone(),
                descriptor.physical_type(),
                descriptor.max_rep_level(),
            ))
        })
        .collect::<Vec<_>>();
    result.metrics.columns_requested = u64::try_from(needed_columns.len()).unwrap_or(u64::MAX);
    let mut reader = OpendalParquetReader::new(
        operator,
        path,
        file_size_bytes,
        bytes_read,
        io,
    );

    for row_group_index in row_groups {
        let Some(row_group) = metadata.row_groups().get(*row_group_index) else {
            continue;
        };
        let Some(row_count) = usize::try_from(row_group.num_rows()).ok() else {
            continue;
        };
        let mut row_group_pages = HashMap::new();
        let mut row_group_page_locations = HashMap::new();
        for (name, column_ordinal, arrow_type, physical_type, max_rep_level) in &columns {
            // Page-header statistics do not provide the offset index's
            // authoritative first-row mapping for repeated fields. Their page
            // values may cover only part of a logical row, so fail open here.
            if *max_rep_level != 0 {
                result.metrics.unsupported_row_mapping_chunks = result
                    .metrics
                    .unsupported_row_mapping_chunks
                    .saturating_add(1);
                continue;
            }
            let Some(column) = row_group.columns().get(*column_ordinal) else {
                result.metrics.failed_chunks = result.metrics.failed_chunks.saturating_add(1);
                continue;
            };
            let chunk_start = column
                .dictionary_page_offset()
                .unwrap_or_else(|| column.data_page_offset());
            let Some(chunk_start) = u64::try_from(chunk_start).ok() else {
                result.metrics.failed_chunks = result.metrics.failed_chunks.saturating_add(1);
                continue;
            };
            let Some(chunk_size) = u64::try_from(column.compressed_size()).ok() else {
                result.metrics.failed_chunks = result.metrics.failed_chunks.saturating_add(1);
                continue;
            };
            let Some(chunk_end) = chunk_start
                .checked_add(chunk_size)
                .filter(|end| *end <= file_size_bytes)
            else {
                result.metrics.failed_chunks = result.metrics.failed_chunks.saturating_add(1);
                continue;
            };
            let Some(mut offset) = u64::try_from(column.data_page_offset())
                .ok()
                .filter(|offset| *offset >= chunk_start && *offset < chunk_end)
            else {
                result.metrics.failed_chunks = result.metrics.failed_chunks.saturating_add(1);
                continue;
            };

            result.metrics.chunks_scanned = result.metrics.chunks_scanned.saturating_add(1);
            let mut first_row = 0usize;
            let mut page_ordinal = 0usize;
            // Every accepted page must cover at least one row, so a valid flat
            // column cannot contain more data pages than its row group.
            let max_page_count = row_count;
            let mut pages = Vec::new();
            let mut page_locations = Vec::new();
            let mut chunk_valid = true;
            while offset < chunk_end {
                if page_ordinal >= max_page_count {
                    chunk_valid = false;
                    break;
                }
                page_ordinal = page_ordinal.saturating_add(1);
                let (header, probe_bytes) = match read_page_header_at(
                    &mut reader,
                    offset,
                    chunk_end,
                    *physical_type,
                )
                    .await
                {
                    Ok(value) => value,
                    Err(error) => {
                        result.metrics.probe_bytes = result
                            .metrics
                            .probe_bytes
                            .saturating_add(error.probe_bytes);
                        tracing::debug!(
                            target: "vein.audit",
                            event = "parquet_page_header_unavailable",
                            path = %path,
                            row_group = row_group_index,
                            column = %name,
                            offset,
                            probe_bytes = error.probe_bytes,
                            error = %error.message,
                            "retaining page range because its header could not be parsed"
                        );
                        chunk_valid = false;
                        break;
                    }
                };
                result.metrics.pages_scanned = result.metrics.pages_scanned.saturating_add(1);
                result.metrics.probe_bytes = result.metrics.probe_bytes.saturating_add(probe_bytes);
                result.metrics.header_bytes = result
                    .metrics
                    .header_bytes
                    .saturating_add(u64::try_from(header.header_size).unwrap_or(u64::MAX));
                let Some(body_offset) = offset.checked_add(header.header_size as u64) else {
                    chunk_valid = false;
                    break;
                };
                let Some(next_offset) = body_offset
                    .checked_add(header.compressed_page_size as u64)
                    .filter(|next| *next > offset && *next <= chunk_end)
                else {
                    chunk_valid = false;
                    break;
                };

                if let Some(page) = header.data_page {
                    let page_rows = page
                        .num_rows
                        .or_else(|| (*max_rep_level == 0).then_some(page.num_values));
                    let Some(page_rows) = page_rows.filter(|rows| *rows > 0) else {
                        chunk_valid = false;
                        break;
                    };
                    let Some(next_row) = first_row.checked_add(page_rows) else {
                        chunk_valid = false;
                        break;
                    };
                    if next_row > row_count {
                        chunk_valid = false;
                        break;
                    }
                    let Some(page_size) = next_offset.checked_sub(offset) else {
                        chunk_valid = false;
                        break;
                    };
                    let (Ok(page_offset), Ok(page_size), Ok(first_row_index)) = (
                        i64::try_from(offset),
                        i32::try_from(page_size),
                        i64::try_from(first_row),
                    ) else {
                        chunk_valid = false;
                        break;
                    };
                    let statistics = crate::parquet_statistics::column_statistics_from_page_header(
                        page.statistics.as_ref(),
                        arrow_type,
                        *physical_type,
                    );
                    if page.statistics.is_some() {
                        result.metrics.pages_with_statistics =
                            result.metrics.pages_with_statistics.saturating_add(1);
                    }
                    if statistics.min_max_complete == Some(true) {
                        result.metrics.pages_with_complete_bounds = result
                            .metrics
                            .pages_with_complete_bounds
                            .saturating_add(1);
                    }
                    pages.push(crate::parquet_statistics::PageHeaderRowStatistics {
                        first_row,
                        row_count: page_rows,
                        statistics,
                    });
                    page_locations.push(parquet::file::page_index::offset_index::PageLocation {
                        offset: page_offset,
                        compressed_page_size: page_size,
                        first_row_index,
                    });
                    first_row = next_row;
                }
                offset = next_offset;
            }

            if chunk_valid && offset == chunk_end && first_row == row_count && !pages.is_empty() {
                row_group_pages.insert(name.clone(), pages);
                row_group_page_locations.insert(*column_ordinal, page_locations);
            } else {
                result.metrics.failed_chunks = result.metrics.failed_chunks.saturating_add(1);
            }
        }
        if !row_group_pages.is_empty() {
            result.pages.insert(*row_group_index, row_group_pages);
            result
                .page_locations
                .insert(*row_group_index, row_group_page_locations);
        }
    }

    result.metrics.elapsed_us = started.elapsed().as_micros() as u64;
    result
}

async fn read_page_header_at(
    reader: &mut OpendalParquetReader,
    offset: u64,
    end: u64,
    physical_type: parquet::basic::Type,
) -> std::result::Result<(parquet::file::serialized_reader::PageHeaderInfo, u64), PageHeaderReadError> {
    let available = end
        .checked_sub(offset)
        .filter(|available| *available > 0)
        .ok_or_else(|| PageHeaderReadError {
            message: "page header offset is outside the column chunk".to_string(),
            probe_bytes: 0,
        })?;
    let max_probe = available.min(MAX_PAGE_HEADER_PROBE_BYTES);
    let mut probe_size = INITIAL_PAGE_HEADER_PROBE_BYTES.min(max_probe);
    let mut probe_bytes_read = 0_u64;
    loop {
        let probe_end = offset
            .checked_add(probe_size)
            .ok_or_else(|| PageHeaderReadError {
                message: "page header probe range overflowed".to_string(),
                probe_bytes: probe_bytes_read,
            })?;
        let bytes = reader
            .get_bytes(offset..probe_end)
            .await
            .map_err(|error| PageHeaderReadError {
                message: error.to_string(),
                probe_bytes: probe_bytes_read,
            })?;
        probe_bytes_read = probe_bytes_read
            .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        let parse_error = match parquet::file::serialized_reader::read_page_header(
            &mut Cursor::new(bytes.as_ref()),
            physical_type,
        ) {
            Ok(header) if header.header_size > 0 && header.header_size <= bytes.len() => {
                return Ok((header, probe_bytes_read));
            }
            Ok(_) => "page header parser returned an invalid header length".to_string(),
            Err(error) => error.to_string(),
        };
        if probe_size >= max_probe {
            return Err(PageHeaderReadError {
                message: parse_error,
                probe_bytes: probe_bytes_read,
            });
        }
        probe_size = probe_size.saturating_mul(2).min(max_probe);
    }
}

#[derive(Debug)]
struct PageHeaderReadError {
    message: String,
    probe_bytes: u64,
}

/// Executes a scan over a list of Parquet files
pub async fn execute_parquet_scan(
    operator: &Operator,
    files: &[ScanFile],
    projected: &VeinSchema,
    dictionary_columns: &[u32],
    pushdown: Option<&ScanPredicate>,
    metadata_store: &MetadataStore,
    registry: &Arc<FunctionRegistry>,
    bytes_read: &Arc<AtomicU64>,
    runtime_filters: &RuntimeFilterSet,
    next: &mut dyn PipelineNode<DataChunkView>,
) -> Result<()> {
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

        // The catalog stat'ed this file while planning and published its parsed
        // footer under a key built from that stat, so the key already carries the
        // file's length. Reusing it keeps a scan from paying storage again for a
        // number the planning pass measured. Only a file the key cannot describe —
        // one the catalog never saw — still needs a stat.
        let stat_started = Instant::now();
        let footer_key = MetadataKey::for_canonical_file(PARQUET_FOOTER, &file.absolute);
        let file_size_bytes = match footer_key.as_ref().map(MetadataKey::size_bytes) {
            Some(size_bytes) => size_bytes,
            None => operator
                .stat(path)
                .await
                .map_err(|e| VeinError::internal(format!("Failed to stat file {path}: {e}")))?
                .content_length(),
        };
        let stat_us = stat_started.elapsed().as_micros() as u64;

        let mut reader = OpendalParquetReader::new(
            operator,
            path,
            file_size_bytes,
            bytes_read.clone(),
            Arc::clone(&io),
        );

        let metadata_started = Instant::now();
        let cached_footer = footer_key
            .as_ref()
            .and_then(|key| metadata_store.get::<ParquetMetaData>(key));
        let footer_hit = cached_footer.is_some();
        let parquet_metadata = match cached_footer {
            Some(metadata) => metadata,
            None => {
                let metadata = reader.get_metadata(None).await.map_err(|error| {
                    VeinError::internal(format!("Failed to read Parquet metadata: {error}"))
                })?;
                if let Some(key) = footer_key {
                    metadata_store.insert(key, Arc::clone(&metadata));
                }
                tracing::debug!(
                    target: "vein.audit",
                    event = "parquet_footer_miss",
                    path = %path,
                    "parquet_footer_miss"
                );
                metadata
            }
        };
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

        // Rows per scanned batch, overridable with `VEIN_SCAN_BATCH_ROWS` so the
        // value can be measured rather than argued about.
        //
        // arrow-parquet defaults to 1024, DuckDB uses 2048 and DataFusion 8192.
        // Every per-chunk cost in the engine — the index lists a gather builds,
        // `take`/`interleave`, the channel handoff, the operator chain — is paid
        // once per batch, so this is the cheapest lever on scan and join
        // throughput alike.
        const DEFAULT_SCAN_BATCH_ROWS: usize = 8192;
        let batch_rows = std::env::var("VEIN_SCAN_BATCH_ROWS")
            .ok()
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(DEFAULT_SCAN_BATCH_ROWS);
        builder = builder.with_batch_size(batch_rows);

        if !projected.fields.is_empty() {
            let mask = ProjectionMask::roots(
                builder.parquet_schema(),
                root_indices(builder.schema(), &projected.fields, path)?,
            );
            builder = builder.with_projection(mask);
        }

        // Prune from statistics on this exact footer. The row filter remains
        // the semantic check for every selected row group; statistics only
        // remove groups that cannot produce a TRUE result.
        let row_groups_total = builder.metadata().num_row_groups();
        let row_group_pruning_started = Instant::now();
        let assigned_range = file.scan_unit_range.clone().unwrap_or(0..row_groups_total);
        let row_groups_assigned = assigned_range
            .end
            .min(row_groups_total)
            .saturating_sub(assigned_range.start.min(row_groups_total));
        let mut statistics_predicates =
            pushdown.map_or_else(Vec::new, |predicate| predicate.statistics.clone());
        let static_statistics_predicates = statistics_predicates.len();
        let runtime_statistics = runtime_filters.statistics_predicates(projected);
        let runtime_statistics_predicates = runtime_statistics.len();
        statistics_predicates.extend(runtime_statistics);
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
        let page_header_columns = crate::parquet_statistics::page_header_columns_needed_with_audit(
            &statistics_predicates,
            &header_row_groups,
            &row_group_pruning.selected,
            page_index_available,
            &page_index_pruning,
            audit_page_headers,
        );
        let page_header_started = Instant::now();
        let page_header_result = if page_header_columns.is_empty() {
            PageHeaderReadResult::default()
        } else {
            read_page_header_statistics(
                operator,
                path,
                file_size_bytes,
                bytes_read.clone(),
                Arc::clone(&io),
                builder.schema().as_ref(),
                builder.metadata(),
                &page_header_columns,
                &header_row_groups,
            )
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
                columns_requested = page_header_columns.len(),
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
            columns_requested = page_header_columns.len(),
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
        let bloom_columns = crate::parquet_statistics::bloom_filter_columns(
            builder.schema().as_ref(),
            builder.metadata(),
            &statistics_predicates,
        );
        let bloom_filter_columns = bloom_columns.len();
        let mut bloom_filters_available = 0_u64;
        let mut bloom_filters_loaded = 0_u64;
        let mut bloom_filter_read_errors = 0_u64;
        let mut bloom_filter_bytes = 0_u64;
        let bloom_filter_load_started = Instant::now();
        let mut row_group_bloom_filters = HashMap::new();
        for selection in &page_pruning.selections {
            let row_group_index = selection.row_group_index();
            let mut filters = HashMap::with_capacity(bloom_columns.len());
            for column in &bloom_columns {
                let Some((has_bloom_filter, bloom_filter_length)) = (|| {
                    let row_group = builder.metadata().row_group(row_group_index);
                    row_group.columns().get(column.parquet_column_index).map(|metadata| {
                        (
                            metadata.bloom_filter_offset().is_some(),
                            metadata
                                .bloom_filter_length()
                                .and_then(|length| u64::try_from(length).ok()),
                        )
                    })
                })() else {
                    continue;
                };
                if !has_bloom_filter {
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

        // Dictionary evidence applies only when every data page in the chunk
        // uses that dictionary. We fetch the dictionary page alone and retain
        // the row group on missing metadata, unsupported literals, or I/O errors.
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
        let mut dictionary_chunks_considered = 0_u64;
        let mut dictionary_chunks_eligible = 0_u64;
        let mut dictionary_pages_loaded = 0_u64;
        let mut dictionary_page_read_errors = 0_u64;
        let mut dictionary_unsupported_chunks = 0_u64;
        let mut dictionary_page_bytes = 0_u64;
        let dictionary_load_started = Instant::now();
        let mut dictionary_reader = OpendalParquetReader::new(
            operator,
            path,
            file_size_bytes,
            bytes_read.clone(),
            Arc::clone(&io),
        );
        let mut dictionary_membership = HashMap::new();
        for selection in &bloom_pruning.selections {
            let row_group_index = selection.row_group_index();
            let row_group = builder.metadata().row_group(row_group_index);
            let mut outcomes = vec![None; dictionary_predicates.len()];
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
                    dictionary_unsupported_chunks =
                        dictionary_unsupported_chunks.saturating_add(1);
                    continue;
                };
                let Some(range) = crate::parquet_statistics::dictionary_page_range(
                    chunk,
                    file_size_bytes,
                ) else {
                    dictionary_unsupported_chunks =
                        dictionary_unsupported_chunks.saturating_add(1);
                    continue;
                };
                dictionary_chunks_eligible = dictionary_chunks_eligible.saturating_add(1);
                let page_bytes = match dictionary_reader.get_bytes(range.clone()).await {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        dictionary_page_read_errors =
                            dictionary_page_read_errors.saturating_add(1);
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
                    outcomes[predicate.index] = outcome;
                    if outcome.is_none() {
                        dictionary_unsupported_chunks =
                            dictionary_unsupported_chunks.saturating_add(1);
                    }
                }
            }
            dictionary_membership.insert(row_group_index, outcomes);
        }
        let dictionary_load_us = dictionary_load_started.elapsed().as_micros() as u64;
        let dictionary_pruning_started = Instant::now();
        let mut dictionary_pruning = crate::parquet_statistics::prune_row_groups_with_dictionary(
            builder.metadata(),
            std::mem::take(&mut bloom_pruning.selections),
            &dictionary_membership,
        );
        dictionary_pruning.selections = attach_header_page_locations(
            std::mem::take(&mut dictionary_pruning.selections),
            &page_header_result.page_locations,
        );
        let page_payload_metrics = page_payload_pruning_metrics(
            &dictionary_pruning.selections,
            &page_header_result.page_locations,
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
        let row_groups_selected_after_dictionary = dictionary_pruning.selections.len();
        let has_scan_selection = file.scan_unit_range.is_some()
            || row_groups_selected != row_groups_total
            || page_rows_skipped != 0
            || row_groups_skipped_by_bloom != 0
            || row_groups_skipped_by_dictionary != 0
            // Preserve header-derived page locations even when metadata cannot
            // prune rows yet; a runtime filter may produce a selection before
            // later filter stages or projected columns are fetched.
            || !page_header_result.page_locations.is_empty();
        if has_scan_selection {
            if dictionary_pruning.selections.is_empty() {
                builder = builder.with_row_groups(Vec::new());
            } else {
                builder = builder.with_row_group_selections(dictionary_pruning.selections);
            }
        }
        let row_group_pruning_us = row_group_pruning_started.elapsed().as_micros() as u64;

        // Stages run before projected columns are materialized. Arrow applies each
        // predicate to the surviving selection from the preceding stage, so later
        // VM programs process fewer rows and only decode their own input columns.
        let mut predicates: Vec<Box<dyn ArrowPredicate>> =
            Vec::with_capacity(pushdown.map_or(0, |predicate| predicate.stages.len()) + 1);
        let mut stage_metrics = Vec::new();
        if let Some(pushdown) = pushdown {
            for (stage_index, stage) in pushdown.stages.iter().enumerate() {
                let mask = ProjectionMask::roots(
                    builder.parquet_schema(),
                    root_indices(builder.schema(), &stage.input.fields, path)?,
                );
                let mut evaluator = VectorVmExecutor::for_evaluation(
                    Arc::clone(&stage.program),
                    Arc::clone(registry),
                );
                let result = stage.result;
                let stage_predicate_count = stage.predicate_count;
                let predicate_metrics = Arc::clone(&predicate_metrics);
                let metrics = Arc::new(ScanPredicateMetrics::default());
                stage_metrics.push((stage_index, stage_predicate_count,
                    stage.input.fields.iter().map(|field| field.name.clone()).collect::<Vec<_>>(),
                    Arc::clone(&metrics)));
                let predicate = ArrowPredicateFn::new(mask, move |batch: RecordBatch| {
                    let rows_in = batch.num_rows();
                    let input_array_bytes = if detailed_metrics_enabled {
                        batch
                            .columns()
                            .iter()
                            .map(|array| array.get_array_memory_size())
                            .sum()
                    } else {
                        0
                    };
                    let eval_started = detailed_metrics_enabled.then(Instant::now);
                    let mut chunk = DataChunk::from_record_batch(batch);
                    let boolean = evaluator
                        .evaluate(&mut chunk, result)
                        .map_err(|error| ArrowError::ComputeError(error.to_string()))?
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .cloned()
                        .ok_or_else(|| {
                            ArrowError::ComputeError(
                                "scan predicate did not evaluate to a boolean column".to_string(),
                            )
                        })?;
                    if let Some(eval_started) = eval_started {
                        let rows_kept = count_true_rows(&boolean);
                        let eval_wall = eval_started.elapsed();
                        predicate_metrics.record_batch(
                            rows_in,
                            rows_kept,
                            eval_wall,
                            input_array_bytes,
                        );
                        metrics.record_batch(rows_in, rows_kept, eval_wall, input_array_bytes);
                        tracing::trace!(
                            target: "vein.audit",
                            event = "scan_filter_batch",
                            stage_index,
                            stage_predicate_count,
                            rows_in,
                            rows_kept,
                            input_array_bytes,
                            eval_wall_us = eval_wall.as_micros() as u64,
                            "scan_filter_batch"
                        );
                    }
                    Ok(boolean)
                });
                predicates.push(Box::new(predicate));
            }
        }
        if let Some(binding) = runtime_filters.bind(projected) {
            let columns = binding.columns();
            let key_column_count = columns.len();
            let mask = ProjectionMask::roots(
                builder.parquet_schema(),
                root_indices(builder.schema(), columns, path)?,
            );
            let predicate = ArrowPredicateFn::new(mask, move |batch: RecordBatch| {
                binding
                    .evaluate(&batch)
                    .map_err(|error| ArrowError::ComputeError(error.to_string()))
            });
            tracing::debug!(
                target: "vein.audit",
                event = "scan_runtime_filter_bound",
                path = %path,
                filter_names = ?runtime_filters.names(),
                key_columns = key_column_count,
                filter_bytes = runtime_filters.memory_bytes(),
                "scan runtime filter bound"
            );
            predicates.push(Box::new(predicate));
        } else if runtime_filters.has_row_filters() {
            tracing::debug!(
                target: "vein.audit",
                event = "scan_runtime_filter_skipped",
                path = %path,
                reason = "required_column_missing",
                filter_names = ?runtime_filters.names(),
                "scan runtime filter skipped"
            );
        }
        if !predicates.is_empty() {
            builder = builder.with_row_filter(RowFilter::new(predicates));
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
            footer_hit,
            file_size_bytes,
            row_groups_total,
            row_groups_assigned,
            row_groups_selected,
            row_groups_skipped,
            row_groups_skipped_rows,
            static_statistics_predicates,
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
            pushed_filter_program_instructions = pushdown.map_or(0, |predicate| {
                predicate
                    .stages
                    .iter()
                    .map(|stage| stage.program.instructions.len())
                    .sum()
            }),
            runtime_filter_count = runtime_filter_names.len(),
            runtime_filter_names = ?runtime_filter_names,
            runtime_filter_bytes = runtime_filters.memory_bytes(),
            detailed_metrics_enabled,
            stat_us,
            metadata_us,
            build_us,
            "scan_ready"
        );

        let mut total_rows = 0;
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
            if batches == 0 {
                first_batch_us = scan_started.elapsed().as_micros() as u64;
            }
            total_rows += batch.num_rows();
            batches += 1;

            tracing::trace!(rows = batch.num_rows(), "Pushing batch down pipeline");
            let chunk = DataChunkView::from_record_batch(batch)?;
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

            // A limit below the pipeline already has every row it will emit, so
            // the rest of this file — and every file after it — can only be
            // discarded. Stop reading, but still report what this file cost.
            if next.finished() {
                break;
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
            target: "vein.audit",
            event = "scan_completed",
            path = %path,
            format = "parquet",
            footer_hit,
            rows = total_rows,
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
            page_header_columns = page_header_columns.len(),
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
            pushed_filter_columns = ?pushed_filter_columns,
            predicate_metrics_enabled = detailed_metrics_enabled,
            predicate_batches = predicate_metrics.batches,
            predicate_rows_in = predicate_metrics.rows_in,
            predicate_rows_kept = predicate_metrics.rows_kept,
            predicate_eval_wall_us = predicate_metrics.eval_wall_us,
            predicate_input_array_bytes = predicate_metrics.input_array_bytes,
            output_source_array_count,
            output_source_array_bytes,
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

    Ok(())
}

#[cfg(test)]
#[path = "reader_metrics_tests.rs"]
mod reader_metrics_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field};
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::push_decoder::ParquetPushDecoderBuilder;
    use parquet::DecodeResult;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use std::sync::Arc;
    use types::{ColumnId, ResolvedColumn};

    #[test]
    fn dictionary_schema_hint_changes_only_requested_utf8_fields() {
        let metadata =
            std::collections::HashMap::from([("source".to_string(), "test".to_string())]);
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("status", DataType::Utf8, true),
                Field::new("orderkey", DataType::Int64, false),
            ],
            metadata,
        ));
        let projected = VeinSchema::new(vec![
            Arc::new(ResolvedColumn::new(
                ColumnId(0),
                "status",
                None,
                VeinDataType::Utf8,
            )),
            Arc::new(ResolvedColumn::new(
                ColumnId(1),
                "orderkey",
                None,
                VeinDataType::Int64,
            )),
        ]);

        let hinted = dictionary_schema_hint(&schema, &projected, &[0]).expect("valid hint");

        assert_eq!(
            hinted.field(0).data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        );
        assert!(hinted.field(0).is_nullable());
        assert_eq!(hinted.field(1).data_type(), &DataType::Int64);
        assert_eq!(hinted.metadata()["source"], "test");
    }

    #[test]
    fn dictionary_schema_hint_rejects_non_utf8_and_out_of_range_fields() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let projected = VeinSchema::new(vec![Arc::new(ResolvedColumn::new(
            ColumnId(0),
            "id",
            None,
            VeinDataType::Int64,
        ))]);

        assert!(dictionary_schema_hint(&schema, &projected, &[0]).is_err());
        assert!(dictionary_schema_hint(&schema, &projected, &[1]).is_err());
    }

    #[test]
    fn scan_predicate_metrics_count_only_true_rows_and_accumulate_batches() {
        let values = BooleanArray::from(vec![Some(true), Some(false), None, Some(true)]);
        assert_eq!(count_true_rows(&values), 2);

        let metrics = ScanPredicateMetrics::default();
        metrics.record_batch(4, 2, Duration::from_micros(7), 128);
        metrics.record_batch(3, 1, Duration::from_micros(5), 64);
        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.batches, 2);
        assert_eq!(snapshot.rows_in, 7);
        assert_eq!(snapshot.rows_kept, 3);
        assert_eq!(snapshot.eval_wall_us, 12);
        assert_eq!(snapshot.input_array_bytes, 192);
    }

    #[tokio::test]
    async fn page_header_statistics_prune_flat_data_pages_without_dropping_matches() {
        let root = tempfile::tempdir().expect("temporary Parquet root");
        let relative_path = "page_headers.parquet";
        let path = root.path().join(relative_path);
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, false),
            Field::new("payload", DataType::Int64, false),
        ]));
        let properties = WriterProperties::builder()
            .set_data_page_size_limit(256)
            .set_write_batch_size(32)
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_write_page_header_statistics(true)
            .build();
        let file = std::fs::File::create(&path).expect("create Parquet file");
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(properties))
            .expect("create Parquet writer");
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(0_i64..8192)),
                Arc::new(Int64Array::from_iter_values((0_i64..8192).map(|value| value * 2))),
            ],
        )
            .expect("valid sorted values");
        writer.write(&batch).expect("write values");
        writer.close().expect("finish Parquet file");

        let file_size = std::fs::metadata(&path)
            .expect("read Parquet file size")
            .len();
        let metadata = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Skip)
            .parse_and_finish(&std::fs::File::open(&path).expect("open Parquet footer"))
            .expect("parse footer without page indexes");
        assert_eq!(metadata.num_row_groups(), 1);
        assert!(metadata.page_index().is_none());

        let root_path = root.path().to_string_lossy().into_owned();
        let operator = Operator::new(opendal::services::Fs::default().root(&root_path))
            .expect("create local filesystem operator");
        let bytes_read = Arc::new(AtomicU64::new(0));
        let page_headers = read_page_header_statistics(
            &operator,
            relative_path,
            file_size,
            Arc::clone(&bytes_read),
            Arc::new(IoStats::default()),
            schema.as_ref(),
            &metadata,
            &["value".to_string()],
            &[0],
        )
            .await;

        assert_eq!(page_headers.metrics.failed_chunks, 0);
        assert_eq!(page_headers.metrics.unsupported_row_mapping_chunks, 0);
        assert!(page_headers.metrics.pages_scanned > 1);
        assert_eq!(
            page_headers.metrics.pages_with_statistics,
            page_headers.metrics.pages_scanned,
            "the fixture explicitly writes statistics into every page header"
        );
        assert_eq!(
            page_headers.metrics.pages_with_complete_bounds,
            page_headers.metrics.pages_scanned,
            "the fixture uses exact Int64 min/max bounds"
        );
        assert!(page_headers.metrics.header_bytes > 0);
        assert!(page_headers.metrics.probe_bytes >= page_headers.metrics.header_bytes);
        assert!(bytes_read.load(Ordering::Relaxed) >= page_headers.metrics.probe_bytes);

        let pages = page_headers
            .pages
            .get(&0)
            .and_then(|columns| columns.get("value"))
            .expect("complete flat column page mapping");
        assert_eq!(pages.len(), page_headers.metrics.pages_scanned as usize);
        assert_eq!(pages.first().expect("first data page").first_row, 0);
        let page_locations = page_headers
            .page_locations
            .get(&0)
            .and_then(|columns| columns.get(&0))
            .expect("header scan returns byte locations for the flat column");
        assert_eq!(page_locations.len(), pages.len());
        assert_eq!(page_locations.first().expect("first data page").first_row_index, 0);
        assert_eq!(
            pages.iter().map(|page| page.row_count).sum::<usize>(),
            8192,
            "page row spans must cover the row group exactly"
        );

        let target = 4097_i64;
        let predicate = catalog::StatisticsPredicate::Comparison {
            column: "value".to_string(),
            comparison: catalog::StatisticsComparison::Equal,
            literal: types::LogicalLiteral::Int64(target),
        };
        let without_header_statistics =
            crate::parquet_statistics::prune_data_pages_with_headers(
                schema.as_ref(),
                &metadata,
                std::slice::from_ref(&predicate),
                &[0],
                &crate::parquet_statistics::PageHeaderStatistics::new(),
            );
        assert_eq!(without_header_statistics.rows_skipped, 0);
        assert!(without_header_statistics.selections[0].selection().is_none());

        let pruning = crate::parquet_statistics::prune_data_pages_with_headers(
            schema.as_ref(),
            &metadata,
            std::slice::from_ref(&predicate),
            &[0],
            &page_headers.pages,
        );
        assert!(pruning.row_ranges_skipped > 0);
        assert!(pruning.rows_skipped > 0);
        let row_group_selection = attach_header_page_locations(
            pruning.selections,
            &page_headers.page_locations,
        )
            .into_iter()
            .next()
            .expect("one row group survives the page-header pruning");
        let selection = row_group_selection
            .selection()
            .cloned()
            .expect("page-header bounds should create a row selection");
        let payload_metrics = page_payload_pruning_metrics(
            std::slice::from_ref(&row_group_selection),
            &page_headers.page_locations,
        );
        assert!(payload_metrics.data_pages_skipped > 0);
        assert!(payload_metrics.skipped_payload_bytes > 0);
        assert!(row_group_selection.page_locations(0).is_some());
        assert!(row_group_selection.page_locations(1).is_none());

        // Exercise Vein's actual push-decoder path with no OffsetIndex. The
        // custom locations must restrict storage requests to the dictionary
        // prefix and data pages selected by the page-header statistics.
        let mut decoder = ParquetPushDecoderBuilder::try_new_decoder(Arc::new(metadata.clone()))
            .expect("create push decoder")
            .with_row_group_selections(vec![row_group_selection])
            .build()
            .expect("build push decoder with header-derived page locations");
        let file_bytes = bytes::Bytes::from(std::fs::read(&path).expect("read test Parquet bytes"));
        let value_chunk = metadata.row_group(0).column(0);
        let (value_chunk_start, value_chunk_len) = value_chunk.byte_range();
        let value_chunk_end = value_chunk_start + value_chunk_len;
        let payload_chunk = metadata.row_group(0).column(1);
        let (payload_chunk_start, payload_chunk_len) = payload_chunk.byte_range();
        let payload_chunk_end = payload_chunk_start + payload_chunk_len;
        let mut requested_ranges = Vec::new();
        let mut push_matches = Vec::new();
        loop {
            match decoder.try_decode().expect("decode selected page ranges") {
                DecodeResult::NeedsData(ranges) => {
                    requested_ranges.extend(ranges.iter().cloned());
                    let data = ranges
                        .iter()
                        .map(|range| {
                            file_bytes.slice(range.start as usize..range.end as usize)
                        })
                        .collect();
                    decoder
                        .push_ranges(ranges, data)
                        .expect("provide requested Parquet ranges");
                }
                DecodeResult::Data(batch) => {
                    let values = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("push-decoder output is Int64");
                    let payload = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("push-decoder payload is Int64");
                    push_matches.extend((0..values.len()).filter_map(|row| {
                        (values.value(row) == target)
                            .then_some((values.value(row), payload.value(row)))
                    }));
                }
                DecodeResult::Finished => break,
            }
        }
        let bytes_requested_in = |chunk_start: u64, chunk_end: u64| {
            requested_ranges.iter().fold(0_u64, |total, range| {
                let start = range.start.max(chunk_start);
                let end = range.end.min(chunk_end);
                total.saturating_add(end.saturating_sub(start))
            })
        };
        let value_bytes_requested = bytes_requested_in(value_chunk_start, value_chunk_end);
        let payload_bytes_requested = bytes_requested_in(payload_chunk_start, payload_chunk_end);
        assert_eq!(push_matches, vec![(target, target * 2)]);
        assert!(!requested_ranges.is_empty());
        assert!(requested_ranges.iter().all(|range| {
            (range.start >= value_chunk_start && range.end <= value_chunk_end)
                || (range.start >= payload_chunk_start && range.end <= payload_chunk_end)
        }));
        let dictionary_prefix_bytes = u64::try_from(page_locations[0].offset)
            .expect("validated first data page offset")
            .saturating_sub(value_chunk_start);
        assert_eq!(
            value_bytes_requested,
            dictionary_prefix_bytes.saturating_add(payload_metrics.selected_payload_bytes),
            "the filtered column should read its required dictionary and selected data pages only"
        );
        assert_eq!(
            payload_bytes_requested, payload_chunk_len,
            "a projected column without page locations must fall back to its complete chunk"
        );
        let total_page_payload_bytes = page_locations
            .iter()
            .map(|page| u64::try_from(page.compressed_page_size).unwrap_or(0))
            .sum::<u64>();
        assert!(payload_metrics.selected_payload_bytes < total_page_payload_bytes);
        assert_eq!(
            payload_metrics.selected_payload_bytes + payload_metrics.skipped_payload_bytes,
            total_page_payload_bytes,
            "selected and skipped data-page bytes must account for the complete page map"
        );

        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(&path).expect("reopen Parquet file"),
        )
            .expect("build selected reader")
            .with_row_selection(selection)
            .build()
            .expect("apply page-derived row selection");
        let matches = reader
            .map(|batch| {
                let batch = batch.expect("read selected rows");
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("Int64 output");
                (0..values.len())
                    .map(|row| values.value(row))
                    .filter(|value| *value == target)
                    .collect::<Vec<_>>()
            })
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(matches, vec![target]);
    }
}
