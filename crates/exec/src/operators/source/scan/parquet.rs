use crate::pipeline::{DataChunk, DataChunkView, PipelineNode};
use crate::runtime_filter::RuntimeFilterSet;
use crate::udf::FunctionRegistry;
use crate::vm::VectorVmExecutor;
use arrow::array::{Array, BooleanArray};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use common::Result;
use common::protocol::ScanPredicate;
use error::VeinError;
use futures::future::BoxFuture;
use futures::{Stream, StreamExt, TryStreamExt};
use opendal::Operator;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::arrow_reader::{ArrowPredicate, ArrowPredicateFn, RowFilter};
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;
use std::future::poll_fn;
use std::pin::Pin;
use std::sync::Arc;
use arrow::datatypes::{DataType as ArrowDataType, Schema, SchemaRef};
use types::{VeinDataType, VeinSchema};
use types::ResolvedColumnRef;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

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
            VeinError::internal(format!("dictionary column index {index} is outside the projected schema"))
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
                Arc::new(field.as_ref().clone().with_data_type(dictionary_type.clone()))
            } else {
                Arc::clone(field)
            }
        })
        .collect::<Vec<_>>();
    for name in dictionary_names {
        if !parquet_schema.fields().iter().any(|field| field.name() == name) {
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
            let reader = parquet::file::metadata::ParquetMetaDataReader::new();
            let meta = reader.load_and_finish(self, size).await?;
            Ok(std::sync::Arc::new(meta))
        })
    }
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
                predicate
                    .input
                    .fields
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let runtime_filter_names = runtime_filters.names();
        tracing::info!(
            target: "vein.audit",
            event = "scan_started",
            path = %path,
            format = "parquet",
            pushed_filter_count = pushdown.map_or(0, |predicate| predicate.predicate_count),
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
                let metadata = reader
                    .get_metadata(None)
                    .await
                    .map_err(|error| {
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
                    VeinError::internal(format!("Failed to infer Parquet schema for '{path}': {error}"))
                })?;
            let schema = dictionary_schema_hint(default_metadata.schema(), projected, dictionary_columns)?;
            ArrowReaderOptions::new().with_schema(schema)
        };
        let arrow_metadata = ArrowReaderMetadata::try_new(parquet_metadata, options).map_err(|error| {
            VeinError::internal(format!("Failed to configure Parquet reader for '{path}': {error}"))
        })?;
        let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arrow_metadata);

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
        let assigned_range = file
            .scan_unit_range
            .clone()
            .unwrap_or(0..row_groups_total);
        let row_groups_assigned = assigned_range
            .end
            .min(row_groups_total)
            .saturating_sub(assigned_range.start.min(row_groups_total));
        let mut statistics_predicates = pushdown
            .map_or_else(Vec::new, |predicate| predicate.statistics.clone());
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
        let row_groups_selected = row_group_pruning.selected.len();
        let row_groups_skipped = row_groups_assigned.saturating_sub(row_groups_selected);
        let row_groups_skipped_rows = row_group_pruning.skipped_rows;
        let estimated_compressed_bytes_skipped =
            row_group_pruning.estimated_compressed_bytes_skipped;
        if file.scan_unit_range.is_some() || row_groups_selected != row_groups_total {
            builder = builder.with_row_groups(row_group_pruning.selected);
        }
        let row_group_pruning_us = row_group_pruning_started.elapsed().as_micros() as u64;

        // The predicate runs before the projected columns are materialized: the
        // reader decodes only the predicate's own columns, evaluates the compiled
        // program on them, and then reads the surviving rows' projected columns.
        let mut predicates: Vec<Box<dyn ArrowPredicate>> = Vec::with_capacity(2);
        if let Some(pushdown) = pushdown {
            let mask = ProjectionMask::roots(
                builder.parquet_schema(),
                root_indices(builder.schema(), &pushdown.input.fields, path)?,
            );
            let mut evaluator = VectorVmExecutor::for_evaluation(
                Arc::clone(&pushdown.program),
                Arc::clone(registry),
            );
            let result = pushdown.result;
            let predicate_metrics = Arc::clone(&predicate_metrics);
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
                    tracing::trace!(
                        target: "vein.audit",
                        event = "scan_filter_batch",
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
            target: "vein.audit",
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
            row_group_pruning_us,
            file_rows,
            pushed_filter_count = pushdown.map_or(0, |predicate| predicate.predicate_count),
            pushed_filter_columns = ?pushed_filter_columns,
            pushed_filter_program_instructions = pushdown
                .map_or(0, |predicate| predicate.program.instructions.len()),
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
        tracing::info!(
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
            row_groups_skipped_rows,
            estimated_compressed_bytes_skipped,
            row_group_pruning_us,
            pushed_filter_count = pushdown.map_or(0, |predicate| predicate.predicate_count),
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
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;
    use types::{ColumnId, ResolvedColumn};

    #[test]
    fn dictionary_schema_hint_changes_only_requested_utf8_fields() {
        let metadata = std::collections::HashMap::from([("source".to_string(), "test".to_string())]);
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
}
