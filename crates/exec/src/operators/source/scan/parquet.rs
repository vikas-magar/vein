use crate::pipeline::{DataChunk, PipelineNode};
use bytes::Bytes;
use common::Result;
use common::protocol::FieldSchema;
use error::VeinError;
use futures::future::BoxFuture;
use futures::{Stream, StreamExt, TryStreamExt};
use opendal::Operator;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;
use std::future::poll_fn;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::ScanFile;
use catalog::{MetadataKey, MetadataKind, MetadataStore};

const MAX_CONCURRENT_RANGE_READS: usize = 8;

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
    projected_schema: &[FieldSchema],
    metadata_store: &MetadataStore,
    bytes_read: &Arc<AtomicU64>,
    next: &mut dyn PipelineNode<DataChunk>,
) -> Result<()> {
    for file in files {
        let path = file.relative.as_str();
        let scan_started = Instant::now();
        tracing::info!(target: "vein.audit", event = "scan_started", path = %path, format = "parquet", "scan_started");

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

        let reader = OpendalParquetReader::new(
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
        let mut builder = match cached_footer {
            Some(metadata) => {
                let metadata = ArrowReaderMetadata::try_new(metadata, ArrowReaderOptions::new())
                    .map_err(|error| {
                        VeinError::internal(format!(
                            "Failed to reuse Parquet metadata for '{path}': {error}"
                        ))
                    })?;
                ParquetRecordBatchStreamBuilder::new_with_metadata(reader, metadata)
            }
            None => {
                let builder = ParquetRecordBatchStreamBuilder::new(reader)
                    .await
                    .map_err(|e| {
                        VeinError::internal(format!("Failed to build parquet stream: {}", e))
                    })?;
                if let Some(key) = footer_key {
                    metadata_store.insert(key, Arc::clone(builder.metadata()));
                }
                tracing::debug!(
                    target: "vein.audit",
                    event = "parquet_footer_miss",
                    path = %path,
                    "parquet_footer_miss"
                );
                builder
            }
        };

        if !projected_schema.is_empty() {
            let arrow_schema = builder.schema();
            let mut root_indices = Vec::with_capacity(projected_schema.len());
            for field in projected_schema {
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
            let mask = ProjectionMask::roots(builder.parquet_schema(), root_indices);
            builder = builder.with_projection(mask);
        }
        let metadata_us = metadata_started.elapsed().as_micros() as u64;
        let row_groups = builder.metadata().num_row_groups();
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
            row_groups,
            file_rows,
            stat_us,
            metadata_us,
            build_us,
            "scan_ready"
        );

        let mut total_rows = 0;
        let mut batches = 0u64;
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
            let chunk = DataChunk::from_record_batch(batch);
            let downstream_started = Instant::now();
            next.push(chunk)?;
            downstream_time += downstream_started.elapsed();

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
        tracing::info!(
            target: "vein.audit",
            event = "scan_completed",
            path = %path,
            format = "parquet",
            footer_hit,
            rows = total_rows,
            file_rows,
            batches,
            row_groups,
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
    }

    Ok(())
}
