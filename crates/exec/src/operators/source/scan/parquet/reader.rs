//! Asynchronous Parquet reads over OpenDAL with bounded range concurrency.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{StreamExt, TryStreamExt};
use opendal::Operator;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};

const MAX_CONCURRENT_RANGE_READS: usize = 8;

pub(super) struct OpendalParquetReader {
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
pub(super) struct IoStats {
    pub(super) calls: AtomicU64,
    pub(super) ranges: AtomicU64,
    pub(super) read_us: AtomicU64,
    pub(super) max_range_us: AtomicU64,
    pub(super) fetch_wall_us: AtomicU64,
}

impl IoStats {
    pub(super) fn record_call(&self) {
        self.calls.fetch_add(1, Ordering::Relaxed);
    }

    /// Wall time of a whole `get_bytes`/`get_byte_ranges` invocation.
    ///
    /// Compared against the summed per-range time, this is the observed
    /// concurrency factor: `read_us / fetch_wall_us` is ~1 when ranges run one
    /// after another and ~N when they overlap.
    pub(super) fn record_fetch(&self, elapsed: Duration) {
        self.fetch_wall_us
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
    }

    pub(super) fn record(&self, elapsed: Duration) {
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
pub(super) fn thread_cpu_time() -> Duration {
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
    pub(super) fn new(
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
