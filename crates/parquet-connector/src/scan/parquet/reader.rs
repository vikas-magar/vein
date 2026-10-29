//! Asynchronous Parquet range reads with shared metrics and metadata loading.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use common::time::Instant;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{StreamExt, TryStreamExt};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};
use storage::ReadStorage;

mod backend;
pub(super) use backend::ParquetIoBackend;
use backend::ParquetRangeSource;

const MAX_CONCURRENT_RANGE_READS: usize = 8;

pub(super) struct ParquetFileReader {
    source: Arc<dyn ParquetRangeSource>,
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
pub(super) fn thread_cpu_time() -> Duration {
    Duration::ZERO
}

impl ParquetFileReader {
    /// Opens a Parquet range reader for one file.
    ///
    /// The backend is selected once per scan, allowing the same Parquet decoding
    /// and pruning path to be measured over storage ranges or a native file
    /// handle. The caller supplies file size from the catalog to avoid a second stat.
    pub(super) async fn new(
        storage: &Arc<dyn ReadStorage>,
        relative_path: &str,
        absolute_path: &std::path::Path,
        backend: ParquetIoBackend,
        size_bytes: u64,
        bytes_read: Arc<AtomicU64>,
        io: Arc<IoStats>,
    ) -> parquet::errors::Result<Self> {
        let source = backend
            .open_source(storage, relative_path, absolute_path, size_bytes)
            .await?;
        Ok(Self {
            source,
            size_bytes,
            bytes_read,
            io,
        })
    }
}

impl AsyncFileReader for ParquetFileReader {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<u64>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.io.record_call();
        let source = Arc::clone(&self.source);
        let bytes_read = Arc::clone(&self.bytes_read);
        let io = Arc::clone(&self.io);
        Box::pin(async move {
            let fetch_started = Instant::now();
            let result = read_range(source, range, bytes_read, Arc::clone(&io)).await;
            io.record_fetch(fetch_started.elapsed());
            result
        })
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        self.io.record_call();
        let io = self.io.clone();
        let range_io = Arc::clone(&io);
        let source = Arc::clone(&self.source);
        let bytes_read = Arc::clone(&self.bytes_read);
        let reads = ranges.into_iter().map(move |range| {
            read_range(
                Arc::clone(&source),
                range,
                Arc::clone(&bytes_read),
                Arc::clone(&range_io),
            )
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

fn read_range(
    source: Arc<dyn ParquetRangeSource>,
    range: std::ops::Range<u64>,
    bytes_read: Arc<AtomicU64>,
    io: Arc<IoStats>,
) -> BoxFuture<'static, parquet::errors::Result<Bytes>> {
    Box::pin(async move {
        let started = Instant::now();
        let data = source.read_range(range).await?;
        io.record(started.elapsed());
        bytes_read.fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(data)
    })
}
