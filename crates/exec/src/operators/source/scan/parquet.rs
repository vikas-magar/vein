use crate::pipeline::{DataChunk, PipelineNode};
use bytes::Bytes;
use common::Result;
use error::VeinError;
use futures::future::BoxFuture;
use futures::{StreamExt, TryStreamExt};
use opendal::Operator;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const MAX_CONCURRENT_RANGE_READS: usize = 8;

/// An adapter to use OpenDAL's Operator as a Parquet AsyncFileReader
struct OpendalParquetReader {
    operator: Operator,
    path: String,
    size_bytes: u64,
    bytes_read: Arc<AtomicU64>,
}

impl OpendalParquetReader {
    async fn new(operator: &Operator, path: &str) -> Result<Self> {
        let meta = operator
            .stat(path)
            .await
            .map_err(|e| VeinError::internal(format!("Failed to stat file {}: {}", path, e)))?;

        let size_bytes = meta.content_length();

        Ok(Self {
            operator: operator.clone(),
            path: path.to_string(),
            size_bytes,
            bytes_read: Arc::new(AtomicU64::new(0)),
        })
    }
}

impl AsyncFileReader for OpendalParquetReader {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<u64>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        Box::pin(async move {
            let data = self
                .operator
                .read_with(&self.path)
                .range(range.start..range.end)
                .await
                .map_err(|e| parquet::errors::ParquetError::General(e.to_string()))?;
            self.bytes_read
                .fetch_add(data.len() as u64, Ordering::Relaxed);
            Ok(Bytes::from(data))
        })
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        let reads = ranges.into_iter().map(|range| {
            let operator = self.operator.clone();
            let path = self.path.clone();
            let bytes_read = self.bytes_read.clone();
            async move {
                let data = operator
                    .read_with(&path)
                    .range(range.start..range.end)
                    .await
                    .map_err(|error| parquet::errors::ParquetError::General(error.to_string()))?;
                bytes_read.fetch_add(data.len() as u64, Ordering::Relaxed);
                Ok(Bytes::from(data))
            }
        });

        Box::pin(async move {
            futures::stream::iter(reads)
                .buffered(MAX_CONCURRENT_RANGE_READS)
                .try_collect::<Vec<_>>()
                .await
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
    paths: &[String],
    next: &mut dyn PipelineNode<DataChunk>,
) -> Result<()> {
    for path in paths {
        let scan_started = Instant::now();
        tracing::info!(target: "vein.audit", event = "scan_started", path = %path, format = "parquet", "scan_started");

        let reader = OpendalParquetReader::new(operator, path).await?;
        let file_size_bytes = reader.size_bytes;
        let bytes_read = reader.bytes_read.clone();

        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(|e| VeinError::internal(format!("Failed to build parquet stream: {}", e)))?;

        let mut stream = builder
            .build()
            .map_err(|e| VeinError::internal(format!("Failed to build parquet stream: {}", e)))?;

        let mut total_rows = 0;
        let mut batches = 0u64;
        let mut parquet_read_time = Duration::ZERO;
        let mut downstream_time = Duration::ZERO;
        let mut last_progress = Instant::now();
        loop {
            let read_started = Instant::now();
            let next_batch = stream.next().await;
            parquet_read_time += read_started.elapsed();
            let Some(batch_result) = next_batch else {
                break;
            };
            let batch = batch_result
                .map_err(|e| VeinError::internal(format!("Parquet read error: {}", e)))?;
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
                    bytes_read = bytes_read.load(Ordering::Relaxed),
                    file_size_bytes,
                    parquet_read_ms = parquet_read_time.as_millis() as u64,
                    downstream_ms = downstream_time.as_millis() as u64,
                    elapsed_ms = scan_started.elapsed().as_millis() as u64,
                    "parquet_scan_progress"
                );
                last_progress = Instant::now();
            }
        }

        tracing::info!(
            target: "vein.audit",
            event = "scan_completed",
            path = %path,
            format = "parquet",
            rows = total_rows,
            batches,
            bytes_read = bytes_read.load(Ordering::Relaxed),
            file_size_bytes,
            parquet_read_ms = parquet_read_time.as_millis() as u64,
            downstream_ms = downstream_time.as_millis() as u64,
            elapsed_ms = scan_started.elapsed().as_millis() as u64,
            "scan_completed"
        );
    }

    Ok(())
}
