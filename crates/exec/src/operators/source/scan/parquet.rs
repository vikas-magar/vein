use crate::pipeline::{DataChunk, PipelineNode};
use bytes::Bytes;
use common::Result;
use common::protocol::FieldSchema;
use error::VeinError;
use futures::future::BoxFuture;
use futures::{StreamExt, TryStreamExt};
use opendal::Operator;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;
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
            Ok(data.to_bytes())
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
                Ok(data.to_bytes())
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
    files: &[ScanFile],
    projected_schema: &[FieldSchema],
    metadata_store: &MetadataStore,
    next: &mut dyn PipelineNode<DataChunk>,
) -> Result<()> {
    for file in files {
        let path = file.relative.as_str();
        let scan_started = Instant::now();
        tracing::info!(target: "vein.audit", event = "scan_started", path = %path, format = "parquet", "scan_started");

        let stat_started = Instant::now();
        let reader = OpendalParquetReader::new(operator, path).await?;
        let stat_us = stat_started.elapsed().as_micros() as u64;
        let file_size_bytes = reader.size_bytes;
        let bytes_read = reader.bytes_read.clone();

        let metadata_started = Instant::now();
        let footer_key = MetadataKey::for_file(PARQUET_FOOTER, &file.absolute);
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
        let mut downstream_time = Duration::ZERO;
        let mut first_batch_us = 0u64;
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
                    bytes_read = bytes_read.load(Ordering::Relaxed),
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
        let bytes_read_total = bytes_read.load(Ordering::Relaxed);
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
            downstream_us = downstream_time.as_micros() as u64,
            elapsed_us = elapsed.as_micros() as u64,
            rows_per_sec = (total_rows as f64 / elapsed.as_secs_f64().max(f64::MIN_POSITIVE))
                as u64,
            "scan_completed"
        );
    }

    Ok(())
}
