//! Per-file reader and footer setup for Parquet scans.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Instant;

use catalog::{MetadataKey, MetadataStore};
use common::Result;
use error::VeinError;
use opendal::Operator;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::file::metadata::ParquetMetaData;

use super::super::ScanFile;
use super::PARQUET_FOOTER;
use super::reader::{IoStats, ParquetFileReader, ParquetIoBackend};

pub(super) struct PreparedParquetFile {
    pub(super) reader: ParquetFileReader,
    pub(super) metadata: Arc<ParquetMetaData>,
    pub(super) file_size_bytes: u64,
    pub(super) stat_us: u64,
    pub(super) metadata_started: Instant,
    pub(super) footer_hit: bool,
}

/// Reuses catalog file metadata when available and opens the scan's Parquet reader.
///
/// The returned reader owns one backend-specific file handle and shared counters.
/// Footer metadata stays in an `Arc`, so preparation does not copy column data.
pub(super) async fn prepare_parquet_file(
    operator: &Operator,
    file: &ScanFile,
    backend: ParquetIoBackend,
    bytes_read: &Arc<AtomicU64>,
    io: Arc<IoStats>,
    metadata_store: &MetadataStore,
) -> Result<PreparedParquetFile> {
    let path = file.relative.as_str();

    // The catalog stat'ed this file while planning and published its parsed
    // footer under a key built from that stat, so the key already carries the
    // file's length. Reusing it avoids another storage stat call.
    let stat_started = Instant::now();
    let footer_key = MetadataKey::for_canonical_file(PARQUET_FOOTER, &file.absolute);
    let file_size_bytes = match footer_key.as_ref().map(MetadataKey::size_bytes) {
        Some(size_bytes) => size_bytes,
        None => operator
            .stat(path)
            .await
            .map_err(|error| VeinError::internal(format!("Failed to stat file {path}: {error}")))?
            .content_length(),
    };
    let stat_us = stat_started.elapsed().as_micros() as u64;

    let mut reader = ParquetFileReader::new(
        operator,
        path,
        &file.absolute,
        backend,
        file_size_bytes,
        Arc::clone(bytes_read),
        io,
    )
        .await
        .map_err(|error| {
            VeinError::internal(format!("Failed to open Parquet range reader for '{path}': {error}"))
        })?;

    let metadata_started = Instant::now();
    let cached_footer = footer_key
        .as_ref()
        .and_then(|key| metadata_store.get::<ParquetMetaData>(key));
    let footer_hit = cached_footer.is_some();
    let metadata = match cached_footer {
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

    Ok(PreparedParquetFile {
        reader,
        metadata,
        file_size_bytes,
        stat_us,
        metadata_started,
        footer_hit,
    })
}
