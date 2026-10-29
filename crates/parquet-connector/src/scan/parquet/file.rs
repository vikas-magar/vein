//! Per-file reader and footer setup for Parquet scans.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use common::time::Instant;

use crate::ParquetMetadataService;
use arrow::datatypes::SchemaRef;
use catalog::MetadataStore;
use common::Result;
use error::VeinError;
use parquet::file::metadata::ParquetMetaData;
use storage::{ReadStorage, StorageFileMetadata, StorageObjectPath};

use super::reader::{IoStats, ParquetFileReader, ParquetIoBackend};
use exec::operators::source::scan::ScanFile;

/// Loads a footer through the same storage range reader used by query scans.
pub(crate) async fn load_footer_from_storage(
    storage: &Arc<dyn ReadStorage>,
    path: &StorageObjectPath,
    source: &StorageFileMetadata,
    metadata_store: &MetadataStore,
) -> Result<(SchemaRef, Arc<ParquetMetaData>, bool)> {
    let relative_path = path.as_str();
    let mut reader = ParquetFileReader::new(
        storage,
        relative_path,
        std::path::Path::new(""),
        ParquetIoBackend::Storage,
        source.size_bytes,
        Arc::default(),
        Arc::new(IoStats::default()),
    )
        .await
        .map_err(|error| {
            VeinError::internal(format!(
                "open Parquet metadata reader for '{relative_path}': {error}"
            ))
        })?;
    let service = ParquetMetadataService::new(metadata_store.clone());
    let (footer, cache_hit) = service
        .load_footer_from_reader(source, &mut reader)
        .await?;
    let schema = service.arrow_schema(&footer, relative_path)?;
    Ok((schema, footer, cache_hit))
}

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
    storage: &Arc<dyn ReadStorage>,
    file: &ScanFile,
    backend: ParquetIoBackend,
    bytes_read: &Arc<AtomicU64>,
    io: Arc<IoStats>,
    metadata_store: &MetadataStore,
) -> Result<PreparedParquetFile> {
    let path = file.relative.as_str();
    let object_path = StorageObjectPath::new(path);

    let stat_started = Instant::now();
    let storage_metadata = match &file.storage_metadata {
        Some(metadata) => metadata.clone(),
        None => storage
            .stat(&object_path)
            .await
            .map_err(|error| VeinError::context(format!("Failed to stat file {path}"), error))?,
    };
    let stat_us = if file.storage_metadata.is_some() {
        0
    } else {
        stat_started.elapsed().as_micros() as u64
    };
    let file_size_bytes = storage_metadata.size_bytes;

    let mut reader = ParquetFileReader::new(
        storage,
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
    let metadata_service = ParquetMetadataService::new(metadata_store.clone());
    let (metadata, footer_hit) = metadata_service
        .load_footer_from_reader(&storage_metadata, &mut reader)
        .await?;
    if !footer_hit {
        tracing::debug!(
            target: "vein.audit",
            event = "parquet_footer_miss",
            source_id = %common::logging::LogLocation::from(path),
            "parquet_footer_miss"
        );
    }

    Ok(PreparedParquetFile {
        reader,
        metadata,
        file_size_bytes,
        stat_us,
        metadata_started,
        footer_hit,
    })
}

#[cfg(test)]
#[path = "file_tests.rs"]
mod tests;
