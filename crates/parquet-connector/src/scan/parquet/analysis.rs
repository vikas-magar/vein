use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use common::statistics::StatisticsInput;
use common::statistics::analysis::StatisticsInputStream;
use error::VeinError;
use futures::StreamExt;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use storage::{ReadStorage, StorageFileMetadata, StorageObjectPath};

use crate::ParquetMetadataService;

use super::reader::{IoStats, ParquetFileReader, ParquetIoBackend};

/// Streams reference-backed inputs from a version-pinned set of Parquet files.
pub(crate) fn analysis_inputs(
    storage: Arc<dyn ReadStorage>,
    files: Vec<(StorageObjectPath, StorageFileMetadata)>,
    metadata_store: catalog::MetadataStore,
    batch_size: usize,
) -> StatisticsInputStream {
    Box::pin(async_stream::try_stream! {
        for (path, source) in files {
            let label = path.as_str().to_owned();
            let mut reader = ParquetFileReader::new(
                &storage,
                &label,
                Path::new(""),
                ParquetIoBackend::Storage,
                source.size_bytes,
                Arc::new(AtomicU64::new(0)),
                Arc::new(IoStats::default()),
            )
                .await
                .map_err(|error| VeinError::internal(format!("open ANALYZE source '{label}': {error}")))?;
            let service = ParquetMetadataService::new(metadata_store.clone());
            let (footer, _) = service
                .load_footer_from_reader(&source, &mut reader)
                .await?;
            let arrow_metadata = ArrowReaderMetadata::try_new(footer, ArrowReaderOptions::new())
                .map_err(|error| VeinError::internal(format!("read ANALYZE schema '{label}': {error}")))?;
            let mut batches = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arrow_metadata)
                .with_batch_size(batch_size)
                .build()
                .map_err(|error| VeinError::internal(format!("build ANALYZE stream '{label}': {error}")))?;
            while let Some(batch) = batches.next().await {
                let batch = batch
                    .map_err(|error| VeinError::internal(format!("read ANALYZE batch '{label}': {error}")))?;
                let input = exec::pipeline::arrow_adapter::from_record_batch(batch)?;
                yield Box::new(input) as Box<dyn StatisticsInput + Send>;
            }
        }
    })
}
