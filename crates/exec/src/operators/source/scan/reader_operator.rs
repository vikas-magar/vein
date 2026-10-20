//! Replaceable execution boundary for Parquet scan implementations.

use crate::pipeline::{DataChunkView, PipelineNode};
use common::Result;

use super::parquet::ParquetScanRequest;

/// Executes one Parquet scan into the engine's reference-backed pipeline.
///
/// Implementations own format-specific decode and pruning. The associated
/// batch type keeps the interface reusable, while `ScanSource` selects the
/// `DataChunkView` implementation so readers can retain row selections and
/// Arrow buffers across the scan boundary.
#[async_trait::async_trait]
pub trait ParquetScanReader: Send + Sync {
    /// Batch representation delivered to the downstream pipeline.
    type Batch: 'static;

    /// Reads the scan request and pushes batches to the next pipeline node.
    async fn execute(
        &self,
        request: ParquetScanRequest<'_>,
        next: &mut dyn PipelineNode<Self::Batch>,
    ) -> Result<()>;
}

/// Adapter for the current Arrow Parquet record-batch stream implementation.
#[derive(Debug, Default)]
pub struct ArrowParquetScanReader;

#[async_trait::async_trait]
impl ParquetScanReader for ArrowParquetScanReader {
    type Batch = DataChunkView;

    async fn execute(
        &self,
        request: ParquetScanRequest<'_>,
        next: &mut dyn PipelineNode<Self::Batch>,
    ) -> Result<()> {
        super::parquet::execute_parquet_scan(request, next).await
    }
}
