//! Replaceable execution boundary for Parquet scan implementations.

use crate::pipeline::PipelineNode;
use common::Result;

use super::parquet::ParquetScanRequest;

/// Executes one Parquet scan into the engine's reference-backed pipeline.
///
/// Implementations own format-specific decode and pruning, and emit the
/// canonical `DataChunkView` so row selections and Arrow buffers remain shared.
#[async_trait::async_trait]
pub trait ParquetScanReader: Send + Sync {
    /// Reads the scan request and pushes views to the next pipeline node.
    async fn execute(
        &self,
        request: ParquetScanRequest<'_>,
        next: &mut dyn PipelineNode,
    ) -> Result<()>;
}

/// Adapter for the current Arrow Parquet record-batch stream implementation.
#[derive(Debug, Default)]
pub struct ArrowParquetScanReader;

#[async_trait::async_trait]
impl ParquetScanReader for ArrowParquetScanReader {
    async fn execute(
        &self,
        request: ParquetScanRequest<'_>,
        next: &mut dyn PipelineNode,
    ) -> Result<()> {
        super::parquet::execute_parquet_scan(request, next).await
    }
}
