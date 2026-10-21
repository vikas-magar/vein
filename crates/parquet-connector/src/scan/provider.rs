//! Adapter from Vein's generic scan contract to the Parquet reader.

use async_trait::async_trait;
use catalog::DataFileFormatId;
use common::Result;
use common::protocol::{DataSource, FileScanTask};
use exec::operators::source::scan::{ScanPlanningRequest, ScanProvider, ScanRequest};
use exec::pipeline::PipelineNode;
use types::ColumnId;

use super::parquet::execute_parquet_scan;

/// Canonical identifier used by catalogs for Parquet files.
pub const PARQUET_FORMAT_ID: &str = "parquet";

/// Parquet reader and pruning adapter registered by an application composition root.
#[derive(Debug)]
pub struct ParquetScanProvider {
    format_id: DataFileFormatId,
}

impl ParquetScanProvider {
    /// Constructs a provider for the canonical Parquet format identifier.
    pub fn new() -> Self {
        Self {
            format_id: DataFileFormatId::new(PARQUET_FORMAT_ID),
        }
    }
}

impl Default for ParquetScanProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ScanProvider for ParquetScanProvider {
    fn format_id(&self) -> &DataFileFormatId {
        &self.format_id
    }

    fn plan_work_units(&self, request: ScanPlanningRequest<'_>) -> Result<Vec<DataSource>> {
        super::parquet::plan_scan(request)
    }

    fn supports_runtime_filter_pushdown(&self, scan: &FileScanTask, columns: &[ColumnId]) -> bool {
        columns
            .iter()
            .all(|id| scan.projected.fields.iter().any(|column| column.id == *id))
    }

    async fn execute(&self, request: ScanRequest<'_>, next: &mut dyn PipelineNode) -> Result<()> {
        execute_parquet_scan(request, next).await
    }
}

#[cfg(test)]
mod tests {
    use super::{PARQUET_FORMAT_ID, ParquetScanProvider};
    use catalog::DataFileFormatId;
    use exec::operators::source::scan::ScanProvider;

    #[test]
    fn provider_exposes_the_canonical_format_identity() {
        let provider = ParquetScanProvider::new();
        assert_eq!(provider.format_id(), &DataFileFormatId::new(PARQUET_FORMAT_ID));
    }
}
