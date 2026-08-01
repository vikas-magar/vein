use crate::daemon::pipeline::{PipelineNode, Source};
use common::Result;
use common::protocol::FileFormat;
use error::VeinError;
use opendal::Operator;

/// Parquet format decoding logic using OpenDAL
pub mod parquet;

/// A generic Source operator that scans files using OpenDAL and dispatches based on format.
pub struct ScanSource {
    operator: Operator,
    paths: Vec<String>,
    format: FileFormat,
}

impl ScanSource {
    /// Create a new ScanSource
    pub fn new(operator: Operator, paths: Vec<String>, format: FileFormat) -> Self {
        Self {
            operator,
            paths,
            format,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Source for ScanSource {
    #[tracing::instrument(skip_all)]
    async fn execute(&mut self, next: &mut dyn PipelineNode) -> Result<()> {
        match self.format {
            FileFormat::Parquet => {
                parquet::execute_parquet_scan(&self.operator, &self.paths, next).await?;
            }
            FileFormat::Csv => {
                return Err(VeinError::internal("CSV decoding not yet implemented"));
            }
            FileFormat::Json => {
                return Err(VeinError::internal("JSON decoding not yet implemented"));
            }
        }

        next.flush()?;
        Ok(())
    }
}
