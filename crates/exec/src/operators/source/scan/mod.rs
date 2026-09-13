use crate::pipeline::{DataChunk, PipelineNode, Source};
use common::protocol::FileFormat;
use common::Result;
use error::VeinError;
use opendal::Operator;

/// Dual-Plane .exo format decoding logic
pub mod exo_scan;
/// Parquet format decoding logic using OpenDAL
pub mod parquet;

pub use exo_scan::execute_exo_scan;
pub use parquet::execute_parquet_scan;

/// A generic Source operator that scans files using OpenDAL and dispatches based on format.
pub struct ScanSource {
    operator: Operator,
    paths: Vec<String>,
    format: FileFormat,
    projection: Option<Vec<String>>,
}

impl ScanSource {
    /// Create a new [`ScanSource`].
    pub fn new(operator: Operator, paths: Vec<String>, format: FileFormat) -> Self {
        Self {
            operator,
            paths,
            format,
            projection: None,
        }
    }

    /// Configures column projection pushdown.
    pub fn with_projection(mut self, projection: Option<Vec<String>>) -> Self {
        self.projection = projection;
        self
    }
}

#[async_trait::async_trait(?Send)]
impl Source<DataChunk> for ScanSource {
    #[tracing::instrument(skip_all)]
    async fn execute(&mut self, next: &mut dyn PipelineNode<DataChunk>) -> Result<()> {
        match self.format {
            FileFormat::Parquet => {
                parquet::execute_parquet_scan(&self.operator, &self.paths, next).await?;
            }
            FileFormat::Exo => {
                exo_scan::execute_exo_scan(&self.paths, self.projection.as_deref(), next).await?;
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

