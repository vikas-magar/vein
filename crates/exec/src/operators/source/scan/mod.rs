use crate::pipeline::{DataChunk, PipelineNode, Source};
use common::protocol::FileFormat;
use common::Result;
use error::VeinError;
use opendal::Operator;

/// Parquet source backed by a local-filesystem OpenDAL operator.
pub mod parquet;

/// Reads local Parquet files into the execution pipeline.
pub struct ScanSource {
    operator: Operator,
    paths: Vec<String>,
    format: FileFormat,
}

impl ScanSource {
    /// Creates a scan over the provided files.
    pub fn new(operator: Operator, paths: Vec<String>, format: FileFormat) -> Self {
        Self { operator, paths, format }
    }
}

#[async_trait::async_trait(?Send)]
impl Source<DataChunk> for ScanSource {
    async fn execute(&mut self, next: &mut dyn PipelineNode<DataChunk>) -> Result<()> {
        match self.format {
            FileFormat::Parquet => parquet::execute_parquet_scan(&self.operator, &self.paths, next).await?,
            _ => return Err(VeinError::internal("local executor supports Parquet files only")),
        }
        next.flush()
    }
}
