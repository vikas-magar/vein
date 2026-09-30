use crate::pipeline::{DataChunk, PipelineNode, Source};
use common::protocol::{FieldSchema, FileFormat};
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
    projected_schema: Vec<FieldSchema>,
}

impl ScanSource {
    /// Creates a scan over the provided files.
    pub fn new(
        operator: Operator,
        paths: Vec<String>,
        format: FileFormat,
        projected_schema: Vec<FieldSchema>,
    ) -> Self {
        Self {
            operator,
            paths,
            format,
            projected_schema,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Source<DataChunk> for ScanSource {
    async fn execute(&mut self, next: &mut dyn PipelineNode<DataChunk>) -> Result<()> {
        match self.format {
            FileFormat::Parquet => parquet::execute_parquet_scan(&self.operator, &self.paths, &self.projected_schema, next).await?,
            _ => return Err(VeinError::internal("local executor supports Parquet files only")),
        }
        next.flush()
    }
}
