use std::path::PathBuf;

use crate::pipeline::{DataChunk, PipelineNode, Source};
use catalog::MetadataStore;
use common::protocol::{FieldSchema, FileFormat};
use common::Result;
use error::VeinError;
use opendal::Operator;

/// Parquet source backed by a local-filesystem OpenDAL operator.
pub mod parquet;

pub use parquet::PARQUET_FOOTER;

/// One file of a scan, identified both by the path storage is asked for and by
/// the absolute path that keys the shared footer store.
#[derive(Debug, Clone)]
pub struct ScanFile {
    /// Path relative to the scan's OpenDAL operator root.
    pub relative: String,
    /// Absolute path of the same file.
    pub absolute: PathBuf,
}

/// Reads local Parquet files into the execution pipeline.
pub struct ScanSource {
    operator: Operator,
    files: Vec<ScanFile>,
    format: FileFormat,
    projected_schema: Vec<FieldSchema>,
    metadata_store: MetadataStore,
}

impl ScanSource {
    /// Creates a scan over the provided files.
    pub fn new(
        operator: Operator,
        files: Vec<ScanFile>,
        format: FileFormat,
        projected_schema: Vec<FieldSchema>,
        metadata_store: MetadataStore,
    ) -> Self {
        Self {
            operator,
            files,
            format,
            projected_schema,
            metadata_store,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Source<DataChunk> for ScanSource {
    async fn execute(&mut self, next: &mut dyn PipelineNode<DataChunk>) -> Result<()> {
        match self.format {
            FileFormat::Parquet => {
                parquet::execute_parquet_scan(
                    &self.operator,
                    &self.files,
                    &self.projected_schema,
                    &self.metadata_store,
                    next,
                )
                    .await?
            }
            _ => return Err(VeinError::internal("local executor supports Parquet files only")),
        }
        next.flush()
    }
}
