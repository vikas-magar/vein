use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::pipeline::{DataChunkView, PipelineNode, Source};
use crate::runtime_filter::RuntimeFilterSet;
use crate::udf::FunctionRegistry;
use catalog::MetadataStore;
use common::protocol::FileFormat;
use common::protocol::ScanPredicate;
use common::Result;
use error::VeinError;
use opendal::Operator;
use types::VeinSchemaRef;

/// Parquet source backed by a local-filesystem OpenDAL operator.
pub mod parquet;

pub use parquet::PARQUET_FOOTER;

/// One file of a scan, identified both by the path storage is asked for and by
/// the absolute path that keys the shared footer store.
#[derive(Debug, Clone)]
pub struct ScanFile {
    /// Path relative to the scan's OpenDAL operator root.
    pub relative: String,
    /// Canonical absolute path of the same file.
    ///
    /// Canonical because it keys the shared footer store: the catalog publishes
    /// a parsed footer under the key it resolved, and the scan has to derive the
    /// same key from the same file without resolving the path a second time.
    pub absolute: PathBuf,
    /// Optional range of reader-defined data units assigned to this file.
    /// Parquet interprets these ordinals as row groups.
    pub scan_unit_range: Option<std::ops::Range<usize>>,
}

/// Reads local Parquet files into the execution pipeline.
pub struct ScanSource {
    operator: Operator,
    files: Vec<ScanFile>,
    format: FileFormat,
    projected: VeinSchemaRef,
    /// Projected columns whose Parquet dictionary encoding should be retained.
    dictionary_columns: Vec<u32>,
    /// Predicate the reader evaluates before materializing rows.
    pushdown: Option<Arc<ScanPredicate>>,
    /// Query-local necessary-condition filters, evaluated before payload decode
    /// by source adapters that advertise support.
    runtime_filters: RuntimeFilterSet,
    metadata_store: MetadataStore,
    /// Function registry the compiled predicate program runs against.
    registry: Arc<FunctionRegistry>,
    /// Bytes this scan has read from storage, shared with the Parquet reader so
    /// the runtime can report real I/O volume in execution metrics.
    bytes_read: Arc<AtomicU64>,
}

impl ScanSource {
    /// Creates a scan over the provided files.
    pub fn new(
        operator: Operator,
        files: Vec<ScanFile>,
        format: FileFormat,
        projected: VeinSchemaRef,
        dictionary_columns: Vec<u32>,
        pushdown: Option<Arc<ScanPredicate>>,
        metadata_store: MetadataStore,
        registry: Arc<FunctionRegistry>,
    ) -> Self {
        Self::new_with_runtime_filters(
            operator,
            files,
            format,
            projected,
            dictionary_columns,
            pushdown,
            metadata_store,
            registry,
            RuntimeFilterSet::empty(),
        )
    }

    /// Creates a scan that may apply query-local runtime filters before reading
    /// the projected payload columns.
    pub fn new_with_runtime_filters(
        operator: Operator,
        files: Vec<ScanFile>,
        format: FileFormat,
        projected: VeinSchemaRef,
        dictionary_columns: Vec<u32>,
        pushdown: Option<Arc<ScanPredicate>>,
        metadata_store: MetadataStore,
        registry: Arc<FunctionRegistry>,
        runtime_filters: RuntimeFilterSet,
    ) -> Self {
        Self {
            operator,
            files,
            format,
            projected,
            dictionary_columns,
            pushdown,
            runtime_filters,
            metadata_store,
            registry,
            bytes_read: Arc::new(AtomicU64::new(0)),
        }
    }
}

#[async_trait::async_trait]
impl Source<DataChunkView> for ScanSource {
    async fn execute(&mut self, next: &mut dyn PipelineNode<DataChunkView>) -> Result<()> {
        match self.format {
            FileFormat::Parquet => {
                parquet::execute_parquet_scan(
                    &self.operator,
                    &self.files,
                    &self.projected,
                    &self.dictionary_columns,
                    self.pushdown.as_deref(),
                    &self.metadata_store,
                    &self.registry,
                    &self.bytes_read,
                    &self.runtime_filters,
                    next,
                )
                    .await?
            }
            _ => return Err(VeinError::internal("local executor supports Parquet files only")),
        }
        next.flush()
    }

    fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }
}


#[cfg(test)]
mod tests;
