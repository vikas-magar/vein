use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::pipeline::{PipelineNode, Source};
use crate::expression::eval::ExpressionKernelProvider;
use crate::runtime::filter::RuntimeFilterSet;
use functions::FunctionRegistry;
use catalog::MetadataStore;
use common::protocol::FileFormat;
use common::protocol::{RuntimeStatisticsScan, ScanPredicate};
use common::statistics::{QueryStatisticsContext, RuntimeStatisticsStore};
use common::Result;
use error::VeinError;
use opendal::Operator;
use types::VeinSchemaRef;

mod reader_operator;
pub use reader_operator::{ArrowParquetScanReader, ParquetScanReader};

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

/// Inputs required to construct one source scan.
pub struct ScanSourceConfig {
    /// Storage operator used to read the source files.
    pub operator: Operator,
    /// Source files and their reader-defined unit ranges.
    pub files: Vec<ScanFile>,
    /// Encoding format of the files.
    pub format: FileFormat,
    /// Columns emitted by the scan.
    pub projected: VeinSchemaRef,
    /// Projected columns whose Parquet dictionary encoding should be retained.
    pub dictionary_columns: Vec<u32>,
    /// Predicate evaluated by the source reader before materializing rows.
    pub pushdown: Option<Arc<ScanPredicate>>,
    /// Query-local necessary-condition filters for source adapters.
    pub runtime_filters: RuntimeFilterSet,
    /// Shared metadata cache used by format readers.
    pub metadata_store: MetadataStore,
    /// Functions available to compiled scan predicates.
    pub registry: Arc<FunctionRegistry>,
    /// Kernels shared by pipeline transforms and source predicates.
    pub expression_kernels: Arc<dyn ExpressionKernelProvider>,
    /// Background collector for lazily computed source statistics.
    pub runtime_statistics: RuntimeStatisticsStore,
    /// Scan identity and coverage information for synopsis completion.
    pub statistics_scan: Option<RuntimeStatisticsScan>,
}

/// Reads local Parquet files into the execution pipeline.
pub struct ScanSource {
    operator: Operator,
    files: Vec<ScanFile>,
    format: FileFormat,
    parquet_reader: Arc<dyn ParquetScanReader>,
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
    expression_kernels: Arc<dyn ExpressionKernelProvider>,
    runtime_statistics: RuntimeStatisticsStore,
    statistics_scan: Option<RuntimeStatisticsScan>,
    query_statistics: Option<Arc<QueryStatisticsContext>>,
    /// Bytes this scan has read from storage, shared with the Parquet reader so
    /// the runtime can report real I/O volume in execution metrics.
    bytes_read: Arc<AtomicU64>,
}

impl ScanSource {
    /// Creates a scan from its source and runtime options.
    pub fn new(config: ScanSourceConfig) -> Self {
        Self {
            operator: config.operator,
            files: config.files,
            format: config.format,
            parquet_reader: Arc::new(ArrowParquetScanReader),
            projected: config.projected,
            dictionary_columns: config.dictionary_columns,
            pushdown: config.pushdown,
            runtime_filters: config.runtime_filters,
            metadata_store: config.metadata_store,
            registry: config.registry,
            expression_kernels: config.expression_kernels,
            runtime_statistics: config.runtime_statistics,
            statistics_scan: config.statistics_scan,
            query_statistics: None,
            bytes_read: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Attaches the owning query's shared statistics context.
    pub fn with_query_statistics_context(
        mut self,
        context: Arc<QueryStatisticsContext>,
    ) -> Self {
        self.query_statistics = Some(context);
        self
    }

    /// Selects the Parquet scan backend used by this source.
    ///
    /// Readers emit pipeline batches directly, so a backend can preserve row
    /// selections in `DataChunkView` instead of compacting payload columns.
    pub fn with_parquet_reader(
        mut self,
        reader: Arc<dyn ParquetScanReader>,
    ) -> Self {
        self.parquet_reader = reader;
        self
    }
}

#[async_trait::async_trait]
impl Source for ScanSource {
    async fn execute(&mut self, next: &mut dyn PipelineNode) -> Result<()> {
        match self.format {
            FileFormat::Parquet => {
                self.parquet_reader.execute(
                    parquet::ParquetScanRequest {
                        operator: &self.operator,
                        files: &self.files,
                        projected: &self.projected,
                        dictionary_columns: &self.dictionary_columns,
                        pushdown: self.pushdown.as_deref(),
                        metadata_store: &self.metadata_store,
                        registry: &self.registry,
                        expression_kernels: &self.expression_kernels,
                        bytes_read: &self.bytes_read,
                        runtime_filters: &self.runtime_filters,
                        runtime_statistics: &self.runtime_statistics,
                        statistics_scan: self.statistics_scan.as_ref(),
                        query_statistics: self.query_statistics.as_ref(),
                    },
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
