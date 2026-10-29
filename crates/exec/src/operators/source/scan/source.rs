//! Source operator that dispatches a scan through its resolved provider.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use catalog::MetadataStore;
use common::Result;
use common::protocol::{ColumnEncodingHint, RuntimeStatisticsScan, ScanPredicate};
use common::statistics::{QueryStatisticsContext, RuntimeStatisticsStore};
use functions::FunctionRegistry;
use storage::{ReadStorage, StorageFileMetadata};
use types::VeinSchemaRef;

use crate::expression::eval::ExpressionKernelProvider;
use crate::pipeline::{PipelineNode, Source};
use crate::runtime::filter::RuntimeFilterSet;

use super::{ScanProvider, ScanRequest, ScanWorkerState};

/// One file of a scan, identified by its storage path and canonical metadata key.
#[derive(Debug, Clone)]
pub struct ScanFile {
    /// Path relative to the storage handle's root.
    pub relative: String,
    /// Local path used by native readers; empty for non-local storage objects.
    pub absolute: PathBuf,
    /// Catalog-observed storage version for cache-safe metadata reuse.
    pub storage_metadata: Option<StorageFileMetadata>,
    /// Optional range of provider-defined scan units assigned to this file.
    pub scan_unit_range: Option<std::ops::Range<usize>>,
}

/// Inputs required to construct a source scan.
pub struct ScanSourceConfig {
    /// Root-scoped storage interface used to read source files.
    pub storage: Arc<dyn ReadStorage>,
    /// Source files and provider-defined unit ranges.
    pub files: Vec<ScanFile>,
    /// Columns emitted by the scan.
    pub projected: VeinSchemaRef,
    /// Best-effort requested physical output encodings.
    pub column_encoding_hints: Vec<ColumnEncodingHint>,
    /// Predicate evaluated by the provider before materializing rows.
    pub pushdown: Option<Arc<ScanPredicate>>,
    /// Query-local necessary-condition filters for source adapters.
    pub runtime_filters: RuntimeFilterSet,
    /// Shared cache used by format readers.
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

/// Source that delegates execution to a provider resolved by the executor.
pub struct ScanSource {
    worker_state: Arc<ScanWorkerState>,
    config: ScanSourceConfig,
    provider: Arc<dyn ScanProvider>,
    query_statistics: Option<Arc<QueryStatisticsContext>>,
    bytes_read: Arc<AtomicU64>,
}

impl ScanSource {
    /// Creates a scan with its already-resolved format provider.
    pub fn new(config: ScanSourceConfig, provider: Arc<dyn ScanProvider>) -> Self {
        Self::new_with_worker_state(config, provider, Arc::default())
    }

    /// Creates a scan using state retained by its scheduling worker.
    pub fn new_with_worker_state(
        config: ScanSourceConfig,
        provider: Arc<dyn ScanProvider>,
        worker_state: Arc<ScanWorkerState>,
    ) -> Self {
        Self {
            worker_state,
            config,
            provider,
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
}

#[async_trait::async_trait]
impl Source for ScanSource {
    fn into_cursor(self: Box<Self>) -> Result<super::ScanCursor<'static>> {
        Ok(ScanSource::into_cursor(*self))
    }

    async fn execute(&mut self, next: &mut dyn PipelineNode) -> Result<()> {
        self.provider.execute(self.request(), next).await?;
        next.flush().await
    }

    fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }
}

impl ScanSource {
    /// Transfers source ownership to a cursor that can outlive its factory call.
    /// The pinned stream owns all configuration; no self-referential struct is built.
    pub fn into_cursor(self) -> super::ScanCursor<'static> {
        use futures::StreamExt;
        Box::pin(async_stream::try_stream! {
            let mut cursor = self.provider.open(self.request());
            while let Some(view) = cursor.next().await { yield view?; }
        })
    }

    fn request(&self) -> ScanRequest<'_> {
        let config = &self.config;
        ScanRequest {
            worker_state: &self.worker_state,
            storage: &config.storage,
            files: &config.files,
            projected: &config.projected,
            column_encoding_hints: &config.column_encoding_hints,
            pushdown: config.pushdown.as_deref(),
            metadata_store: &config.metadata_store,
            registry: &config.registry,
            expression_kernels: &config.expression_kernels,
            bytes_read: &self.bytes_read,
            runtime_filters: &config.runtime_filters,
            runtime_statistics: &config.runtime_statistics,
            statistics_scan: config.statistics_scan.as_ref(),
            query_statistics: self.query_statistics.as_ref(),
        }
    }
}
