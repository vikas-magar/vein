use std::sync::atomic::Ordering;
use std::sync::Arc;

use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use catalog::{CatalogProvider, DataFileFormatId, MetadataStore, TableMetadata};
use common::Result;
use common::arrow::SelectionVector;
use common::statistics::RuntimeStatisticsStore;
use common::statistics::analysis::{StatisticsAnalysisProvider, StatisticsAnalysisSnapshot};
use error::VeinError;
use exec::operators::sink::{WriteStorage, WriteTransaction};
use exec::operators::source::scan::{ScanProvider, ScanProviderRegistry, ScanRequest};
use exec::pipeline::{DataChunkView, PipelineNode};
use functions::FunctionRegistry;
use futures::TryStreamExt;
use plan::{ColumnId, ResolvedColumn, VeinDataType, VeinSchema};
use std::sync::atomic::AtomicUsize;

use super::{LocalEngineBuilder, LocalEngineComponents, StatisticsCheckpointPolicy};

struct MemoryCatalog {
    table: Arc<TableMetadata>,
    metadata: MetadataStore,
}

#[async_trait]
impl CatalogProvider for MemoryCatalog {
    async fn get_table(&self, name: &str) -> Result<Arc<TableMetadata>> {
        (name == self.table.name)
            .then(|| Arc::clone(&self.table))
            .ok_or_else(|| VeinError::invalid_input(format!("unknown test table '{name}'")))
    }

    fn metadata_store(&self) -> MetadataStore {
        self.metadata.clone()
    }
}

struct MemoryScanProvider {
    format: DataFileFormatId,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl ScanProvider for MemoryScanProvider {
    fn format_id(&self) -> &DataFileFormatId {
        &self.format
    }

    async fn execute(&self, _request: ScanRequest<'_>, next: &mut dyn PipelineNode) -> Result<()> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let values = Arc::new(Int64Array::from(vec![41, 42]));
        next.push(
            DataChunkView::from_arrays(vec![values], 2, SelectionVector::new_dense(2))?
                .with_schema(schema)?,
        )
    }
}

struct UnusedWriteStorage;

impl WriteStorage for UnusedWriteStorage {
    fn validate_target(&self, _uri: &str, _options: &[(String, String)]) -> Result<()> {
        Ok(())
    }

    fn begin_write(
        &self,
        _target: &common::protocol::StorageTarget,
    ) -> Result<Box<dyn WriteTransaction>> {
        Err(VeinError::unsupported("test engine is read-only", None))
    }
}

struct UnusedAnalysisProvider;

#[async_trait]
impl StatisticsAnalysisProvider for UnusedAnalysisProvider {
    async fn open_analysis(&self, _table: &str, _batch_size: usize) -> Result<StatisticsAnalysisSnapshot> {
        Err(VeinError::unsupported("test analysis is unavailable", None))
    }

    async fn publish_analysis(
        &self,
        _table: &str,
        _statistics: &catalog::AnalyzedTableStatistics,
    ) -> Result<()> {
        Err(VeinError::unsupported("test analysis is unavailable", None))
    }
}

#[test]
fn injected_catalog_and_scan_provider_execute_with_shared_query_statistics() {
    let directory = tempfile::tempdir().expect("temporary scan root");
    std::fs::File::create(directory.path().join("items.data"))
        .expect("create placeholder scan object");
    let column = Arc::new(ResolvedColumn::new(ColumnId(0), "id", None, VeinDataType::Int64));
    let schema = Arc::new(VeinSchema::new(vec![column]));
    let format = DataFileFormatId::new("memory_test");
    let table = Arc::new(
        TableMetadata::new("items", Arc::clone(&schema))
            .with_location(directory.path().to_string_lossy())
            .with_file_format(format.clone())
            .with_files(vec![
                directory.path().join("items.data").to_string_lossy().into_owned(),
            ]),
    );
    let catalog = Arc::new(MemoryCatalog { table, metadata: MetadataStore::new() });
    let calls = Arc::new(AtomicUsize::new(0));
    let mut scan_providers = ScanProviderRegistry::empty();
    scan_providers.register(MemoryScanProvider { format, calls: Arc::clone(&calls) })
        .expect("register scan provider");
    let runtime_statistics = RuntimeStatisticsStore::default();
    let components = LocalEngineComponents {
        catalog,
        functions: Arc::new(FunctionRegistry::with_builtins()),
        scan_providers,
        read_storage_factory: Arc::new(storage::opendal::OpenDalReadStorageFactory),
        write_storage: Arc::new(UnusedWriteStorage),
        statistics_provider: Arc::new(UnusedAnalysisProvider),
        runtime_statistics,
        optimizer: Arc::new(optimizer::OptimizerPipeline::default()),
        planner: Arc::new(physical::DefaultPhysicalPlanner::new()),
        adaptive_policy: Arc::new(StatisticsCheckpointPolicy),
    };
    let engine = LocalEngineBuilder::new(components).build();

    tokio::runtime::Runtime::new().expect("Tokio runtime").block_on(async {
        let query = engine.prepare("FROM items |> SELECT id").await.expect("prepare query");
        assert!(Arc::ptr_eq(
            query.statistics_context(),
            query.tasks[0].statistics_context.as_ref().expect("task statistics context"),
        ));
        let batches = query.into_stream().try_collect::<Vec<_>>().await.expect("execute query");
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(batches.iter().map(DataChunkView::num_rows).sum::<usize>(), 2);
        let values = batches[0].materialize_column(0).expect("output column");
        let values = values.as_any().downcast_ref::<Int64Array>().expect("Int64 output");
        assert_eq!(values.values(), &[41, 42]);
    });
}
