use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use catalog::{DataFileFormatId, MetadataStore};
use common::Result;
use common::arrow::SelectionVector;
use common::statistics::RuntimeStatisticsStore;

use crate::operators::source::scan::{
    ScanFile, ScanProvider, ScanProviderRegistry, ScanRequest, ScanSource, ScanSourceConfig,
};
use crate::expression::eval::ExpressionKernelRegistry;
use crate::runtime::filter::RuntimeFilterSet;
use crate::pipeline::{DataChunkView, PipelineNode, Source};
use functions::FunctionRegistry;
use storage::{ReadStorageFactory, StorageLocation, opendal::OpenDalReadStorageFactory};

struct EmittingProvider {
    format_id: DataFileFormatId,
    calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl ScanProvider for EmittingProvider {
    fn format_id(&self) -> &DataFileFormatId {
        &self.format_id
    }

    async fn execute(
        &self,
        _request: ScanRequest<'_>,
        next: &mut dyn PipelineNode,
    ) -> Result<()> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let values = Arc::new(Int64Array::from(vec![41, 42]));
        let view = DataChunkView::from_arrays(
            vec![values],
            2,
            SelectionVector::new_dense(2),
        )?
            .with_schema(schema)?;
        next.push(view)
    }
}

#[tokio::test]
async fn scan_source_dispatches_to_registered_provider_and_accepts_view_batches() {
    let directory = tempfile::tempdir().expect("temporary scan directory");
    let calls = Arc::new(AtomicUsize::new(0));
    let format_id = DataFileFormatId::new("test_format");
    let mut providers = ScanProviderRegistry::empty();
    providers
        .register(EmittingProvider {
            format_id: format_id.clone(),
            calls: Arc::clone(&calls),
        })
        .expect("register test scan provider");
    let provider = providers.resolve(&format_id).expect("resolve provider");
    let config = ScanSourceConfig {
        storage: OpenDalReadStorageFactory
            .open(&StorageLocation::local(directory.path().to_path_buf()))
            .expect("local storage"),
        files: Vec::<ScanFile>::new(),
        projected: Arc::new(types::VeinSchema::empty()),
        column_encoding_hints: Vec::new(),
        pushdown: None,
        runtime_filters: RuntimeFilterSet::empty(),
        metadata_store: MetadataStore::new(),
        registry: Arc::new(FunctionRegistry::new()),
        expression_kernels: Arc::new(ExpressionKernelRegistry::with_builtins()),
        runtime_statistics: RuntimeStatisticsStore::default(),
        statistics_scan: None,
    };
    let mut source = ScanSource::new(config, provider);
    let mut sink = CountingSink::default();

    source.execute(&mut sink).await.expect("injected reader should run");

    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(sink.rows, 2);
}

#[derive(Default)]
struct CountingSink {
    rows: usize,
}

impl PipelineNode for CountingSink {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.rows += view.num_rows();
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}
