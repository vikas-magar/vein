use super::input::MemoryFeedBuilder;
use super::*;

/// Supplies one array-backed view for each synthetic file in a scan.
pub(in crate::runtime::tests) struct MultiFileFeedBuilder {
    inner: crate::factory::DefaultExecutorBuilder,
    pub(in crate::runtime::tests) source_arrays: Arc<Mutex<Vec<Arc<arrow::array::Int64Array>>>>,
    pub(in crate::runtime::tests) active_sources: Arc<std::sync::atomic::AtomicUsize>,
    pub(in crate::runtime::tests) peak_sources: Arc<std::sync::atomic::AtomicUsize>,
}

impl MultiFileFeedBuilder {
    pub(in crate::runtime::tests) fn new() -> Self {
        Self {
            inner: crate::factory::DefaultExecutorBuilder::new(Arc::new(
                functions::FunctionRegistry::new(),
            )),
            source_arrays: Arc::new(Mutex::new(Vec::new())),
            active_sources: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            peak_sources: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }
}

pub(in crate::runtime::tests) struct ActiveSourceGuard(Arc<std::sync::atomic::AtomicUsize>);

impl Drop for ActiveSourceGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

pub(in crate::runtime::tests) struct TrackedFileSource {
    view: Option<DataChunkView>,
    active_sources: Arc<std::sync::atomic::AtomicUsize>,
    peak_sources: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait]
impl Source for TrackedFileSource {
    async fn execute(&mut self, next: &mut dyn PipelineNode) -> Result<()> {
        let active = self.active_sources.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak_sources.fetch_max(active, Ordering::Relaxed);
        let _active = ActiveSourceGuard(Arc::clone(&self.active_sources));
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        let view = self
            .view
            .take()
            .ok_or_else(|| VeinError::internal("synthetic source view already consumed"))?;
        next.push(view).await
    }
}

impl ExecutorBuilder for MultiFileFeedBuilder {
    fn max_parallelism(&self) -> usize {
        2
    }

    fn build_source(
        &self,
        source: &DataSource,
        task_id: TaskId,
        attempt: u32,
    ) -> Result<Box<dyn Source>> {
        let DataSource::File(scan) = source else {
            return self.inner.build_source(source, task_id, attempt);
        };
        let file = scan
            .files
            .first()
            .ok_or_else(|| VeinError::internal("synthetic scan source has no file"))?;
        let value = file
            .strip_suffix(".parquet")
            .and_then(|value| value.parse::<i64>().ok())
            .ok_or_else(|| VeinError::internal("synthetic scan file has an invalid value"))?;
        let values = Arc::new(arrow::array::Int64Array::from(vec![value]));
        self.source_arrays
            .lock()
            .map_err(|_| VeinError::internal("test source array mutex poisoned"))?
            .push(Arc::clone(&values));
        let array: arrow::array::ArrayRef = values;
        let view = DataChunkView::from_dense_arrays(vec![array])?;
        Ok(Box::new(TrackedFileSource {
            view: Some(view),
            active_sources: Arc::clone(&self.active_sources),
            peak_sources: Arc::clone(&self.peak_sources),
        }))
    }

    fn build_expression_executor(
        &self,
        program: &Arc<ExpressionProgram>,
        next: Box<dyn PipelineNode>,
    ) -> Result<Box<dyn Transform>> {
        self.inner.build_expression_executor(program, next)
    }

    fn build_sink(&self, sink: &DataSink) -> Result<Box<dyn Sink>> {
        self.inner.build_sink(sink)
    }

    fn build_streaming_sink(
        &self,
        sink: &DataSink,
        output: flume::Sender<DataChunkView>,
    ) -> Result<Box<dyn Sink>> {
        self.inner.build_streaming_sink(sink, output)
    }
}

pub(in crate::runtime::tests) fn runner() -> DefaultTaskExecutor {
    DefaultTaskExecutor::new(Arc::new(MemoryFeedBuilder::new()))
}

pub(in crate::runtime::tests) fn runner_with_values(values: Vec<i64>) -> DefaultTaskExecutor {
    DefaultTaskExecutor::new(Arc::new(MemoryFeedBuilder::with_values(values)))
}
