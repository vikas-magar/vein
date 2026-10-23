use std::sync::Arc;

use crate::expression::executor::ExpressionExecutor;
use crate::operators::sink::{CollectSink, DiscardSink, WriteSink};
use crate::operators::source::scan::ScanWorkerState;
use crate::pipeline::{DataChunkView, PipelineNode, Sink, Source, Transform};
use crate::runtime::ExecutorBuilder;
use crate::runtime::filter::{RuntimeFilterDecisionPolicy, RuntimeFilterFactory, RuntimeFilterSet};
use common::Result;
use common::identifiers::TaskId;
use common::protocol::{DataSink, DataSource, ExpressionProgram};
use common::statistics::QueryStatisticsContext;
use error::VeinError;
use functions::FunctionRegistry;
use types::ColumnId;

use super::DefaultExecutorBuilder;
use crate::factory::scan_partition::partition_source;

#[async_trait::async_trait]
impl ExecutorBuilder for DefaultExecutorBuilder {
    fn function_registry(&self) -> Option<Arc<FunctionRegistry>> {
        Some(Arc::clone(&self.registry))
    }

    fn partition_scan_source(
        &self,
        source: &DataSource,
        max_parallelism: usize,
    ) -> Result<Vec<DataSource>> {
        partition_source(source, &self.scan_providers, &self.metadata_store, max_parallelism, None)
    }

    fn partition_scan_source_with_runtime_filters(
        &self,
        source: &DataSource,
        max_parallelism: usize,
        runtime_filters: Option<&RuntimeFilterSet>,
    ) -> Result<Vec<DataSource>> {
        partition_source(
            source,
            &self.scan_providers,
            &self.metadata_store,
            max_parallelism,
            runtime_filters,
        )
    }

    fn build_source(
        &self,
        source: &DataSource,
        _task_id: TaskId,
        _attempt: u32,
    ) -> Result<Box<dyn Source>> {
        self.build_source_inner(source, &RuntimeFilterSet::empty(), None, Arc::default())
    }

    fn build_source_with_runtime_filters(
        &self,
        source: &DataSource,
        _task_id: TaskId,
        _attempt: u32,
        filters: &RuntimeFilterSet,
    ) -> Result<Box<dyn Source>> {
        self.build_source_inner(source, filters, None, Arc::default())
    }

    fn build_source_with_query_context(
        &self,
        source: &DataSource,
        _task_id: TaskId,
        _attempt: u32,
        runtime_filters: Option<&RuntimeFilterSet>,
        query_statistics: Option<Arc<QueryStatisticsContext>>,
        worker_state: Arc<ScanWorkerState>,
    ) -> Result<Box<dyn Source>> {
        self.build_source_inner(
            source,
            runtime_filters.unwrap_or(&RuntimeFilterSet::empty()),
            query_statistics,
            worker_state,
        )
    }

    fn supports_runtime_filter_pushdown(&self, source: &DataSource, columns: &[ColumnId]) -> bool {
        let DataSource::File(scan) = source else {
            return false;
        };
        self.scan_providers.resolve(&scan.data_file_format).is_ok_and(|provider| {
            provider.supports_runtime_filter_pushdown(scan, columns)
        })
    }

    fn runtime_filter_factory(&self) -> Option<Arc<dyn RuntimeFilterFactory>> {
        Some(Arc::clone(&self.runtime_filter_factory))
    }

    fn runtime_filter_decision_policy(&self) -> Arc<dyn RuntimeFilterDecisionPolicy> {
        Arc::clone(&self.runtime_filter_policy)
    }

    fn build_expression_executor(
        &self,
        program: &Arc<ExpressionProgram>,
        next: Box<dyn PipelineNode>,
    ) -> Result<Box<dyn Transform>> {
        Ok(Box::new(ExpressionExecutor::new(
            Arc::clone(program),
            Arc::clone(&self.expression_kernels),
            Arc::clone(&self.registry),
            next,
        )))
    }

    fn build_sink(&self, sink: &DataSink) -> Result<Box<dyn Sink>> {
        match sink {
            DataSink::Discard => Ok(Box::new(DiscardSink::new())),
            DataSink::Client { schema } => Ok(Box::new(CollectSink::new(Arc::new(schema.to_arrow())))),
            DataSink::Storage(target) => {
                let storage = self.storage.as_ref().ok_or_else(|| {
                    VeinError::internal("WRITE requires a configured table storage backend")
                })?;
                Ok(Box::new(WriteSink::new(target.clone(), Arc::clone(storage))?))
            }
        }
    }

    fn build_streaming_sink(
        &self,
        sink: &DataSink,
        output: flume::Sender<DataChunkView>,
    ) -> Result<Box<dyn Sink>> {
        match sink {
            DataSink::Client { schema } => Ok(Box::new(CollectSink::with_sender(
                Arc::new(schema.to_arrow()),
                output,
            ))),
            _ => self.build_sink(sink),
        }
    }
}
