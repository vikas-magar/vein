//! Pipeline construction abstraction over the wire protocol.

use std::sync::Arc;

use crate::operators::source::scan::ScanWorkerState;
use crate::pipeline::{DataChunkView, PipelineNode, Sink, Source, Transform};
use crate::runtime::filter::{
    ConservativeRuntimeFilterPolicy, RuntimeFilterDecisionPolicy, RuntimeFilterFactory,
    RuntimeFilterSet,
};
use crate::runtime::split_scan_by_file;
use common::Result;
use common::identifiers::TaskId;
use common::protocol::{DataSink, DataSource, ExpressionProgram};
use common::statistics::QueryStatisticsContext;
use functions::FunctionRegistry;

/// Builds pipeline stages for a task from the wire protocol.
///
/// Decouples `exec` from `common::protocol`: the executor's own
/// [`Source`], [`Transform`], and [`Sink`] implementations are selected here
/// from the self-contained task description a worker receives.
pub trait ExecutorBuilder: Send + Sync {
    /// Binds one built-in operator's shared state for its requested resumable phases.
    fn prepare_pipeline_operator(
        &self,
        node: &Arc<common::protocol::PhysicalPlanNode>,
        _metadata: Arc<crate::pipeline::resumable::OperatorMetadata>,
        _phases: &[common::protocol::pipeline_graph::PipelinePhase],
    ) -> Result<Arc<dyn crate::pipeline::resumable::executable::PipelineOperatorState>> {
        Err(common::VeinError::unsupported(format!(
            "resumable phases are not yet provided for '{}'", node.operator.data_flow().operator), None))
    }

    /// Extension factories supplied by the embedding application at startup.
    fn operator_factory_registry(&self) -> Option<Arc<crate::pipeline::resumable::OperatorFactoryRegistry>> {
        None
    }

    /// Registry shared by scalar and aggregate execution for this executor.
    fn function_registry(&self) -> Option<Arc<FunctionRegistry>> {
        None
    }

    /// Maximum number of file scan workers used by one local pipeline.
    ///
    /// Builders can override this to align scan concurrency with their storage
    /// backend or resource budget. The default follows the process CPU
    /// parallelism and always returns at least one worker.
    fn max_parallelism(&self) -> usize {
        std::thread::available_parallelism().map_or(1, |parallelism| parallelism.get())
    }

    /// Decompose a scan into storage-reader work units that can be scheduled
    /// independently. The default uses file boundaries; source adapters may
    /// expose finer units when cached source metadata allows it. Returning no
    /// units means the adapter proved the scan empty.
    fn partition_scan_source(
        &self,
        source: &DataSource,
        _max_parallelism: usize,
    ) -> Result<Vec<DataSource>> {
        Ok(split_scan_by_file(source))
    }

    /// Decompose a scan with access to the runtime filters produced by an
    /// upstream join build. Adapters can use exact metadata to reject work
    /// units before scheduling. The default preserves adapters that only
    /// support ordinary source partitioning.
    fn partition_scan_source_with_runtime_filters(
        &self,
        source: &DataSource,
        max_parallelism: usize,
        _runtime_filters: Option<&RuntimeFilterSet>,
    ) -> Result<Vec<DataSource>> {
        self.partition_scan_source(source, max_parallelism)
    }

    /// Build a source from the task's [`DataSource`] and attempt identity.
    fn build_source(
        &self,
        source: &DataSource,
        task_id: TaskId,
        attempt: u32,
    ) -> Result<Box<dyn Source>>;

    /// Build a source with optional execution-local filters.
    ///
    /// The default adapter ignores filters, preserving exact source behavior for
    /// builders and formats that do not implement early filtering.
    fn build_source_with_runtime_filters(
        &self,
        source: &DataSource,
        task_id: TaskId,
        attempt: u32,
        filters: &RuntimeFilterSet,
    ) -> Result<Box<dyn Source>> {
        let _ = filters;
        self.build_source(source, task_id, attempt)
    }

    /// Build a source with this query's shared statistics context.
    ///
    /// Adapters that collect source feedback can override this method. Other
    /// adapters retain their existing source behavior through the default.
    fn build_source_with_query_context(
        &self,
        source: &DataSource,
        task_id: TaskId,
        attempt: u32,
        runtime_filters: Option<&RuntimeFilterSet>,
        _query_statistics: Option<Arc<QueryStatisticsContext>>,
        _worker_state: Arc<ScanWorkerState>,
    ) -> Result<Box<dyn Source>> {
        match runtime_filters {
            Some(filters) if !filters.is_empty() => {
                self.build_source_with_runtime_filters(source, task_id, attempt, filters)
            }
            _ => self.build_source(source, task_id, attempt),
        }
    }

    /// Whether this builder's source adapter can apply filters to this source.
    ///
    /// The default declines pushdown. Builders should opt in only when their
    /// source implementation can evaluate the supplied columns before
    /// materializing the projected payload. Returning `false` keeps the join on
    /// the ordinary exact path.
    fn supports_runtime_filter_pushdown(
        &self,
        source: &DataSource,
        columns: &[types::ColumnId],
    ) -> bool {
        let _ = (source, columns);
        false
    }

    /// Optional strategy for creating execution-local filters after a join build.
    fn runtime_filter_factory(&self) -> Option<Arc<dyn RuntimeFilterFactory>> {
        None
    }

    /// Policy for choosing whether to build and apply join runtime filters.
    ///
    /// The default skips filter setup only when the direct probe scan is known
    /// to contain no rows, and otherwise applies every safe candidate. Engines
    /// can inject a different policy using the same runtime measurements.
    fn runtime_filter_decision_policy(&self) -> Arc<dyn RuntimeFilterDecisionPolicy> {
        Arc::new(ConservativeRuntimeFilterPolicy)
    }

    /// Build an expression transform over reference-backed pipeline batches.
    fn build_expression_executor(
        &self,
        program: &Arc<ExpressionProgram>,
        next: Box<dyn PipelineNode>,
    ) -> Result<Box<dyn Transform>>;

    /// Build a sink from the task's [`DataSink`]. The sink is the terminal
    /// stage; a sink that returns data (e.g. `DataSink::Client`) exposes it via
    /// [`Sink::collect`].
    fn build_sink(&self, sink: &DataSink) -> Result<Box<dyn Sink>>;

    /// Build a sink that forwards client batches through a bounded output channel.
    ///
    /// Non-client sinks may use the regular sink implementation because their
    /// results are persisted or discarded rather than returned to the client.
    fn build_streaming_sink(
        &self,
        sink: &DataSink,
        output: flume::Sender<DataChunkView>,
    ) -> Result<Box<dyn Sink>> {
        let _ = output;
        self.build_sink(sink)
    }
}
