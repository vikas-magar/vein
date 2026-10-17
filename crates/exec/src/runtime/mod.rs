//! The executor's execution seam: the traits that decouple pipeline
//! construction and task execution from the wire protocol and the concrete
//! stage implementations.
//!
//! These are the abstractions the rest of the engine programs against:
//! a [`ExecutorBuilder`] selects concrete [`Source`]/[`Transform`]/[`Sink`]
//! implementations from a wire task, and a [`TaskExecutor`] runs a single
//! task end-to-end (see `crate::pipeline`).

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::Instrument;

use crate::memory::{ExecutionResourceLimits, QueryMemoryTracker};
use crate::operators::aggregate;
use crate::operators::join::hash_join::BuildResult;
use crate::operators::join::spill::{
    JoinSpillRun, JoinSpillSchema, JoinSpillWriter, open_reader, partition_runs, payload_view,
    remove_run,
};
use crate::operators::join::{
    BuildSink, HashJoin, HashTable, JoinFilterCollector,
};
use crate::operators::sink::{ClientOutput, CollectSink, ViewResultBatch};
use crate::operators::{LimitTransform, SortTransform};
use crate::pipeline::{DataChunk, DataChunkView, PipelineNode, Sink, Source, Transform};
use crate::runtime_filter::{
    ConservativeRuntimeFilterPolicy, RuntimeFilterBuildInput, RuntimeFilterDecisionContext,
    RuntimeFilterDecisionPolicy, RuntimeFilterFactory, RuntimeFilterSet,
};
use crate::spill::{LocalSpillStorageFactory, SpillStorageFactory};
use crate::trace::ExecutionTrace;
use common::Result;
use common::cancellation::CancellationToken;
use common::identifiers::{IntermediateSourceId, QueryId, TaskId};
use common::statistics::QueryStatisticsContext;
use common::protocol::{
    DataSink, DataSource, ExecuteTask, JoinKind, JoinPlan, MicroProgram,
    PhysicalPlanNode,
    PhysicalPlanOperator, PipelineBoundaryKind,
};
use error::VeinError;

/// Query-scoped resources shared by every task and adaptive stage in one query.
///
/// Keeping this handle alive also keeps retained checkpoint arrays accounted
/// against the same memory and spill budgets as continuation operators.
#[derive(Clone)]
pub struct QueryExecutionResources {
    memory: QueryMemoryTracker,
}

impl QueryExecutionResources {
    /// Current query memory reservation, including retained checkpoint arrays.
    pub fn memory_used_bytes(&self) -> usize {
        self.memory.used()
    }

    /// Peak query memory reservation observed across all stages.
    pub fn memory_peak_bytes(&self) -> usize {
        self.memory.peak()
    }
}

#[cfg(test)]
use arrow::datatypes::DataType;
#[cfg(test)]
use crate::operators::join::{JoinKeys, JoinType};
#[cfg(test)]
use types::VeinDataType;

mod result;
pub use result::{TaskMetrics, TaskResult};

mod materialize;

pub(crate) use materialize::MaterializeViewNode;

mod dependency_guard;
mod join_utils;
mod schedule;
mod task_executor;

use join_utils::{
    direct_scan_probe_columns, join_keys, join_outputs, join_type, join_types,
};
pub(crate) use join_utils::split_scan_by_file;

use dependency_guard::PipelineDependencyGuard;
use schedule::PipelineExecutionSchedule;


/// Builds pipeline stages for a task from the wire protocol.
///
/// Decouples `exec` from `common::protocol`: the executor's own
/// [`Source`], [`Transform`], and [`Sink`] implementations are selected here
/// from the self-contained task description a worker receives.
pub trait ExecutorBuilder: Send + Sync {
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
    /// expose finer units such as Parquet row groups when cached metadata allows
    /// it. Returning no units means the adapter proved the scan empty.
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
    ) -> Result<Box<dyn Source<DataChunkView>>>;

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
    ) -> Result<Box<dyn Source<DataChunkView>>> {
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
    ) -> Result<Box<dyn Source<DataChunkView>>> {
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

    /// Build a Vector VM transform interpreting the task's [`MicroProgram`].
    fn build_vm(
        &self,
        program: &Arc<MicroProgram>,
        next: Box<dyn PipelineNode<DataChunk>>,
    ) -> Result<Box<dyn Transform<DataChunk>>>;

    /// Build a VM transform over reference-backed pipeline batches.
    fn build_view_vm(
        &self,
        program: &Arc<MicroProgram>,
        next: Box<dyn PipelineNode<DataChunkView>>,
    ) -> Result<Box<dyn Transform<DataChunkView>>>;

    /// Build a sink from the task's [`DataSink`]. The sink is the terminal
    /// stage; a sink that returns data (e.g. `DataSink::Client`) exposes it via
    /// [`Sink::collect`].
    fn build_sink(&self, sink: &DataSink) -> Result<Box<dyn Sink<DataChunkView>>>;

    /// Build a sink that forwards client batches through a bounded output channel.
    ///
    /// Non-client sinks may use the regular sink implementation because their
    /// results are persisted or discarded rather than returned to the client.
    fn build_streaming_sink(
        &self,
        sink: &DataSink,
        output: flume::Sender<RecordBatch>,
    ) -> Result<Box<dyn Sink<DataChunkView>>> {
        let _ = output;
        self.build_sink(sink)
    }

    /// Builds an internal client result sink which retains views without gathering.
    ///
    /// Storage/discard destinations are rejected because this API returns client
    /// results. Both collected and streaming view results use the same sink.
    fn build_view_result_sink(
        &self,
        sink: &DataSink,
        output: Option<flume::Sender<ViewResultBatch>>,
    ) -> Result<Box<dyn Sink<DataChunkView, ViewResultBatch>>> {
        let DataSink::Client { schema } = sink else {
            return Err(VeinError::planning(
                "view results require a client sink",
                None,
            ));
        };
        let schema = Arc::new(schema.to_arrow());
        Ok(Box::new(match output {
            Some(sender) => CollectSink::with_sender(schema, sender),
            None => CollectSink::new(schema),
        }))
    }
}

/// Executes a single task (`source -> transforms -> sink`) end-to-end.
///
/// This is the unit of work the distributed master schedules: the executor
/// receives a self-contained [`ExecuteTask`] and runs it, returning its result.
///
/// Every future here is `Send`, so tasks can be dispatched onto a multi-threaded
/// runtime and executed concurrently; nothing in the pipeline holds `!Send`
/// state.
#[async_trait]
pub trait TaskExecutor: Send + Sync {
    /// Create one resource scope for a multi-stage query execution.
    ///
    /// Executors that do not support query-wide accounting return an error;
    /// callers can then keep the original single-stage plan.
    fn create_query_resources(
        &self,
        _cancel: CancellationToken,
    ) -> Result<Arc<QueryExecutionResources>> {
        Err(VeinError::unsupported(
            "executor does not support query-scoped resources",
            None,
        ))
    }

    /// Run one task, driving its source through the operator chain.
    async fn execute(&self, task: &ExecuteTask, cancel: CancellationToken) -> Result<TaskResult>;

    /// Executes the same canonical plan while retaining client result views.
    async fn execute_views(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
    ) -> Result<TaskResult<ViewResultBatch>>;

    /// Runs a client-result task into a query-accounted, replayable checkpoint.
    ///
    /// This is the producer-side seam for multi-stage adaptive execution. The
    /// checkpoint retains `DataChunkView`s while memory permits and spills via
    /// the same tracker used by the task.
    async fn execute_to_checkpoint(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
    ) -> Result<checkpoint::CheckpointTaskResult>;

    /// Run a checkpoint producer with the query's shared memory/spill budget.
    async fn execute_to_checkpoint_with_resources(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
        _resources: Arc<QueryExecutionResources>,
    ) -> Result<checkpoint::CheckpointTaskResult> {
        self.execute_to_checkpoint(task, cancel).await
    }

    /// Make a sealed query-local relation available to a later physical fragment.
    ///
    /// The returned guard owns the registration and removes it when dropped.
    /// Custom executors may leave this unsupported until they implement runtime
    /// source resolution.
    fn register_intermediate_source(
        &self,
        _query_id: QueryId,
        _source_id: IntermediateSourceId,
        _relation: checkpoint::IntermediateRelation,
    ) -> Result<checkpoint::IntermediateSourceRegistration> {
        Err(VeinError::unsupported(
            "executor does not support query-local intermediate sources",
            None,
        ))
    }

    /// Streams reference-backed results through a bounded channel.
    async fn execute_views_streaming(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
        output: flume::Sender<ViewResultBatch>,
    ) -> Result<TaskResult<ViewResultBatch>>;

    /// Stream a continuation using the same resource budget as earlier stages.
    async fn execute_views_streaming_with_resources(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
        output: flume::Sender<ViewResultBatch>,
        _resources: Arc<QueryExecutionResources>,
    ) -> Result<TaskResult<ViewResultBatch>> {
        self.execute_views_streaming(task, cancel, output).await
    }

    /// Stream client batches using the query-wide memory and spill budget.
    async fn execute_streaming_with_resources(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
        output: flume::Sender<RecordBatch>,
        _resources: Arc<QueryExecutionResources>,
    ) -> Result<TaskResult> {
        self.execute_streaming(task, cancel, output).await
    }

    /// Execute a task while forwarding client batches through a bounded channel.
    ///
    /// The default adapter preserves compatibility for custom executors; the
    /// default executor overrides this to avoid retaining the complete output.
    async fn execute_streaming(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
        output: flume::Sender<RecordBatch>,
    ) -> Result<TaskResult> {
        let mut result = self.execute(task, cancel).await?;
        for batch in result.output.drain(..) {
            output
                .send_async(batch)
                .await
                .map_err(|e| VeinError::internal(format!("task output channel closed: {e}")))?;
        }
        Ok(result)
    }
}

pub mod checkpoint;
mod recursive;
mod pipeline_adapters;
mod join_pipeline;

use join_pipeline::{JoinProbeCounter, SharedBuildSink, SharedJoinOutput, SpilledJoinProbe};
use pipeline_adapters::{CancelGuard, ChannelPipelineNode, SinkHandle, SuppressFlush, TransformHandle};

/// The default [`TaskExecutor`]: builds a task's pipeline via an injected
/// [`ExecutorBuilder`] and drives the source through the operator chain.
///
/// The chain is constructed sink-first so each transform knows its downstream
/// `next`; a [`CancelGuard`] is inserted just above the sink to abort on
/// cancellation. On success, the output collected by the sink is returned as a
/// [`TaskResult`].
pub struct DefaultTaskExecutor {
    builder: Arc<dyn ExecutorBuilder>,
    resource_limits: ExecutionResourceLimits,
    spill_storage_factory: Arc<dyn SpillStorageFactory>,
    intermediate_sources: checkpoint::IntermediateSourceRegistry,
}

impl DefaultTaskExecutor {
    /// Build probe-side runtime filters only when the join key is still a direct
    /// projected scan column. Unknown lineage is a reason to keep exact execution.
    fn runtime_filters_for_join(
        &self,
        probe: &PhysicalPlanNode,
        plan: &JoinPlan,
        build: &BuildResult,
        memory: &QueryMemoryTracker,
        trace: &ExecutionTrace,
        node_id: u32,
    ) -> Result<Option<Arc<RuntimeFilterSet>>> {
        if !matches!(plan.join_type, JoinKind::Inner | JoinKind::Semi) {
            return Ok(None);
        }
        let BuildResult::InMemory(table) = build else {
            tracing::debug!(
                target: "vein.audit",
                event = "join_runtime_filter_skipped",
                node_id,
                reason = "spilled_build",
                "join runtime filter skipped"
            );
            return Ok(None);
        };
        let Some((source, columns)) = direct_scan_probe_columns(probe, plan) else {
            tracing::debug!(
                target: "vein.audit",
                event = "join_runtime_filter_skipped",
                node_id,
                reason = "probe_lineage_or_scan_unsupported",
                "join runtime filter skipped"
            );
            return Ok(None);
        };
        if !self
            .builder
            .supports_runtime_filter_pushdown(source, &columns)
        {
            tracing::debug!(
                target: "vein.audit",
                event = "join_runtime_filter_skipped",
                node_id,
                reason = "source_adapter_unsupported",
                "join runtime filter skipped"
            );
            return Ok(None);
        }
        let Some(factory) = self.builder.runtime_filter_factory() else {
            tracing::debug!(
                target: "vein.audit",
                event = "join_runtime_filter_skipped",
                node_id,
                reason = "factory_unavailable",
                "join runtime filter skipped"
            );
            return Ok(None);
        };

        let build_hashes = table.runtime_filter_hashes();
        let probe_rows_estimate = match source {
            DataSource::File(scan) => scan
                .runtime_statistics
                .as_ref()
                .and_then(|statistics| statistics.expected_rows),
            _ => None,
        };
        let probe_source_key = match source {
            DataSource::File(scan) => scan
                .catalog_snapshot
                .as_ref()
                .map(common::statistics::runtime::table_statistics_key),
            _ => None,
        };
        let query_statistics = trace.query_statistics_context().snapshot();
        let source_statistics_count = query_statistics.sources.len();
        let runtime_scan_observation_count = query_statistics.scans.values().map(Vec::len).sum::<usize>();
        let runtime_operators_with_output = query_statistics
            .operators
            .iter()
            .filter(|operator| operator.output_batches > 0)
            .count();
        let runtime_operator_output_rows = query_statistics
            .operators
            .iter()
            .map(|operator| operator.output_rows)
            .sum::<u64>();
        let runtime_operator_output_batches = query_statistics
            .operators
            .iter()
            .map(|operator| operator.output_batches)
            .sum::<u64>();
        let runtime_source_bytes_observed = query_statistics
            .operators
            .iter()
            .map(|operator| operator.source_bytes_read)
            .sum::<u64>();
        let context = RuntimeFilterDecisionContext {
            build_rows: table.len() as u64,
            build_keys: build_hashes.len(),
            probe_rows_estimate,
            probe_source_key,
            available_memory_bytes: memory.limit().saturating_sub(memory.used()),
            query_statistics,
        };
        let (effective_probe_rows, probe_rows_source) = context.probe_rows();
        let policy = self.builder.runtime_filter_decision_policy();
        if !policy.should_build_candidates(&context) {
            tracing::info!(
                target: "vein.statistics",
                event = "join_runtime_filter_decision",
                node_id,
                build_rows = context.build_rows,
                build_keys = context.build_keys,
                probe_rows_estimate = ?context.probe_rows_estimate,
                effective_probe_rows,
                probe_rows_source,
                available_memory_bytes = context.available_memory_bytes,
                source_statistics_count,
                runtime_scan_observation_count,
                runtime_operators_with_output,
                runtime_operator_output_rows,
                runtime_operator_output_batches,
                runtime_source_bytes_observed,
                candidate_construction = "skipped",
                selected_filters = 0usize,
                reason = "policy_declined_setup",
                "join runtime filter decision"
            );
            return Ok(None);
        }

        let build_key_bounds = table.runtime_key_bounds()?;
        let bounded_key_columns = build_key_bounds.iter().filter(|bounds| bounds.is_some()).count();
        let filters = factory.build_filters(RuntimeFilterBuildInput {
            probe_columns: &columns,
            build_hashes,
            build_key_bounds,
            memory,
        })?;
        if filters.is_empty() {
            tracing::debug!(
                target: "vein.audit",
                event = "join_runtime_filter_skipped",
                node_id,
                reason = "factory_declined",
                build_hashes = build_hashes.len(),
                "join runtime filter skipped"
            );
            return Ok(None);
        }

        let candidate_names = filters.iter().map(|filter| filter.name()).collect::<Vec<_>>();
        let selected_indices = policy.select_filters(&context, &filters)?;
        let statistics = trace
            .operator_statistics_handle(node_id)
            .ok_or_else(|| VeinError::internal("join node is missing query statistics"))?;
        let selected_filters = RuntimeFilterSet::from_selection_with_statistics(
            &filters,
            &selected_indices,
            statistics,
        )?;
        if selected_filters.is_empty() {
            tracing::info!(
                target: "vein.statistics",
                event = "join_runtime_filter_decision",
                node_id,
                build_rows = context.build_rows,
                build_keys = context.build_keys,
                probe_rows_estimate = ?context.probe_rows_estimate,
                effective_probe_rows,
                probe_rows_source,
                available_memory_bytes = context.available_memory_bytes,
                source_statistics_count,
                runtime_scan_observation_count,
                runtime_operators_with_output,
                runtime_operator_output_rows,
                runtime_operator_output_batches,
                runtime_source_bytes_observed,
                candidate_construction = "completed",
                candidate_filters = ?candidate_names,
                selected_filters = 0usize,
                reason = "policy_selected_no_filters",
                "join runtime filter decision"
            );
            return Ok(None);
        }

        let filters = Arc::new(selected_filters);
        tracing::info!(
            target: "vein.statistics",
            event = "join_runtime_filter_decision",
            node_id,
            build_rows = context.build_rows,
            build_keys = context.build_keys,
            probe_rows_estimate = ?context.probe_rows_estimate,
            effective_probe_rows,
            probe_rows_source,
            available_memory_bytes = context.available_memory_bytes,
            source_statistics_count,
            runtime_scan_observation_count,
            runtime_operators_with_output,
            runtime_operator_output_rows,
            runtime_operator_output_batches,
            runtime_source_bytes_observed,
            candidate_construction = "completed",
            candidate_filters = ?candidate_names,
            filter_names = ?filters.names(),
            selected_filters = filters.len(),
            key_columns = columns.len(),
            indexed_build_keys = build_hashes.len(),
            bounded_key_columns,
            filter_bytes = filters.memory_bytes(),
            reason = "policy_selected_candidates",
            "join runtime filter decision"
        );
        Ok(Some(filters))
    }

    fn trace_data_flow(
        node: &PhysicalPlanNode,
        task: &ExecuteTask,
        trace: &ExecutionTrace,
        depth: usize,
    ) {
        let contract = node.operator.data_flow();
        tracing::debug!(
            target: "vein.audit",
            event = "physical_operator_data_flow",
            query_id = %task.query_id,
            task_id = %task.task_id,
            node_id = trace.node_id(node),
            depth,
            operator = contract.operator,
            input_materialization = ?contract.input_materialization,
            output_representation = ?contract.output,
            output_columns = node.output_schema.fields.len(),
            "physical_operator_data_flow"
        );
    }

    /// Create a task executor that builds pipelines with `builder`.
    pub fn new(builder: Arc<dyn ExecutorBuilder>) -> Self {
        Self {
            builder,
            resource_limits: ExecutionResourceLimits::default(),
            spill_storage_factory: Arc::new(LocalSpillStorageFactory),
            intermediate_sources: checkpoint::IntermediateSourceRegistry::default(),
        }
    }

    /// Create a task executor with a per-execution budget for blocking state.
    pub fn with_memory_limit(builder: Arc<dyn ExecutorBuilder>, memory_limit: usize) -> Self {
        Self::with_resource_limits(
            builder,
            ExecutionResourceLimits::new(memory_limit, u64::MAX),
        )
    }

    /// Create a task executor with memory and spill-disk limits per execution.
    pub fn with_resource_limits(
        builder: Arc<dyn ExecutorBuilder>,
        resource_limits: ExecutionResourceLimits,
    ) -> Self {
        Self {
            builder,
            resource_limits,
            spill_storage_factory: Arc::new(LocalSpillStorageFactory),
            intermediate_sources: checkpoint::IntermediateSourceRegistry::default(),
        }
    }

    /// Use a factory that creates one isolated spill backend per query.
    pub fn with_spill_storage_factory(mut self, factory: Arc<dyn SpillStorageFactory>) -> Self {
        self.spill_storage_factory = factory;
        self
    }

    /// Execute a recursive input tree with bounded batches between operators.
    async fn execute_with_output<O: ClientOutput>(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
        make_sink: impl FnOnce(
            &dyn ExecutorBuilder,
            &DataSink,
            &QueryMemoryTracker,
        ) -> Result<Box<dyn Sink<DataChunkView, O>>>
        + Send,
    ) -> Result<TaskResult<O>> {
        self.execute_with_output_and_resources(task, cancel, None, make_sink)
            .await
    }

    async fn execute_with_output_and_resources<O: ClientOutput>(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
        resources: Option<Arc<QueryExecutionResources>>,
        make_sink: impl FnOnce(
            &dyn ExecutorBuilder,
            &DataSink,
            &QueryMemoryTracker,
        ) -> Result<Box<dyn Sink<DataChunkView, O>>>
        + Send,
    ) -> Result<TaskResult<O>> {
        let start = Instant::now();
        if cancel.is_cancelled() {
            return Err(VeinError::cancelled());
        }
        let query_id = task.query_id.to_string();
        let task_id = task.task_id.to_string();
        let statistics_context = task.statistics_context.clone()
            .unwrap_or_else(|| Arc::new(QueryStatisticsContext::new([])));
        let mut trace = ExecutionTrace::new_with_query_context(
            &task.plan.root,
            query_id.clone(),
            task_id.clone(),
            task.task_id.into_raw(),
            statistics_context,
        );
        // Emit the compact plan before validation so malformed plans still have
        // node IDs, schemas, mappings, and flow contracts in the debug trace.
        trace.log_plan();
        Self::trace_data_flow(&task.plan.root, task, &trace, 0);
        task.plan.validate()?;
        let PhysicalPlanOperator::Sink {
            input,
            sink: sink_plan,
        } = &task.plan.root.operator
        else {
            return Err(VeinError::planning(
                "physical plan root must be a sink",
                None,
            ));
        };
        let schedule = PipelineExecutionSchedule::derive(&task.plan, &trace)?;
        let memory = match resources {
            Some(resources) => resources.memory.clone(),
            None => {
                let spill_storage = self.spill_storage_factory.create_for_query()?;
                QueryMemoryTracker::with_spill_storage(
                    self.resource_limits,
                    cancel.clone(),
                    spill_storage,
                )
            }
        };
        trace.attach_memory_tracker(memory.clone());

        let sink_started = Instant::now();
        let sink = make_sink(self.builder.as_ref(), sink_plan, &memory)?;
        let sink: Arc<Mutex<Box<dyn Sink<DataChunkView, O>>>> = Arc::new(Mutex::new(sink));
        let sink_build_us = sink_started.elapsed().as_micros() as u64;
        let task_span = tracing::info_span!(
            target: "vein.audit",
            "task_execution",
            request_id = %task.request_id,
            query_id = %task.query_id,
            stage_id = %task.stage_id,
            task_id = %task.task_id,
            attempt = task.attempt,
        );
        let head: Box<dyn PipelineNode<DataChunkView>> = Box::new(CancelGuard::new(
            Box::new(SinkHandle(Arc::clone(&sink))),
            cancel.clone(),
        ));
        let run_started = Instant::now();
        let source_started = Instant::now();
        let mut root_guard = trace.execution_guard(trace.node_id(&task.plan.root));
        let execution_context = recursive::RecursiveExecutionContext {
            task,
            cancel: &cancel,
            memory: &memory,
            span: &task_span,
            schedule: &schedule,
            trace: &trace,
        };
        let execution_result = self
            .execute_input(input, head, &execution_context, 0)
            .await;
        root_guard.finish(&execution_result);
        let bytes_read = execution_result?;
        schedule.complete_boundary(&task.plan.root, PipelineBoundaryKind::ResultSink)?;
        schedule.verify_complete()?;
        let source_build_us = source_started.elapsed().as_micros() as u64;
        let run_us = run_started.elapsed().as_micros() as u64;

        let collect_started = Instant::now();
        let mut sink = sink
            .lock()
            .map_err(|_| VeinError::internal("sink mutex poisoned"))?;
        let output = sink.collect();
        let row_count = sink
            .row_count()
            .max(output.iter().map(|batch| batch.num_rows() as u64).sum());
        let write_receipt = sink.write_receipt().cloned();
        let collect_us = collect_started.elapsed().as_micros() as u64;
        let duration_ns = start.elapsed().as_nanos() as u64;
        tracing::info!(
            target: "vein.audit",
            event = "task_finished",
            task_id = %task.task_id,
            sink_build_us,
            vm_build_us = 0u64,
            source_build_us,
            run_us,
            collect_us,
            rows = row_count,
            output_batches = output.len(),
            bytes_read,
            memory_limit_bytes = memory.limit(),
            memory_peak_bytes = memory.peak(),
            memory_reserved_bytes = memory.used(),
            spill_disk_limit_bytes = memory.spill_bytes_limit(),
            spill_disk_peak_bytes = memory.spill_bytes_peak(),
            spill_disk_reserved_bytes = memory.spill_bytes_used(),
            total_us = duration_ns / 1000,
            "task_finished"
        );
        trace.mark_completed();
        Ok(TaskResult {
            row_count,
            output,
            write_receipt,
            metrics: TaskMetrics {
                bytes_read,
                duration_ns,
            },
        })
    }
}

fn install_join_residual(
    builder: &dyn ExecutorBuilder,
    plan: &JoinPlan,
    join: HashJoin,
) -> Result<HashJoin> {
    let Some(residual) = &plan.residual else {
        return Ok(join);
    };
    let collector = JoinFilterCollector::default();
    let vm = builder.build_view_vm(&residual.program, Box::new(collector.clone()))?;
    Ok(join.with_residual_filter(join_outputs(&residual.input_columns), vm, collector))
}


#[cfg(test)]
mod tests;
