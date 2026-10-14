//! The executor's execution seam: the traits that decouple pipeline
//! construction and task execution from the wire protocol and the concrete
//! stage implementations.
//!
//! These are the abstractions the rest of the engine programs against:
//! a [`ExecutorBuilder`] selects concrete [`Source`]/[`Transform`]/[`Sink`]
//! implementations from a wire task, and a [`TaskExecutor`] runs a single
//! task end-to-end (see `crate::pipeline`).

use arrow::array::Array;
use arrow::datatypes::DataType;
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
    BuildSink, HashJoin, HashTable, JoinFilterCollector, JoinKeys, JoinType, OutputColumn,
};
use crate::operators::sink::{ClientOutput, CollectSink, ViewResultBatch, WriteReceipt};
use crate::operators::{LimitTransform, SortTransform};
use crate::pipeline::{
    DataChunk, DataChunkView, MaterializationReport, PipelineNode, Sink, Source, Transform,
};
use crate::runtime_filter::{RuntimeFilterBuildInput, RuntimeFilterFactory, RuntimeFilterSet};
use crate::spill::{LocalSpillStorageFactory, SpillStorageFactory};
use crate::trace::{ExecutionTrace, MaterializationTrace};
use common::Result;
use common::cancellation::CancellationToken;
use common::identifiers::TaskId;
use common::protocol::{
    DataSink, DataSource, DerivedPipelineGraph, ExecuteTask, InputMaterialization, JoinKind,
    JoinOutput, JoinPlan, MicroProgram, OutputRepresentation, PhysicalPlanNode,
    PhysicalPlanOperator, PipelineBoundaryKind, PipelineId,
};
use error::VeinError;
use types::VeinDataType;

/// Per-task execution metrics, reported to the master on completion.
#[derive(Debug, Clone, Copy, Default)]
pub struct TaskMetrics {
    /// Bytes read from the source.
    pub bytes_read: u64,
    /// Wall-clock execution time in nanoseconds.
    pub duration_ns: u64,
}

/// The outcome of running a single task to completion.
#[derive(Debug, Clone)]
pub struct TaskResult<O = RecordBatch> {
    /// Number of rows produced by the pipeline.
    pub row_count: u64,
    /// Captured output batches (populated by a [`Sink`] that retains results).
    pub output: Vec<O>,
    /// Execution metrics (bytes read, duration).
    pub metrics: TaskMetrics,
    /// Present only after a storage sink has committed successfully.
    pub write_receipt: Option<WriteReceipt>,
}

impl<O> Default for TaskResult<O> {
    fn default() -> Self {
        Self {
            row_count: 0,
            output: Vec::new(),
            metrics: TaskMetrics::default(),
            write_receipt: None,
        }
    }
}

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
    /// Run one task, driving its source through the operator chain.
    async fn execute(&self, task: &ExecuteTask, cancel: CancellationToken) -> Result<TaskResult>;

    /// Executes the same canonical plan while retaining client result views.
    async fn execute_views(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
    ) -> Result<TaskResult<ViewResultBatch>>;

    /// Streams reference-backed results through a bounded channel.
    async fn execute_views_streaming(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
        output: flume::Sender<ViewResultBatch>,
    ) -> Result<TaskResult<ViewResultBatch>>;

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

/// A [`PipelineNode`] wrapper that aborts the pipeline if cancellation has been
/// requested. Placed just above the sink, it is invoked once per batch that
/// reaches the end of the chain, letting a long-running source stop promptly.
struct CancelGuard<B: 'static> {
    inner: Box<dyn PipelineNode<B>>,
    cancel: CancellationToken,
}

impl<B: 'static> CancelGuard<B> {
    fn new(inner: Box<dyn PipelineNode<B>>, cancel: CancellationToken) -> Self {
        Self { inner, cancel }
    }
}

impl<B: 'static> PipelineNode<B> for CancelGuard<B> {
    fn push(&mut self, batch: B) -> Result<()> {
        if self.cancel.is_cancelled() {
            return Err(VeinError::cancelled());
        }
        self.inner.push(batch)
    }

    fn flush(&mut self) -> Result<()> {
        if self.cancel.is_cancelled() {
            return Err(VeinError::cancelled());
        }
        self.inner.flush()
    }
}

/// A [`PipelineNode`] adapter that forwards pushes into a shared sink, so the
/// runner can retrieve the sink's result via [`Sink::collect`] after execution.
struct SinkHandle<B: 'static, O: 'static = RecordBatch>(Arc<Mutex<Box<dyn Sink<B, O>>>>);

impl<B: 'static, O: 'static> PipelineNode<B> for SinkHandle<B, O> {
    fn push(&mut self, batch: B) -> Result<()> {
        self.0
            .lock()
            .map_err(|_| VeinError::internal("sink mutex poisoned"))?
            .push(batch)
    }

    fn flush(&mut self) -> Result<()> {
        self.0
            .lock()
            .map_err(|_| VeinError::internal("sink mutex poisoned"))?
            .flush()
    }
}

/// Adapts an already-built transform so another pipeline stage can own it as a node.
struct TransformHandle(Box<dyn Transform<DataChunkView>>);

impl PipelineNode<DataChunkView> for TransformHandle {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.0.push(view)
    }

    fn flush(&mut self) -> Result<()> {
        self.0.flush()
    }

    fn finished(&self) -> bool {
        self.0.finished()
    }

    fn fork_local(&self) -> Result<Option<Box<dyn PipelineNode<DataChunkView>>>> {
        let Some(transform) = self.0.fork_transform()? else {
            return Ok(None);
        };
        Ok(Some(Box::new(Self(transform))))
    }

    fn combine_local(
        self: Box<Self>,
        canonical: &mut dyn PipelineNode<DataChunkView>,
    ) -> Result<()> {
        let target = canonical
            .as_any_mut()
            .downcast_mut::<TransformHandle>()
            .ok_or_else(|| VeinError::internal("transform local state has a mismatched target"))?;
        self.0.combine_transform(target.0.as_mut())
    }

}

/// Explicit array boundary for blocking operators and RecordBatch result sinks.
/// Reports conversion work once per flush, inside the owning task's trace span.
pub(crate) struct MaterializeViewNode<
    N: PipelineNode<DataChunk> + ?Sized = dyn PipelineNode<DataChunk>,
> {
    next: Box<N>,
    boundary: &'static str,
    report: MaterializationReport,
    batches: u64,
    rows: u64,
    conversion_us: u64,
    trace: Option<MaterializationTrace>,
}

impl<N: PipelineNode<DataChunk> + ?Sized> MaterializeViewNode<N> {
    /// Only whole-batch consumers may install this adapter. Deriving the policy
    /// prevents runtime wiring from silently bypassing an operator's contract.
    fn for_operator(next: Box<N>, operator: &PhysicalPlanOperator) -> Result<Self> {
        let contract = operator.data_flow();
        if contract.input_materialization != InputMaterialization::WholeBatch {
            return Err(VeinError::planning(
                format!(
                    "operator '{}' does not require whole-batch materialization",
                    contract.operator
                ),
                None,
            ));
        }
        let boundary = if contract.output == OutputRepresentation::Sink {
            "result"
        } else {
            contract.operator
        };
        Ok(Self::new(next, boundary))
    }

    pub(crate) fn new(next: Box<N>, boundary: &'static str) -> Self {
        Self {
            next,
            boundary,
            report: MaterializationReport::default(),
            batches: 0,
            rows: 0,
            conversion_us: 0,
            trace: None,
        }
    }

    fn with_trace(mut self, trace: Option<MaterializationTrace>) -> Self {
        self.trace = trace;
        self
    }
}

impl<N: PipelineNode<DataChunk> + ?Sized> PipelineNode<DataChunkView> for MaterializeViewNode<N> {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        let started = Instant::now();
        let (chunk, report) = view.to_data_chunk_with_report()?;
        let conversion_elapsed = started.elapsed();
        self.conversion_us = self
            .conversion_us
            .saturating_add(u64::try_from(conversion_elapsed.as_micros()).unwrap_or(u64::MAX));
        self.report.accumulate(report);
        self.batches = self.batches.saturating_add(1);
        self.rows = self.rows.saturating_add(view.num_rows() as u64);
        if let Some(trace) = &self.trace {
            trace.record(view.num_rows(), report, conversion_elapsed);
        }
        self.next.push(chunk)
    }

    fn flush(&mut self) -> Result<()> {
        self.next.flush()?;
        tracing::info!(
            target: "vein.audit",
            event = "view_materialization",
            query_id = self.trace.as_ref().map(MaterializationTrace::query_id).unwrap_or(""),
            task_id = self.trace.as_ref().map(MaterializationTrace::task_id).unwrap_or(""),
            node_id = self.trace.as_ref().map(MaterializationTrace::node_id).unwrap_or(u32::MAX),
            boundary = self.boundary,
            batches = self.batches,
            rows = self.rows,
            shared_columns = self.report.shared_columns,
            gathered_columns = self.report.gathered_columns,
            empty_columns = self.report.empty_columns,
            gathered_output_buffer_bytes = self.report.gathered_output_buffer_bytes,
            conversion_us = self.conversion_us,
            "view_materialization"
        );
        Ok(())
    }

    fn finished(&self) -> bool {
        self.next.finished()
    }

    fn fork_local(&self) -> Result<Option<Box<dyn PipelineNode<DataChunkView>>>> {
        let Some(next) = self.next.fork_local()? else {
            return Ok(None);
        };
        let local = MaterializeViewNode::<dyn PipelineNode<DataChunk>>::new(next, self.boundary)
            .with_trace(self.trace.clone());
        Ok(Some(Box::new(local)))
    }

    fn combine_local(
        self: Box<Self>,
        canonical: &mut dyn PipelineNode<DataChunkView>,
    ) -> Result<()> {
        let target = canonical
            .as_any_mut()
            .downcast_mut::<MaterializeViewNode<dyn PipelineNode<DataChunk>>>()
            .ok_or_else(|| {
                VeinError::internal("materialization local state has a mismatched target")
            })?;
        if self.boundary != target.boundary {
            return Err(VeinError::internal(
                "cannot combine materialization nodes with different boundaries",
            ));
        }
        self.next.combine_local(target.next.as_mut())?;
        target.report.accumulate(self.report);
        target.batches = target.batches.saturating_add(self.batches);
        target.rows = target.rows.saturating_add(self.rows);
        target.conversion_us = target.conversion_us.saturating_add(self.conversion_us);
        Ok(())
    }
}

impl<N: Sink<DataChunk> + ?Sized> Sink<DataChunkView> for MaterializeViewNode<N> {
    fn collect(&mut self) -> Vec<RecordBatch> {
        self.next.collect()
    }

    fn row_count(&self) -> u64 {
        self.next.row_count()
    }

    fn write_receipt(&self) -> Option<&WriteReceipt> {
        self.next.write_receipt()
    }
}

/// Shares a build sink with the join executor while a source owns its pipeline.
struct SharedBuildSink(Arc<Mutex<Option<BuildSink>>>);

impl PipelineNode<DataChunkView> for SharedBuildSink {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.0
            .lock()
            .map_err(|_| VeinError::internal("join build mutex poisoned"))?
            .as_mut()
            .ok_or_else(|| VeinError::internal("join build sink already finished"))?
            .push(view)
    }
    fn flush(&mut self) -> Result<()> {
        self.0
            .lock()
            .map_err(|_| VeinError::internal("join build mutex poisoned"))?
            .as_mut()
            .ok_or_else(|| VeinError::internal("join build sink already finished"))?
            .flush()
    }

    fn fork_local(&self) -> Result<Option<Box<dyn PipelineNode<DataChunkView>>>> {
        let build_sink = self
            .0
            .lock()
            .map_err(|_| VeinError::internal("join build mutex poisoned"))?;
        let build_sink = build_sink
            .as_ref()
            .ok_or_else(|| VeinError::internal("join build sink already finished"))?;
        Ok(build_sink
            .fork_worker_local()?
            .map(|local| Box::new(LocalBuildSink(local)) as Box<dyn PipelineNode<DataChunkView>>))
    }
}

/// Worker-private adapter whose completed table is merged into `SharedBuildSink`.
struct LocalBuildSink(BuildSink);

impl PipelineNode<DataChunkView> for LocalBuildSink {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.0.push(view)
    }

    fn flush(&mut self) -> Result<()> {
        self.0.flush()
    }

    fn combine_local(
        self: Box<Self>,
        canonical: &mut dyn PipelineNode<DataChunkView>,
    ) -> Result<()> {
        let canonical = canonical
            .as_any_mut()
            .downcast_mut::<SharedBuildSink>()
            .ok_or_else(|| VeinError::internal("join build local state has a mismatched target"))?;
        let mut canonical_sink = canonical
            .0
            .lock()
            .map_err(|_| VeinError::internal("join build mutex poisoned"))?;
        let canonical_sink = canonical_sink
            .as_mut()
            .ok_or_else(|| VeinError::internal("join build sink already finished"))?;
        canonical_sink.combine_worker(self.0)
    }
}

/// Transfers owned chunks from scan workers to the sole downstream owner.
///
/// The bounded channel moves `DataChunk` handles; Arrow buffers remain shared.
/// `finished` lets source readers stop after a downstream limit has completed.
struct ChannelPipelineNode {
    sender: flume::Sender<DataChunkView>,
    finished: Arc<AtomicBool>,
}

/// Runtime completion state for the graph derived from the canonical plan.
///
/// The recursive interpreter still owns operator semantics. This graph state
/// makes pipeline dependencies executable: join probe input cannot flow until
/// every prerequisite build pipeline has completed, and blocking boundaries
/// are marked only after their input pipelines finish.
struct PipelineExecutionSchedule {
    graph: DerivedPipelineGraph,
    completed: Vec<Arc<AtomicBool>>,
}

impl PipelineExecutionSchedule {
    fn derive(plan: &common::protocol::PhysicalPlan, trace: &ExecutionTrace) -> Result<Self> {
        let graph = DerivedPipelineGraph::derive(plan)?;
        let execution_order = graph.execution_order().ok_or_else(|| {
            VeinError::planning("derived pipeline dependencies contain a cycle", None)
        })?;
        tracing::debug!(
            target: "vein.audit",
            event = "pipeline_schedule_created",
            pipeline_count = execution_order.len(),
            dependency_count = graph.dependencies().len(),
            "pipeline_schedule_created"
        );
        for pipeline in graph.pipelines() {
            let operator_node_ids = pipeline
                .operators
                .iter()
                .map(|node| trace.node_id(node))
                .collect::<Vec<_>>();
            tracing::debug!(
                target: "vein.audit",
                event = "derived_pipeline",
                pipeline_id = pipeline.id.0,
                source_node_id = trace.node_id(&pipeline.source.node),
                source_boundary = ?pipeline.source.kind,
                operator_node_ids = ?operator_node_ids,
                sink_node_id = trace.node_id(&pipeline.sink.node),
                sink_boundary = ?pipeline.sink.kind,
                "derived_pipeline"
            );
        }
        for dependency in graph.dependencies() {
            tracing::debug!(
                target: "vein.audit",
                event = "pipeline_dependency",
                prerequisite_pipeline_id = dependency.prerequisite.0,
                dependent_pipeline_id = dependency.dependent.0,
                dependency_kind = ?dependency.kind,
                "pipeline_dependency"
            );
        }
        Ok(Self {
            completed: (0..graph.pipelines().len())
                .map(|_| Arc::new(AtomicBool::new(false)))
                .collect(),
            graph,
        })
    }

    fn dependencies_for_operator(
        &self,
        node: &Arc<PhysicalPlanNode>,
    ) -> Result<Vec<(PipelineId, Arc<AtomicBool>)>> {
        let pipeline_ids = self
            .graph
            .pipelines()
            .iter()
            .filter(|pipeline| {
                pipeline
                    .operators
                    .iter()
                    .any(|operator| Arc::ptr_eq(operator, node))
            })
            .map(|pipeline| pipeline.id)
            .collect::<Vec<_>>();
        if pipeline_ids.is_empty() {
            return Err(VeinError::planning(
                "join operator is missing from the derived pipeline graph",
                None,
            ));
        }

        let mut dependencies = Vec::new();
        for pipeline_id in pipeline_ids {
            for dependency in self
                .graph
                .dependencies()
                .iter()
                .filter(|dependency| dependency.dependent == pipeline_id)
            {
                let state = self
                    .completed
                    .get(dependency.prerequisite.0)
                    .ok_or_else(|| {
                        VeinError::planning(
                            "pipeline dependency references an unknown pipeline",
                            None,
                        )
                    })?;
                if !dependencies
                    .iter()
                    .any(|(id, _): &(PipelineId, Arc<AtomicBool>)| *id == dependency.prerequisite)
                {
                    dependencies.push((dependency.prerequisite, Arc::clone(state)));
                }
            }
        }
        Ok(dependencies)
    }

    fn complete_boundary(
        &self,
        node: &Arc<PhysicalPlanNode>,
        kind: PipelineBoundaryKind,
    ) -> Result<()> {
        let mut completed_any = false;
        for pipeline in
            self.graph.pipelines().iter().filter(|pipeline| {
                pipeline.sink.kind == kind && Arc::ptr_eq(&pipeline.sink.node, node)
            })
        {
            for dependency in self
                .graph
                .dependencies()
                .iter()
                .filter(|dependency| dependency.dependent == pipeline.id)
            {
                let state = self
                    .completed
                    .get(dependency.prerequisite.0)
                    .ok_or_else(|| {
                        VeinError::planning(
                            "pipeline dependency references an unknown pipeline",
                            None,
                        )
                    })?;
                if !state.load(Ordering::Acquire) {
                    return Err(VeinError::internal(format!(
                        "pipeline {:?} reached boundary {:?} before prerequisite {:?}",
                        pipeline.id, kind, dependency.prerequisite
                    )));
                }
            }
            self.completed[pipeline.id.0].store(true, Ordering::Release);
            tracing::debug!(
                target: "vein.audit",
                event = "pipeline_boundary_completed",
                pipeline_id = pipeline.id.0,
                boundary = ?kind,
                dependency_count = self
                    .graph
                    .dependencies()
                    .iter()
                    .filter(|dependency| dependency.dependent == pipeline.id)
                    .count(),
                "pipeline_boundary_completed"
            );
            completed_any = true;
        }
        if !completed_any {
            return Err(VeinError::planning(
                "completed operator boundary is missing from the derived pipeline graph",
                None,
            ));
        }
        Ok(())
    }

    fn verify_complete(&self) -> Result<()> {
        if let Some((index, _)) = self
            .completed
            .iter()
            .enumerate()
            .find(|(_, completed)| !completed.load(Ordering::Acquire))
        {
            return Err(VeinError::internal(format!(
                "pipeline {:?} did not complete during task execution",
                PipelineId(index)
            )));
        }
        Ok(())
    }
}

/// Guards the first probe push (or empty-input flush) with graph prerequisites.
struct PipelineDependencyGuard {
    inner: Box<dyn PipelineNode<DataChunkView>>,
    dependencies: Vec<(PipelineId, Arc<AtomicBool>)>,
    checked: bool,
}

impl PipelineDependencyGuard {
    fn new(
        inner: Box<dyn PipelineNode<DataChunkView>>,
        dependencies: Vec<(PipelineId, Arc<AtomicBool>)>,
    ) -> Self {
        Self {
            inner,
            dependencies,
            checked: false,
        }
    }

    fn ensure_ready(&mut self) -> Result<()> {
        if self.checked {
            return Ok(());
        }
        if let Some((pipeline, _)) = self
            .dependencies
            .iter()
            .find(|(_, completed)| !completed.load(Ordering::Acquire))
        {
            return Err(VeinError::internal(format!(
                "join probe started before prerequisite pipeline {:?} completed",
                pipeline
            )));
        }
        self.checked = true;
        Ok(())
    }
}

impl PipelineNode<DataChunkView> for PipelineDependencyGuard {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.ensure_ready()?;
        self.inner.push(view)
    }

    fn flush(&mut self) -> Result<()> {
        self.ensure_ready()?;
        self.inner.flush()
    }

    fn finished(&self) -> bool {
        self.inner.finished()
    }

    fn fork_local(&self) -> Result<Option<Box<dyn PipelineNode<DataChunkView>>>> {
        let Some(inner) = self.inner.fork_local()? else {
            return Ok(None);
        };
        Ok(Some(Box::new(Self::new(
            inner,
            self.dependencies.clone(),
        ))))
    }

    fn combine_local(
        self: Box<Self>,
        canonical: &mut dyn PipelineNode<DataChunkView>,
    ) -> Result<()> {
        let target = canonical
            .as_any_mut()
            .downcast_mut::<PipelineDependencyGuard>()
            .ok_or_else(|| VeinError::internal("join dependency state has a mismatched target"))?;
        if self.dependencies.len() != target.dependencies.len()
            || !self
                .dependencies
                .iter()
                .zip(&target.dependencies)
                .all(|(local, canonical)| local.0 == canonical.0)
        {
            return Err(VeinError::internal(
                "cannot combine join probes with different dependencies",
            ));
        }
        self.inner.combine_local(target.inner.as_mut())?;
        target.checked |= self.checked;
        Ok(())
    }
}

impl PipelineNode<DataChunkView> for ChannelPipelineNode {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.sender.send(view).map_err(|_| VeinError::cancelled())
    }
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
    fn finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
    }
}

/// Counts active rows and chunks entering a join without inspecting or cloning columns.
struct JoinProbeCounter {
    inner: Box<dyn PipelineNode<DataChunkView>>,
    rows: Arc<AtomicU64>,
    batches: Arc<AtomicU64>,
}

impl PipelineNode<DataChunkView> for JoinProbeCounter {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.rows
            .fetch_add(view.num_rows() as u64, Ordering::Relaxed);
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.inner.push(view)
    }
    fn flush(&mut self) -> Result<()> {
        self.inner.flush()
    }
    fn finished(&self) -> bool {
        self.inner.finished()
    }

    fn fork_local(&self) -> Result<Option<Box<dyn PipelineNode<DataChunkView>>>> {
        let Some(inner) = self.inner.fork_local()? else {
            return Ok(None);
        };
        Ok(Some(Box::new(Self {
            inner,
            rows: Arc::clone(&self.rows),
            batches: Arc::clone(&self.batches),
        })))
    }

    fn combine_local(
        self: Box<Self>,
        canonical: &mut dyn PipelineNode<DataChunkView>,
    ) -> Result<()> {
        let target = canonical
            .as_any_mut()
            .downcast_mut::<JoinProbeCounter>()
            .ok_or_else(|| VeinError::internal("join probe counter has a mismatched target"))?;
        self.inner.combine_local(target.inner.as_mut())
    }
}

type SharedJoinOutput = Arc<Mutex<Box<dyn PipelineNode<DataChunkView>>>>;

/// Forwards partition results into the one downstream operator instance.
/// HashJoin flushes once per partition; this adapter defers that flush until
/// every partition has completed.
#[derive(Clone)]
struct JoinPartitionOutput(SharedJoinOutput);

impl PipelineNode<DataChunkView> for JoinPartitionOutput {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.0
            .lock()
            .map_err(|_| VeinError::internal("join partition output mutex poisoned"))?
            .push(view)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn finished(&self) -> bool {
        self.0.lock().map(|next| next.finished()).unwrap_or(false)
    }
}

/// Probe-side pipeline used after the build sink spills. It materializes the
/// probe side once into an IPC run, then recursively executes bounded hash
/// partitions through the same HashJoin operator used by the in-memory path.
struct SpilledJoinProbe {
    probe_writer: Option<JoinSpillWriter>,
    build_run: JoinSpillRun,
    build_schema: JoinSpillSchema,
    probe_schema: JoinSpillSchema,
    plan: JoinPlan,
    builder: Arc<dyn ExecutorBuilder>,
    tracker: QueryMemoryTracker,
    next: SharedJoinOutput,
}

impl SpilledJoinProbe {
    fn process_partitions(
        &self,
        build_runs: Vec<JoinSpillRun>,
        probe_runs: Vec<JoinSpillRun>,
        bit: u32,
    ) -> Result<()> {
        self.tracker.check_cancelled()?;
        let mut table = HashTable::with_memory_tracker(
            join_keys(&self.plan.build_keys),
            join_keys(&self.plan.probe_keys),
            self.tracker.clone(),
        )?;
        let mut overflow = false;
        'build_runs: for run in &build_runs {
            let mut reader = open_reader(run)?;
            while let Some(batch) = reader.next() {
                self.tracker.check_cancelled()?;
                let batch = batch.map_err(|error| {
                    VeinError::internal(format!("decode spilled join build batch: {error}"))
                })?;
                // Leasing decoded payload arrays is part of loading a partition
                // into the hash table. It can exhaust the budget before
                // insert_view gets a chance to reject the row references, so
                // treat either failure as a signal to repartition.
                let view = match payload_view(batch, &self.build_schema, &self.tracker) {
                    Ok(view) => view,
                    Err(VeinError::ResourceExhausted { .. }) => {
                        overflow = true;
                        break 'build_runs;
                    }
                    Err(error) => return Err(error),
                };
                match table.insert_view(view) {
                    Ok(()) => {}
                    Err(VeinError::ResourceExhausted { .. }) => {
                        overflow = true;
                        break 'build_runs;
                    }
                    Err(error) => return Err(error),
                }
            }
        }

        // Load the complete build partition first, but do not emit any output
        // until the largest probe batch can coexist with it. If it cannot,
        // repartition both sides before the downstream operator sees rows.
        if !overflow {
            let working_set = self.tracker.used();
            'probe_runs: for run in &probe_runs {
                let mut reader = open_reader(run)?;
                while let Some(batch) = reader.next() {
                    self.tracker.check_cancelled()?;
                    let batch = batch.map_err(|error| {
                        VeinError::internal(format!("decode spilled join probe batch: {error}"))
                    })?;
                    let batch_bytes = batch
                        .columns()
                        .iter()
                        .map(|array| array.get_array_memory_size())
                        .fold(0_usize, usize::saturating_add);
                    if working_set
                        .checked_add(batch_bytes)
                        .is_none_or(|required| required > self.tracker.limit())
                    {
                        overflow = true;
                        break 'probe_runs;
                    }
                }
            }
        }

        if overflow {
            drop(table);
            if bit >= u64::BITS {
                return Err(VeinError::resource_exhausted(
                    "recursive hash join partition",
                    u128::from(bit) + 1,
                    u128::from(u64::BITS),
                ));
            }
            tracing::debug!(
                target: "vein.audit",
                event = "join_spill_repartition",
                partition_bit = bit,
                build_runs = build_runs.len(),
                probe_runs = probe_runs.len(),
                "join spill partition still exceeds memory budget"
            );
            let build_children = if build_runs.is_empty() {
                [None, None]
            } else {
                partition_runs(&build_runs, &self.build_schema, &self.tracker, bit)?
            };
            let probe_children = if probe_runs.is_empty() {
                [None, None]
            } else {
                partition_runs(&probe_runs, &self.probe_schema, &self.tracker, bit)?
            };
            for run in build_runs.into_iter().chain(probe_runs) {
                remove_run(run)?;
            }
            for partition in 0..2 {
                let build: Vec<JoinSpillRun> =
                    build_children[partition].clone().into_iter().collect();
                let probe: Vec<JoinSpillRun> =
                    probe_children[partition].clone().into_iter().collect();
                if !build.is_empty() || !probe.is_empty() {
                    self.process_partitions(build, probe, bit + 1)?;
                }
            }
            return Ok(());
        }

        let build_rows = table.len() as u64;
        tracing::debug!(
            target: "vein.audit",
            event = "join_spill_partition_loaded",
            partition_bit = bit,
            build_rows,
            "join spill partition loaded"
        );
        let join_type = join_type(self.plan.join_type)?;
        let next = Box::new(JoinPartitionOutput(Arc::clone(&self.next)));
        let join = HashJoin::new(
            Arc::new(table),
            join_type,
            join_outputs(&self.plan.outputs),
            join_types(&self.plan.probe_types),
            join_types(&self.plan.build_types),
            next,
        );
        let join = install_join_residual(self.builder.as_ref(), &self.plan, join)?;
        let mut join: Box<dyn PipelineNode<DataChunkView>> = Box::new(join);
        for run in &probe_runs {
            let mut reader = open_reader(run)?;
            while let Some(batch) = reader.next() {
                self.tracker.check_cancelled()?;
                let batch = batch.map_err(|error| {
                    VeinError::internal(format!("decode spilled join probe batch: {error}"))
                })?;
                join.push(payload_view(batch, &self.probe_schema, &self.tracker)?)?;
            }
        }
        join.flush()?;
        for run in build_runs.into_iter().chain(probe_runs) {
            remove_run(run)?;
        }
        Ok(())
    }
}

impl PipelineNode<DataChunkView> for SpilledJoinProbe {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.probe_writer
            .as_mut()
            .ok_or_else(|| VeinError::internal("spilled join probe is already finished"))?
            .push(&view, &join_keys(&self.plan.probe_keys))
    }

    fn flush(&mut self) -> Result<()> {
        let probe_run = self
            .probe_writer
            .take()
            .ok_or_else(|| VeinError::internal("spilled join probe has no writer"))?
            .finish()?;
        tracing::info!(
            target: "vein.audit",
            event = "join_spill_probe_finished",
            build_rows = self.build_run.rows,
            build_bytes = self.build_run.bytes,
            probe_rows = probe_run.rows,
            probe_bytes = probe_run.bytes,
            "join spill inputs materialized"
        );
        self.process_partitions(vec![self.build_run.clone()], vec![probe_run], 0)?;
        self.next
            .lock()
            .map_err(|_| VeinError::internal("join spill output mutex poisoned"))?
            .flush()
    }

    fn finished(&self) -> bool {
        self.next
            .lock()
            .map(|next| next.finished())
            .unwrap_or(false)
    }
}

/// A [`PipelineNode`] adapter that forwards pushes and swallows the flush.
///
/// A join's probe side is split into one chain per file, all feeding the one
/// shared sink, so each chain's flush is swallowed and the task flushes the sink
/// exactly once — rather than once per file, which would commit a write sink
/// repeatedly.
struct SuppressFlush<B: 'static> {
    inner: Box<dyn PipelineNode<B>>,
}

impl<B: 'static> SuppressFlush<B> {
    fn new(inner: Box<dyn PipelineNode<B>>) -> Self {
        Self { inner }
    }
}

impl<B: 'static> PipelineNode<B> for SuppressFlush<B> {
    fn push(&mut self, batch: B) -> Result<()> {
        self.inner.push(batch)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn combine_local(
        self: Box<Self>,
        canonical: &mut dyn PipelineNode<B>,
    ) -> Result<()> {
        self.inner.combine_local(canonical)
    }
}

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

        let build_key_bounds = table.runtime_key_bounds()?;
        let bounded_key_columns = build_key_bounds.iter().filter(|bounds| bounds.is_some()).count();
        let filters = factory.build_filters(RuntimeFilterBuildInput {
            probe_columns: &columns,
            build_hashes: table.runtime_filter_hashes(),
            build_key_bounds,
            memory,
        })?;
        if filters.is_empty() {
            tracing::debug!(
                target: "vein.audit",
                event = "join_runtime_filter_skipped",
                node_id,
                reason = "factory_declined",
                build_hashes = table.runtime_filter_hashes().len(),
                "join runtime filter skipped"
            );
            return Ok(None);
        }

        let filters = Arc::new(RuntimeFilterSet::new(filters));
        tracing::info!(
            target: "vein.audit",
            event = "join_runtime_filter_built",
            node_id,
            filter_names = ?filters.names(),
            key_columns = columns.len(),
            build_rows = table.len(),
            indexed_build_keys = table.runtime_filter_hashes().len(),
            bounded_key_columns,
            filter_bytes = filters.memory_bytes(),
            "join runtime filter built"
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
        }
    }

    /// Use a factory that creates one isolated spill backend per query.
    pub fn with_spill_storage_factory(mut self, factory: Arc<dyn SpillStorageFactory>) -> Self {
        self.spill_storage_factory = factory;
        self
    }

    /// Execute a recursive input tree with bounded batches between operators.
    fn execute_input<'a>(
        &'a self,
        input: &'a Arc<PhysicalPlanNode>,
        next: Box<dyn PipelineNode<DataChunkView>>,
        task: &'a ExecuteTask,
        cancel: &'a CancellationToken,
        memory: &'a QueryMemoryTracker,
        span: &'a tracing::Span,
        schedule: &'a PipelineExecutionSchedule,
        trace: &'a ExecutionTrace,
        depth: usize,
    ) -> Pin<Box<dyn Future<Output = Result<u64>> + Send + 'a>> {
        self.execute_input_with_runtime_filters(
            input, next, task, cancel, memory, span, schedule, trace, None, depth,
        )
    }

    /// Execute one input node with filters scoped to its direct source.
    /// Recursive operators use `execute_input` unless they can prove a safe
    /// column-preserving path, which keeps unsupported plans on exact execution.
    fn execute_input_with_runtime_filters<'a>(
        &'a self,
        input: &'a Arc<PhysicalPlanNode>,
        next: Box<dyn PipelineNode<DataChunkView>>,
        task: &'a ExecuteTask,
        cancel: &'a CancellationToken,
        memory: &'a QueryMemoryTracker,
        span: &'a tracing::Span,
        schedule: &'a PipelineExecutionSchedule,
        trace: &'a ExecutionTrace,
        runtime_filters: Option<Arc<RuntimeFilterSet>>,
        depth: usize,
    ) -> Pin<Box<dyn Future<Output = Result<u64>> + Send + 'a>> {
        let node_id = trace.node_id(input);
        let operator = input.operator.data_flow().operator;
        let node_span = tracing::debug_span!(
            target: "vein.audit",
            "physical_operator",
            query_id = %task.query_id,
            task_id = %task.task_id,
            node_id,
            operator,
            depth,
        );
        let trace_for_metrics = trace;
        Box::pin(async move {
            let mut execution_guard = trace_for_metrics.execution_guard(node_id);
            let result = async move {
                if cancel.is_cancelled() {
                    return Err(VeinError::cancelled());
                }
                tracing::debug!(
                target: "vein.audit",
                event = "physical_operator_started",
                node_id,
                operator,
                depth,
                "physical_operator_started"
            );
                // Execution depth starts at the sink's child; diagnostics include
                // the sink itself as canonical tree depth zero.
                Self::trace_data_flow(input, task, trace, depth.saturating_add(1));
                let mut next = trace.observer(
                    trace.node_id(input),
                    next,
                    task.query_id.to_string(),
                    task.task_id.to_string(),
                );
                match &input.operator {
                    PhysicalPlanOperator::Pipeline {
                        source: data_source,
                        program,
                        ..
                    } => {
                        let max_parallelism = self.builder.max_parallelism().max(1);
                        let sources = self
                            .builder
                            .partition_scan_source_with_runtime_filters(
                                data_source,
                                max_parallelism,
                                runtime_filters.as_deref(),
                            )?;
                        if sources.is_empty() {
                            tracing::debug!(
                                target: "vein.audit",
                                event = "scan_work_scheduled",
                                work_unit_count = 0,
                                worker_count = 0,
                                "scan has no candidate storage units"
                            );
                            let mut empty_pipeline = if program.instructions.is_empty() {
                                next
                            } else {
                                self.builder.build_view_vm(program, next)?
                            };
                            empty_pipeline.flush()?;
                            trace.record_source_bytes(node_id, 0);
                            return Ok(0);
                        }
                        if sources.len() > 1 {
                            let worker_count = max_parallelism.min(sources.len());
                            tracing::debug!(
                                target: "vein.audit",
                                event = "scan_work_scheduled",
                                work_unit_count = sources.len(),
                                worker_count,
                                "scan storage units scheduled"
                            );
                            let mut local_nodes = Vec::new();
                            local_nodes.try_reserve(worker_count).map_err(|error| {
                                VeinError::internal(format!(
                                    "cannot reserve scan-local pipeline states: {error}"
                                ))
                            })?;
                            let mut fan_in_fallback = if worker_count == 1 {
                                Some("parallelism_one")
                            } else {
                                None
                            };
                            if fan_in_fallback.is_none() {
                                for _ in 0..worker_count {
                                    match next.fork_local() {
                                        Ok(Some(local)) => local_nodes.push(local),
                                        Ok(None) => {
                                            fan_in_fallback = Some("operator_chain_not_combinable");
                                            break;
                                        }
                                        Err(VeinError::ResourceExhausted { .. }) => {
                                            fan_in_fallback = Some("worker_state_memory_budget");
                                            break;
                                        }
                                        Err(error) => return Err(error),
                                    }
                                }
                            }
                            if local_nodes.len() == worker_count && fan_in_fallback.is_none() {
                                let mut canonical: Box<dyn PipelineNode<DataChunkView>> =
                                    if program.instructions.is_empty() {
                                        next
                                    } else {
                                        Box::new(TransformHandle(
                                            self.builder.build_view_vm(program, next)?,
                                        ))
                                    };
                                tracing::debug!(
                                    target: "vein.audit",
                                    event = "scan_fan_in_selected",
                                    mode = "worker_local_combine",
                                    work_unit_count = sources.len(),
                                    worker_count,
                                    "scan workers own independent downstream operator state"
                                );
                                let pending = Arc::new(Mutex::new(VecDeque::from(sources)));
                                let runtime = tokio::runtime::Handle::current();
                                let mut workers = tokio::task::JoinSet::new();
                                for local in local_nodes {
                                    let builder = Arc::clone(&self.builder);
                                    let pending = Arc::clone(&pending);
                                    let program = Arc::clone(program);
                                    let task_id = task.task_id;
                                    let attempt = task.attempt;
                                    let cancel = cancel.clone();
                                    let span = span.clone();
                                    let runtime = runtime.clone();
                                    let runtime_filters = runtime_filters.clone();
                                    workers.spawn_blocking(move || {
                                        let mut bytes = 0u64;
                                        let worker_tail: Box<dyn PipelineNode<DataChunkView>> =
                                            Box::new(SuppressFlush::new(local));
                                        let mut pipeline: Box<dyn PipelineNode<DataChunkView>> =
                                            if program.instructions.is_empty() {
                                                worker_tail
                                            } else {
                                                Box::new(TransformHandle(
                                                    builder.build_view_vm(&program, worker_tail)?,
                                                ))
                                            };
                                        loop {
                                            if cancel.is_cancelled() {
                                                return Err(VeinError::cancelled());
                                            }
                                            let source_spec = pending
                                                .lock()
                                                .map_err(|_| {
                                                    VeinError::internal(
                                                        "scan work queue mutex poisoned",
                                                    )
                                                })?
                                                .pop_front();
                                            let Some(source_spec) = source_spec else {
                                                break;
                                            };
                                            let mut source = match runtime_filters.as_deref() {
                                                Some(filters) if !filters.is_empty() => builder
                                                    .build_source_with_runtime_filters(
                                                        &source_spec,
                                                        task_id,
                                                        attempt,
                                                        filters,
                                                    )?,
                                                _ => builder.build_source(
                                                    &source_spec,
                                                    task_id,
                                                    attempt,
                                                )?,
                                            };
                                            runtime.block_on(async {
                                                tokio::select! {
                                                    biased;
                                                    _ = cancel.cancelled() => Err(VeinError::cancelled()),
                                                    result = source.execute(pipeline.as_mut()).instrument(span.clone()) => result,
                                                }
                                            })?;
                                            bytes = bytes.saturating_add(source.bytes_read());
                                        }
                                        Ok((bytes, pipeline))
                                    });
                                }

                                let mut active_workers = worker_count;
                                let mut bytes = 0u64;
                                let mut local_states = Vec::new();
                                local_states.try_reserve(worker_count).map_err(|error| {
                                    VeinError::internal(format!(
                                        "cannot reserve completed scan-local states: {error}"
                                    ))
                                })?;
                                let mut worker_error = None;
                                while active_workers > 0 {
                                    tokio::select! {
                                        biased;
                                        _ = cancel.cancelled() => {
                                            worker_error.get_or_insert_with(VeinError::cancelled);
                                            break;
                                        }
                                        worker = workers.join_next() => {
                                            match worker {
                                                Some(Ok(Ok((worker_bytes, local)))) => {
                                                    active_workers -= 1;
                                                    bytes = bytes.saturating_add(worker_bytes);
                                                    local_states.push(local);
                                                }
                                                Some(Ok(Err(error))) => {
                                                    worker_error = Some(error);
                                                    cancel.cancel();
                                                    break;
                                                }
                                                Some(Err(error)) => {
                                                    worker_error = Some(VeinError::internal(format!(
                                                        "local scan worker failed: {error}"
                                                    )));
                                                    cancel.cancel();
                                                    break;
                                                }
                                                None => active_workers = 0,
                                            }
                                        }
                                    }
                                }
                                if let Some(error) = worker_error {
                                    cancel.cancel();
                                    while workers.join_next().await.is_some() {}
                                    return Err(error);
                                }
                                for local in local_states {
                                    local.combine_local(canonical.as_mut())?;
                                }
                                tracing::debug!(
                                    target: "vein.audit",
                                    event = "scan_fan_in_combined",
                                    worker_count,
                                    "worker-local pipeline state combined"
                                );
                                canonical.flush()?;
                                trace.record_source_bytes(node_id, bytes);
                                return Ok(bytes);
                            }
                            tracing::debug!(
                                target: "vein.audit",
                                event = "scan_fan_in_selected",
                                mode = "bounded_channel_serial",
                                work_unit_count = sources.len(),
                                worker_count,
                                fallback_reason = fan_in_fallback.unwrap_or("local_state_unavailable"),
                                "scan workers use the serial downstream fallback"
                            );
                            let pending = Arc::new(Mutex::new(VecDeque::from(sources)));
                            let finished = Arc::new(AtomicBool::new(false));
                            let (sender, receiver) = flume::bounded(worker_count);
                            let runtime = tokio::runtime::Handle::current();
                            let mut workers = tokio::task::JoinSet::new();
                            for _ in 0..worker_count {
                                let builder = Arc::clone(&self.builder);
                                let pending = Arc::clone(&pending);
                                let sender = sender.clone();
                                let finished = Arc::clone(&finished);
                                let program = Arc::clone(program);
                                let task_id = task.task_id;
                                let attempt = task.attempt;
                                let cancel = cancel.clone();
                                let span = span.clone();
                                let runtime = runtime.clone();
                                let runtime_filters = runtime_filters.clone();
                                workers.spawn_blocking(move || {
                                    let mut bytes = 0u64;
                                    loop {
                                        if cancel.is_cancelled() {
                                            return Err(VeinError::cancelled());
                                        }
                                        let source_spec = pending
                                            .lock()
                                            .map_err(|_| {
                                                VeinError::internal("scan work queue mutex poisoned")
                                            })?
                                            .pop_front();
                                        let Some(source_spec) = source_spec else {
                                            return Ok(bytes);
                                        };
                                        let mut source = match runtime_filters.as_deref() {
                                            Some(filters) if !filters.is_empty() => builder
                                                .build_source_with_runtime_filters(
                                                    &source_spec,
                                                    task_id,
                                                    attempt,
                                                    filters,
                                                )?,
                                            _ => builder.build_source(&source_spec, task_id, attempt)?,
                                        };
                                        let channel: Box<dyn PipelineNode<DataChunkView>> = Box::new(
                                            SuppressFlush::new(Box::new(ChannelPipelineNode {
                                                sender: sender.clone(),
                                                finished: Arc::clone(&finished),
                                            })),
                                        );
                                        let result = if program.instructions.is_empty() {
                                            let mut next = channel;
                                            runtime.block_on(async {
                                                tokio::select! {
                                                biased;
                                                _ = cancel.cancelled() => Err(VeinError::cancelled()),
                                                result = source.execute(next.as_mut()).instrument(span.clone()) => result,
                                            }
                                            })
                                        } else {
                                            let vm = builder.build_view_vm(&program, channel)?;
                                            let mut next = vm;
                                            runtime.block_on(async {
                                                tokio::select! {
                                                biased;
                                                _ = cancel.cancelled() => Err(VeinError::cancelled()),
                                                result = source.execute(next.as_mut()).instrument(span.clone()) => result,
                                            }
                                            })
                                        };
                                        result?;
                                        bytes = bytes.saturating_add(source.bytes_read());
                                        if finished.load(Ordering::Acquire) {
                                            return Ok(bytes);
                                        }
                                    }
                                });
                            }
                            drop(sender);

                            let mut active_workers = worker_count;
                            let mut receiver_open = true;
                            let mut bytes = 0u64;
                            let mut worker_error = None;
                            while active_workers > 0 || receiver_open {
                                tokio::select! {
                                biased;
                                _ = cancel.cancelled() => {
                                    worker_error.get_or_insert_with(VeinError::cancelled);
                                    break;
                                }
                                        message = receiver.recv_async(), if receiver_open => {
                                    match message {
                                        Ok(chunk) => {
                                            if let Err(error) = next.push(chunk) {
                                                worker_error = Some(error);
                                                cancel.cancel();
                                                break;
                                            }
                                            if next.finished() {
                                                finished.store(true, Ordering::Release);
                                            }
                                        }
                                        Err(_) => receiver_open = false,
                                    }
                                }
                                worker = workers.join_next(), if active_workers > 0 => {
                                    match worker {
                                        Some(Ok(Ok(worker_bytes))) => {
                                            active_workers -= 1;
                                            bytes = bytes.saturating_add(worker_bytes);
                                        }
                                        Some(Ok(Err(error))) => {
                                            worker_error = Some(error);
                                            cancel.cancel();
                                            break;
                                        }
                                        Some(Err(error)) => {
                                            worker_error = Some(VeinError::internal(format!(
                                                "input scan worker failed: {error}"
                                            )));
                                            cancel.cancel();
                                            break;
                                        }
                                        None => active_workers = 0,
                                    }
                                }
                            }
                            }
                            drop(receiver);
                            if let Some(error) = worker_error {
                                cancel.cancel();
                                while workers.join_next().await.is_some() {}
                                return Err(error);
                            }
                            next.flush()?;
                            trace.record_source_bytes(node_id, bytes);
                            return Ok(bytes);
                        }
                        let source_spec = &sources[0];
                        let mut source = match runtime_filters.as_deref() {
                            Some(filters) if !filters.is_empty() => self
                                .builder
                                .build_source_with_runtime_filters(
                                    source_spec,
                                    task.task_id,
                                    task.attempt,
                                    filters,
                                )?,
                            _ => self
                                .builder
                                .build_source(source_spec, task.task_id, task.attempt)?,
                        };
                        let bytes_read = if program.instructions.is_empty() {
                            let mut source_next = next;
                            tokio::select! {
                            biased;
                            _ = cancel.cancelled() => Err(VeinError::cancelled()),
                            result = source.execute(source_next.as_mut()).instrument(span.clone()) => {
                                result?;
                                Ok(source.bytes_read())
                            }
                        }
                        } else {
                            let vm = self.builder.build_view_vm(program, next)?;
                            let mut source_next = vm;
                            tokio::select! {
                            biased;
                            _ = cancel.cancelled() => Err(VeinError::cancelled()),
                            result = source.execute(source_next.as_mut()).instrument(span.clone()) => {
                                result?;
                                Ok(source.bytes_read())
                            }
                        }
                        };
                        if let Ok(bytes) = &bytes_read {
                            trace.record_source_bytes(node_id, *bytes);
                        }
                        bytes_read
                    }
                    PhysicalPlanOperator::Transform { input, program } => {
                        let transform = self.builder.build_view_vm(program, next)?;
                        let bytes = self
                            .execute_input(
                                input,
                                Box::new(TransformHandle(transform)),
                                task,
                                cancel,
                                memory,
                                span,
                                schedule,
                                trace,
                                depth + 1,
                            )
                            .await?;
                        Ok(bytes)
                    }
                    PhysicalPlanOperator::Aggregate { input: child, plan }
                    | PhysicalPlanOperator::Distinct { input: child, plan } => {
                        let operator_memory = trace.operator_memory(node_id, memory);
                        let operator =
                            if matches!(&input.operator, PhysicalPlanOperator::Distinct { .. }) {
                                "distinct"
                            } else {
                                "aggregate"
                            };
                        let (accumulator, handle) =
                            aggregate::shared_with_memory_tracker(plan.clone(), operator_memory)?;
                        let handle: Box<dyn PipelineNode<DataChunkView>> =
                            Box::new(MaterializeViewNode::for_operator(handle, &input.operator)?
                                .with_trace(trace.materialization_handle(node_id)));
                        let bytes = self
                            .execute_input(
                                child,
                                handle,
                                task,
                                cancel,
                                memory,
                                span,
                                schedule,
                                trace,
                                depth + 1,
                            )
                            .await?;
                        let boundary =
                            if matches!(&input.operator, PhysicalPlanOperator::Distinct { .. }) {
                                PipelineBoundaryKind::DistinctInput
                            } else {
                                PipelineBoundaryKind::AggregateInput
                            };
                        schedule.complete_boundary(input, boundary)?;
                        let (metrics, group_count) = {
                            let mut accumulator = accumulator
                                .lock()
                                .map_err(|_| VeinError::internal("aggregate mutex poisoned"))?;
                            accumulator.finish_with(aggregate::AGGREGATE_BATCH_ROWS, |batch| {
                                next.push(DataChunkView::from_chunk(batch)?)
                            })?;
                            (accumulator.metrics(), accumulator.group_count())
                        };
                        next.flush()?;
                        tracing::info!(
                        target: "vein.audit",
                        event = "recursive_aggregate_finished",
                        operator,
                        depth,
                        input_bytes = bytes,
                        input_batches = metrics.input_batches,
                        input_rows = metrics.input_rows,
                        grouping_sets = metrics.grouping_sets,
                        grouping_probes = metrics.grouping_probes,
                        group_index_candidate_checks = metrics.group_index_candidate_checks,
                        group_index_reserve_ns = metrics.group_index_reserve_ns,
                        group_index_capacity_growth_events = metrics.group_index_capacity_growth_events,
                        group_index_entries_at_capacity_growth = metrics.group_index_entries_at_capacity_growth,
                        group_index_capacity = metrics.group_index_capacity,
                        group_index_entries_ns = metrics.group_index_entries_ns,
                        full_hash_matches = metrics.full_hash_matches,
                        group_key_comparisons = metrics.group_key_comparisons,
                        group_collision_retries = metrics.group_collision_retries,
                        group_index_hits = metrics.group_index_hits,
                        group_index_misses = metrics.group_index_misses,
                        new_groups = metrics.new_groups,
                        groups = group_count,
                        retained_key_slots_initialized = metrics.retained_key_slots_initialized,
                        string_key_bytes_copied = metrics.string_key_bytes_copied,
                        hasher_setup_ns = metrics.hasher_setup_ns,
                        key_hash_ns = metrics.key_hash_ns,
                        hash_finalize_ns = metrics.hash_finalize_ns,
                        hash_generation_ns = metrics.hash_generation_ns,
                        group_resolution_ns = metrics.group_resolution_ns,
                        group_materialize_ns = metrics.group_materialize_ns,
                        group_ids_distribution_ns = metrics.group_ids_distribution_ns,
                        state_update_ns = metrics.state_update_ns,
                        output_materialize_ns = metrics.output_materialize_ns,
                        "recursive_aggregate_finished"
                    );
                        Ok(bytes)
                    }
                    PhysicalPlanOperator::Sort {
                        input: child,
                        plan,
                        key_program,
                        output_columns,
                    } => {
                        let operator_memory = trace.operator_memory(node_id, memory);
                        let output_columns = usize::try_from(*output_columns).map_err(|_| {
                            VeinError::planning("recursive sort output width is invalid", None)
                        })?;
                        let sort: Box<dyn PipelineNode<DataChunkView>> =
                            Box::new(SortTransform::with_memory_tracker(
                                plan.clone(),
                                Some(output_columns),
                                operator_memory,
                                next,
                            )?);
                        let next: Box<dyn PipelineNode<DataChunkView>> = match key_program {
                            Some(program) => {
                                Box::new(TransformHandle(self.builder.build_view_vm(program, sort)?))
                            }
                            None => sort,
                        };
                        let bytes = self
                            .execute_input(child, next, task, cancel, memory, span, schedule, trace, depth + 1)
                            .await?;
                        schedule.complete_boundary(input, PipelineBoundaryKind::SortInput)?;
                        Ok(bytes)
                    }
                    PhysicalPlanOperator::Limit { input, spec } => {
                        let limit = LimitTransform::new(spec.offset, spec.count, next);
                        self.execute_input(
                            input,
                            Box::new(limit),
                            task,
                            cancel,
                            memory,
                            span,
                            schedule,
                            trace,
                            depth + 1,
                        )
                            .await
                    }
                    PhysicalPlanOperator::Sink { .. } => Err(VeinError::planning(
                        "sink operator may only appear at the physical plan root",
                        None,
                    )),
                    PhysicalPlanOperator::Join { probe, build, plan } => {
                        let started = Instant::now();
                        let operator_memory = trace.operator_memory(node_id, memory);
                        let build_sink = Arc::new(Mutex::new(Some(BuildSink::with_spill_support(
                            join_keys(&plan.probe_keys),
                            join_keys(&plan.build_keys),
                            &join_types(&plan.build_types),
                            operator_memory.clone(),
                        )?)));
                        let build_bytes = self
                            .execute_input(
                                build,
                                Box::new(SharedBuildSink(Arc::clone(&build_sink))),
                                task,
                                cancel,
                                memory,
                                span,
                                schedule,
                                trace,
                                depth + 1,
                            )
                            .await?;
                        schedule.complete_boundary(input, PipelineBoundaryKind::JoinBuild)?;
                        let build_sink = build_sink
                            .lock()
                            .map_err(|_| VeinError::internal("join build mutex poisoned"))?
                            .take()
                            .ok_or_else(|| {
                                VeinError::internal("join build sink missing after execution")
                            })?;
                        let build_result = build_sink.finish_with_spill()?;
                        let build_rows = match &build_result {
                            BuildResult::InMemory(table) => table.len() as u64,
                            BuildResult::Spilled { run, .. } => run.rows,
                        };
                        let build_us = started.elapsed().as_micros() as u64;
                        let build_bytes = match &build_result {
                            BuildResult::InMemory(_) => build_bytes,
                            BuildResult::Spilled { run, .. } => {
                                tracing::info!(
                                target: "vein.audit",
                                event = "join_build_spill_run_ready",
                                rows = run.rows,
                                bytes = run.bytes,
                                "join build spill run ready"
                            );
                                run.bytes
                            }
                        };
                        let runtime_filters = self.runtime_filters_for_join(
                            probe,
                            plan,
                            &build_result,
                            &operator_memory,
                            node_id,
                        )?;
                        let probe_rows = Arc::new(AtomicU64::new(0));
                        let probe_batches = Arc::new(AtomicU64::new(0));
                        let probe_started = Instant::now();
                        let dependencies = schedule.dependencies_for_operator(input)?;
                        let join: Box<dyn PipelineNode<DataChunkView>> = match build_result {
                            BuildResult::InMemory(table) => {
                                let join = HashJoin::new(
                                    Arc::new(table),
                                    join_type(plan.join_type)?,
                                    join_outputs(&plan.outputs),
                                    join_types(&plan.probe_types),
                                    join_types(&plan.build_types),
                                    next,
                                );
                                let join = install_join_residual(self.builder.as_ref(), plan, join)?;
                                Box::new(PipelineDependencyGuard::new(Box::new(join), dependencies))
                            }
                            BuildResult::Spilled { run, schema } => {
                                let probe_schema = JoinSpillSchema::new(&join_types(&plan.probe_types));
                                let probe_writer =
                                    JoinSpillWriter::new(operator_memory.clone(), probe_schema.clone())?;
                                let downstream: SharedJoinOutput = Arc::new(Mutex::new(next));
                                let spilled = SpilledJoinProbe {
                                    probe_writer: Some(probe_writer),
                                    build_run: run,
                                    build_schema: schema,
                                    probe_schema,
                                    plan: plan.clone(),
                                    builder: Arc::clone(&self.builder),
                                    tracker: operator_memory.clone(),
                                    next: downstream,
                                };
                                Box::new(PipelineDependencyGuard::new(
                                    Box::new(spilled),
                                    dependencies,
                                ))
                            }
                        };
                        let probe_bytes = self
                            .execute_input_with_runtime_filters(
                                probe,
                                Box::new(JoinProbeCounter {
                                    inner: join,
                                    rows: Arc::clone(&probe_rows),
                                    batches: Arc::clone(&probe_batches),
                                }),
                                task,
                                cancel,
                                memory,
                                span,
                                schedule,
                                trace,
                                runtime_filters.clone(),
                                depth + 1,
                            )
                            .await?;
                        let probe_us = probe_started.elapsed().as_micros() as u64;
                        if let Some(filters) = &runtime_filters {
                            let rows_seen = filters.rows_seen();
                            let rows_kept = filters.rows_kept();
                            tracing::info!(
                                target: "vein.audit",
                                event = "join_runtime_filter_completed",
                                node_id,
                                filter_names = ?filters.names(),
                                filter_bytes = filters.memory_bytes(),
                                rows_seen,
                                rows_kept,
                                rows_rejected = rows_seen.saturating_sub(rows_kept),
                                "join runtime filter completed"
                            );
                        }
                        tracing::info!(
                        target: "vein.audit",
                        event = "join_node_finished",
                        depth,
                        join_type = ?plan.join_type,
                        build_rows,
                        build_bytes,
                        build_us,
                        probe_rows = probe_rows.load(Ordering::Relaxed),
                        probe_batches = probe_batches.load(Ordering::Relaxed),
                        probe_bytes,
                        probe_us,
                        "join_node_finished"
                    );
                        Ok(build_bytes.saturating_add(probe_bytes))
                    }
                }
            }.instrument(node_span).await;
            execution_guard.finish(&result);
            result
        })
    }

    async fn execute_with_output<O: ClientOutput>(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
        make_sink: impl FnOnce(
            &dyn ExecutorBuilder,
            &DataSink,
        ) -> Result<Box<dyn Sink<DataChunkView, O>>>
        + Send,
    ) -> Result<TaskResult<O>> {
        let start = Instant::now();
        if cancel.is_cancelled() {
            return Err(VeinError::cancelled());
        }
        let query_id = task.query_id.to_string();
        let task_id = task.task_id.to_string();
        let mut trace = ExecutionTrace::new(&task.plan.root, query_id.clone(), task_id.clone());
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
        let spill_storage = self.spill_storage_factory.create_for_query()?;
        let memory = QueryMemoryTracker::with_spill_storage(
            self.resource_limits,
            cancel.clone(),
            spill_storage,
        );
        trace.attach_memory_tracker(memory.clone());

        let sink_started = Instant::now();
        let sink = make_sink(self.builder.as_ref(), sink_plan)?;
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
        let execution_result = self
            .execute_input(
                input, head, task, &cancel, &memory, &task_span, &schedule, &trace, 0,
            )
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

#[async_trait]
impl TaskExecutor for DefaultTaskExecutor {
    async fn execute(&self, task: &ExecuteTask, cancel: CancellationToken) -> Result<TaskResult> {
        self.execute_with_output(task, cancel, |builder, sink| builder.build_sink(sink))
            .await
    }

    async fn execute_views(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
    ) -> Result<TaskResult<ViewResultBatch>> {
        self.execute_with_output(task, cancel, |builder, sink| {
            builder.build_view_result_sink(sink, None)
        })
        .await
    }

    async fn execute_views_streaming(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
        output: flume::Sender<ViewResultBatch>,
    ) -> Result<TaskResult<ViewResultBatch>> {
        self.execute_with_output(task, cancel, move |builder, sink| {
            builder.build_view_result_sink(sink, Some(output))
        })
        .await
    }

    async fn execute_streaming(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
        output: flume::Sender<RecordBatch>,
    ) -> Result<TaskResult> {
        self.execute_with_output(task, cancel, move |builder, sink| {
            builder.build_streaming_sink(sink, output)
        })
        .await
    }
}

/// A plan's key columns as chunk column positions.
fn direct_scan_probe_columns<'a>(
    probe: &'a PhysicalPlanNode,
    plan: &JoinPlan,
) -> Option<(&'a DataSource, Vec<types::ColumnId>)> {
    let PhysicalPlanOperator::Pipeline {
        source,
        input_schema,
        program,
    } = &probe.operator
    else {
        return None;
    };
    if !program.instructions.is_empty() {
        return None;
    }
    let DataSource::File(scan) = source.as_ref() else {
        return None;
    };
    if plan.probe_keys.is_empty() || plan.probe_keys.len() != plan.build_keys.len() {
        return None;
    }

    let mut columns = Vec::with_capacity(plan.probe_keys.len());
    for (probe_key, build_key) in plan.probe_keys.iter().zip(&plan.build_keys) {
        let probe_index = usize::try_from(*probe_key).ok()?;
        let build_index = usize::try_from(*build_key).ok()?;
        let output_column = probe.output_schema.fields.get(probe_index)?;
        let input_column = input_schema
            .fields
            .iter()
            .find(|column| column.id == output_column.id)?;
        let source_column = scan
            .projected
            .fields
            .iter()
            .find(|column| column.id == output_column.id)?;
        let probe_type = plan.probe_types.get(probe_index)?;
        let build_type = plan.build_types.get(build_index)?;
        if output_column.data_type != *probe_type
            || input_column.data_type != output_column.data_type
            || source_column.data_type != output_column.data_type
            || probe_type != build_type
        {
            return None;
        }
        columns.push(output_column.id);
    }
    Some((source.as_ref(), columns))
}

fn join_keys(columns: &[u32]) -> JoinKeys {
    JoinKeys::new(columns.iter().map(|column| *column as usize).collect())
}

/// One source per file, so a join's build side can be read in parallel.
///
/// An ordinary scan spreads a table's files across the worker pool; the build
/// side of a join was read as a single source, so its files were walked one
/// after another — the largest serial cost in a join. The split mirrors the
/// physical builder's, which is `pub(crate)` there.
pub(crate) fn split_scan_by_file(source: &DataSource) -> Vec<DataSource> {
    let DataSource::File(scan) = source else {
        return vec![source.clone()];
    };
    if scan.files.len() <= 1 {
        return vec![source.clone()];
    }
    scan.files
        .iter()
        .map(|file| {
            let mut one = scan.clone();
            one.files = vec![file.clone()];
            one.task_id = TaskId::new();
            DataSource::File(one)
        })
        .collect()
}

/// A plan's join flavour as the operator's.
///
/// Semi and anti emit each qualifying probe row once and expose no build columns.
fn join_type(kind: JoinKind) -> Result<JoinType> {
    Ok(match kind {
        JoinKind::Inner => JoinType::Inner,
        JoinKind::Left => JoinType::Left,
        JoinKind::Right => JoinType::Right,
        JoinKind::Full => JoinType::Full,
        JoinKind::Semi => JoinType::Semi,
        JoinKind::Anti => JoinType::Anti,
        JoinKind::Single => JoinType::Single,
        JoinKind::Cross => JoinType::Cross,
    })
}

/// A plan's output layout as the operator's.
fn join_outputs(outputs: &[JoinOutput]) -> Vec<OutputColumn> {
    outputs
        .iter()
        .map(|output| match output {
            JoinOutput::Probe(index) => OutputColumn::Probe(*index as usize),
            JoinOutput::Build(index) => OutputColumn::Build(*index as usize),
        })
        .collect()
}

/// Arrow types for one side's columns; a joined null column needs one.
fn join_types(types: &[VeinDataType]) -> Vec<DataType> {
    types.iter().map(DataType::from).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operators::source::MemorySource;
    use crate::spill::{LocalSpillStorage, SpillStorage};
    use arrow::array::Array;
    use common::identifiers::{QueryId, RequestId, StageId, TaskId};
    use common::protocol::{
        AggOpCode, AggregatePlan, ExecutionMode, FileFormat, FileScanTask, JoinKind, JoinOutput,
        JoinPlan, LimitSpec, MicroOp, PhysicalPlan, PhysicalPlanNode, PhysicalPlanOperator,
        SortKey, SortPlan,
    };
    use std::sync::atomic::AtomicUsize;

    struct CountingSpillStorageFactory(Arc<AtomicUsize>);

    impl SpillStorageFactory for CountingSpillStorageFactory {
        fn create_for_query(&self) -> Result<Arc<dyn SpillStorage>> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(Arc::new(LocalSpillStorage::new()))
        }
    }

    #[test]
    fn shared_build_sink_combines_unlimited_worker_tables_without_copying_payloads() {
        use arrow::array::Int64Array;

        let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
        let build = BuildSink::with_spill_support(
            JoinKeys::new(vec![0]),
            JoinKeys::new(vec![0]),
            &[DataType::Int64, DataType::Int64],
            tracker,
        )
        .expect("create spill-capable canonical join sink");
        let mut canonical = SharedBuildSink(Arc::new(Mutex::new(Some(build))));
        let mut first = canonical
            .fork_local()
            .expect("fork first worker state")
            .expect("unlimited joins support local build state");
        let mut second = canonical
            .fork_local()
            .expect("fork second worker state")
            .expect("unlimited joins support local build state");

        let first_payload: arrow::array::ArrayRef = Arc::new(Int64Array::from(vec![10, 20]));
        let second_payload: arrow::array::ArrayRef = Arc::new(Int64Array::from(vec![21, 30]));
        let first_view = DataChunkView::from_chunk(
            DataChunk::from_vectors(vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::clone(&first_payload),
            ]),
        )
        .expect("view first worker build chunk");
        let second_view = DataChunkView::from_chunk(
            DataChunk::from_vectors(vec![
                Arc::new(Int64Array::from(vec![2, 3])),
                Arc::clone(&second_payload),
            ]),
        )
        .expect("view second worker build chunk");

        first.push(first_view).expect("insert first worker chunk");
        second.push(second_view).expect("insert second worker chunk");
        first.flush().expect("finish first worker table");
        second.flush().expect("finish second worker table");
        first
            .combine_local(&mut canonical)
            .expect("merge first worker table");
        second
            .combine_local(&mut canonical)
            .expect("merge second worker table");

        let mut sink = canonical
            .0
            .lock()
            .expect("canonical build lock")
            .take()
            .expect("canonical build sink remains available");
        assert_eq!(sink.rows(), 4);
        assert_eq!(sink.combined_workers(), 2);
        let BuildResult::InMemory(table) = sink.finish_with_spill().expect("finish in-memory build") else {
            panic!("unlimited build should not spill");
        };

        let probe = DataChunk::from_vectors(vec![Arc::new(Int64Array::from(vec![2]))]);
        let mut matches = Vec::new();
        table.probe(&probe, &mut matches).expect("probe merged worker tables");
        assert_eq!(matches.len(), 2, "duplicate keys from both workers are retained");

        let mut payloads = Vec::with_capacity(matches.len());
        for (_, (chunk, row)) in matches {
            let (array, physical_row) = table
                .build_chunk(chunk)
                .resolve(1, row as usize)
                .expect("resolve retained worker payload")
                .expect("build payload is not null-extended");
            payloads.push(
                array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("Int64 payload")
                    .value(physical_row),
            );
            assert!(
                Arc::ptr_eq(array, &first_payload) || Arc::ptr_eq(array, &second_payload),
                "merge retains the original worker Arrow arrays"
            );
        }
        payloads.sort_unstable();
        assert_eq!(payloads, vec![20, 21]);
    }

    #[test]
    fn shared_build_sink_keeps_bounded_memory_on_the_spill_safe_serial_path() {
        let tracker = QueryMemoryTracker::new(
            1 << 20,
            common::cancellation::CancellationToken::new(),
        );
        let build = BuildSink::with_spill_support(
            JoinKeys::new(vec![0]),
            JoinKeys::new(vec![0]),
            &[DataType::Int64],
            tracker,
        )
        .expect("create bounded spill-capable build sink");
        let canonical = SharedBuildSink(Arc::new(Mutex::new(Some(build))));

        assert!(
            canonical.fork_local().expect("check worker-local support").is_none(),
            "independent tables must not bypass the query memory budget and spill path"
        );
    }

    struct MaterializationCollector {
        chunks: Arc<Mutex<Vec<DataChunk>>>,
        flushed: Arc<AtomicBool>,
    }

    impl PipelineNode<DataChunk> for MaterializationCollector {
        fn push(&mut self, chunk: DataChunk) -> Result<()> {
            self.chunks.lock().unwrap().push(chunk);
            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            self.flushed.store(true, Ordering::Relaxed);
            Ok(())
        }

        fn finished(&self) -> bool {
            self.flushed.load(Ordering::Relaxed)
        }
    }

    #[test]
    fn whole_batch_adapter_checks_the_canonical_operator_contract() {
        let source = Arc::new(PhysicalPlanNode {
            output_schema: Arc::new(types::VeinSchema::empty()),
            operator: PhysicalPlanOperator::Pipeline {
                source: Arc::new(DataSource::Memory),
                input_schema: Arc::new(types::VeinSchema::empty()),
                program: Arc::new(MicroProgram::empty()),
            },
        });
        let aggregate = AggregatePlan {
            groups: 0,
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
            aggs: Vec::new(),
            aggregate_input_types: Vec::new(),
            types: Vec::new(),
        };
        let sink = || {
            Box::new(MaterializationCollector {
                chunks: Arc::new(Mutex::new(Vec::new())),
                flushed: Arc::new(AtomicBool::new(false)),
            }) as Box<dyn PipelineNode<DataChunk>>
        };
        for (operator, boundary) in [
            (
                PhysicalPlanOperator::Aggregate {
                    input: Arc::clone(&source),
                    plan: aggregate.clone(),
                },
                "aggregate",
            ),
            (
                PhysicalPlanOperator::Distinct {
                    input: Arc::clone(&source),
                    plan: aggregate,
                },
                "distinct",
            ),
        ] {
            let adapter = MaterializeViewNode::for_operator(sink(), &operator).unwrap();
            assert_eq!(adapter.boundary, boundary);
        }
        let limit = PhysicalPlanOperator::Limit {
            input: Arc::clone(&source),
            spec: LimitSpec {
                offset: 0,
                count: 1,
            },
        };
        let discard = PhysicalPlanOperator::Sink {
            input: Arc::clone(&source),
            sink: Arc::new(DataSink::Discard),
        };
        let client = PhysicalPlanOperator::Sink {
            input: Arc::clone(&source),
            sink: Arc::new(DataSink::Client {
                schema: Arc::new(types::VeinSchema::empty()),
            }),
        };
        for operator in [&source.operator, &limit, &discard, &client] {
            let error = MaterializeViewNode::for_operator(sink(), operator)
                .err()
                .unwrap();
            assert!(
                error
                    .to_string()
                    .contains("does not require whole-batch materialization")
            );
            assert!(error.to_string().contains(operator.data_flow().operator));
        }
    }

    #[test]
    fn materialization_boundary_preserves_values_and_accumulates_conversion_work() {
        use arrow::array::Int64Array;
        use common::arrow::SelectionVector;

        let chunks = Arc::new(Mutex::new(Vec::new()));
        let flushed = Arc::new(AtomicBool::new(false));
        let mut node = MaterializeViewNode::new(
            Box::new(MaterializationCollector {
                chunks: Arc::clone(&chunks),
                flushed: Arc::clone(&flushed),
            }),
            "result",
        );
        let source: arrow::array::ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
        for selection in [
            SelectionVector::new_dense(3),
            SelectionVector::new_sparse(vec![2, 0]),
        ] {
            node.push(
                DataChunkView::from_chunk(DataChunk::new(vec![Arc::clone(&source)], selection))
                    .unwrap(),
            )
            .unwrap();
        }
        assert_eq!(node.batches, 2);
        assert_eq!(node.rows, 5);
        assert_eq!(node.report.shared_columns, 1);
        assert_eq!(node.report.gathered_columns, 1);
        let output = chunks.lock().unwrap();
        let shared = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let gathered = output[1]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            shared.values().as_ptr(),
            source
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ptr()
        );
        assert_eq!(gathered, &Int64Array::from(vec![30, 10]));
        assert_eq!(
            node.report.gathered_output_buffer_bytes,
            gathered.get_buffer_memory_size()
        );
        drop(output);
        assert!(!node.finished());
        node.flush().unwrap();
        assert!(flushed.load(Ordering::Relaxed));
        assert!(node.finished());
    }

    #[test]
    fn physical_join_kinds_map_to_their_executor_types() {
        let mappings = [
            (JoinKind::Inner, JoinType::Inner),
            (JoinKind::Left, JoinType::Left),
            (JoinKind::Right, JoinType::Right),
            (JoinKind::Full, JoinType::Full),
            (JoinKind::Semi, JoinType::Semi),
            (JoinKind::Anti, JoinType::Anti),
            (JoinKind::Single, JoinType::Single),
            (JoinKind::Cross, JoinType::Cross),
        ];

        for (physical, executor) in mappings {
            assert_eq!(
                join_type(physical).expect("join flavor is supported"),
                executor
            );
        }
    }

    // Compact fixture DSL; runtime tests immediately lower it to the canonical plan tree.
    enum InputPlanFixture {
        Pipeline {
            source: DataSource,
            program: Arc<MicroProgram>,
        },
        Transform {
            input: Box<InputPlanFixture>,
            program: Arc<MicroProgram>,
        },
        Aggregate {
            input: Box<InputPlanFixture>,
            plan: AggregatePlan,
        },
        Distinct {
            input: Box<InputPlanFixture>,
            plan: AggregatePlan,
        },
        Sort {
            input: Box<InputPlanFixture>,
            plan: SortPlan,
            key_program: Option<Arc<MicroProgram>>,
            output_columns: u32,
        },
        Limit {
            input: Box<InputPlanFixture>,
            spec: LimitSpec,
        },
        Join {
            probe: Box<InputPlanFixture>,
            build: Box<InputPlanFixture>,
            plan: JoinPlan,
        },
    }

    /// A builder whose only difference from the default is that it can
    /// materialize a `DataSource::Memory` task, which the default builder
    /// rejects because the wire variant carries no payload.
    struct MemoryFeedBuilder {
        inner: crate::factory::DefaultExecutorBuilder,
        values: Vec<Option<i64>>,
    }

    impl MemoryFeedBuilder {
        fn new() -> Self {
            Self::with_values(vec![1, 2, 3, 4, 5])
        }

        fn with_values(values: Vec<i64>) -> Self {
            Self::with_nullable_values(values.into_iter().map(Some).collect())
        }

        fn with_nullable_values(values: Vec<Option<i64>>) -> Self {
            Self {
                inner: crate::factory::DefaultExecutorBuilder::new(Arc::new(
                    crate::udf::FunctionRegistry::new(),
                )),
                values,
            }
        }
    }

    impl ExecutorBuilder for MemoryFeedBuilder {
        fn build_source(
            &self,
            source: &DataSource,
            task_id: TaskId,
            attempt: u32,
        ) -> Result<Box<dyn Source<DataChunkView>>> {
            if !matches!(source, DataSource::Memory) {
                return self.inner.build_source(source, task_id, attempt);
            }
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("dummy", arrow::datatypes::DataType::Int64, true),
            ]));
            let values = Arc::new(arrow::array::Int64Array::from(self.values.clone()));
            let batch = RecordBatch::try_new(schema, vec![values]).expect("valid test batch");
            Ok(Box::new(MemorySource::new(batch)))
        }

        fn build_vm(
            &self,
            program: &Arc<MicroProgram>,
            next: Box<dyn PipelineNode<DataChunk>>,
        ) -> Result<Box<dyn Transform<DataChunk>>> {
            self.inner.build_vm(program, next)
        }

        fn build_view_vm(
            &self,
            program: &Arc<MicroProgram>,
            next: Box<dyn PipelineNode<DataChunkView>>,
        ) -> Result<Box<dyn Transform<DataChunkView>>> {
            self.inner.build_view_vm(program, next)
        }

        fn build_sink(&self, sink: &DataSink) -> Result<Box<dyn Sink<DataChunkView>>> {
            self.inner.build_sink(sink)
        }

        fn build_streaming_sink(
            &self,
            sink: &DataSink,
            output: flume::Sender<RecordBatch>,
        ) -> Result<Box<dyn Sink<DataChunkView>>> {
            self.inner.build_streaming_sink(sink, output)
        }
    }

    /// Supplies one borrowed Arrow batch for each synthetic file in a scan.
    struct MultiFileFeedBuilder {
        inner: crate::factory::DefaultExecutorBuilder,
        source_arrays: Arc<Mutex<Vec<Arc<arrow::array::Int64Array>>>>,
        active_sources: Arc<std::sync::atomic::AtomicUsize>,
        peak_sources: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl MultiFileFeedBuilder {
        fn new() -> Self {
            Self {
                inner: crate::factory::DefaultExecutorBuilder::new(Arc::new(
                    crate::udf::FunctionRegistry::new(),
                )),
                source_arrays: Arc::new(Mutex::new(Vec::new())),
                active_sources: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                peak_sources: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            }
        }
    }

    struct ActiveSourceGuard(Arc<std::sync::atomic::AtomicUsize>);

    impl Drop for ActiveSourceGuard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::Relaxed);
        }
    }

    struct TrackedFileSource {
        batch: Option<RecordBatch>,
        active_sources: Arc<std::sync::atomic::AtomicUsize>,
        peak_sources: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait]
    impl Source<DataChunkView> for TrackedFileSource {
        async fn execute(&mut self, next: &mut dyn PipelineNode<DataChunkView>) -> Result<()> {
            let active = self.active_sources.fetch_add(1, Ordering::Relaxed) + 1;
            self.peak_sources.fetch_max(active, Ordering::Relaxed);
            let _active = ActiveSourceGuard(Arc::clone(&self.active_sources));
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            let batch = self
                .batch
                .take()
                .ok_or_else(|| VeinError::internal("synthetic source batch already consumed"))?;
            next.push(DataChunkView::from_record_batch(batch)?)
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
        ) -> Result<Box<dyn Source<DataChunkView>>> {
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
            let batch = RecordBatch::try_new(Arc::new(int64_schema().to_arrow()), vec![array])
                .map_err(|error| VeinError::internal(format!("build synthetic batch: {error}")))?;
            Ok(Box::new(TrackedFileSource {
                batch: Some(batch),
                active_sources: Arc::clone(&self.active_sources),
                peak_sources: Arc::clone(&self.peak_sources),
            }))
        }

        fn build_vm(
            &self,
            program: &Arc<MicroProgram>,
            next: Box<dyn PipelineNode<DataChunk>>,
        ) -> Result<Box<dyn Transform<DataChunk>>> {
            self.inner.build_vm(program, next)
        }

        fn build_view_vm(
            &self,
            program: &Arc<MicroProgram>,
            next: Box<dyn PipelineNode<DataChunkView>>,
        ) -> Result<Box<dyn Transform<DataChunkView>>> {
            self.inner.build_view_vm(program, next)
        }

        fn build_sink(&self, sink: &DataSink) -> Result<Box<dyn Sink<DataChunkView>>> {
            self.inner.build_sink(sink)
        }

        fn build_streaming_sink(
            &self,
            sink: &DataSink,
            output: flume::Sender<RecordBatch>,
        ) -> Result<Box<dyn Sink<DataChunkView>>> {
            self.inner.build_streaming_sink(sink, output)
        }
    }

    fn runner() -> DefaultTaskExecutor {
        DefaultTaskExecutor::new(Arc::new(MemoryFeedBuilder::new()))
    }

    fn runner_with_values(values: Vec<i64>) -> DefaultTaskExecutor {
        DefaultTaskExecutor::new(Arc::new(MemoryFeedBuilder::with_values(values)))
    }

    #[tokio::test]
    async fn multi_file_scan_uses_bounded_zero_copy_fan_in_on_current_thread_runtime() {
        let builder = Arc::new(MultiFileFeedBuilder::new());
        let runner = DefaultTaskExecutor::new(builder.clone());
        let mut task = task();
        let schema = int64_schema();
        let input = plan_node(
            Arc::clone(&schema),
            PhysicalPlanOperator::Pipeline {
                source: Arc::new(DataSource::File(FileScanTask {
                    files: vec!["3.parquet".into(), "1.parquet".into(), "2.parquet".into()],
                    format: FileFormat::Parquet,
                    byte_range: None,
                    scan_unit_range: None,
                    projected: Arc::clone(&schema),
                    dictionary_columns: Vec::new(),
                    pushdown: None,
                    task_id: TaskId::new(),
                    catalog_snapshot: None,
                })),
                input_schema: Arc::clone(&schema),
                program: Arc::new(MicroProgram::empty()),
            },
        );
        task.plan = plan_with_input(&task, input);

        let result = runner
            .execute(&task, CancellationToken::new())
            .await
            .expect("bounded file scan should complete on a current-thread runtime");

        assert_eq!(builder.max_parallelism(), 2);
        assert_eq!(builder.peak_sources.load(Ordering::Relaxed), 2);
        assert_eq!(result.row_count, 3);
        let mut actual = result
            .output
            .iter()
            .map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .expect("int64 output")
                    .value(0)
            })
            .collect::<Vec<_>>();
        actual.sort_unstable();
        assert_eq!(actual, vec![1, 2, 3]);

        let source_arrays = builder
            .source_arrays
            .lock()
            .expect("test source array mutex")
            .clone();
        for batch in &result.output {
            let output = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("int64 output");
            assert!(
                source_arrays
                    .iter()
                    .any(|source| { source.values().as_ptr() == output.values().as_ptr() })
            );
        }
    }

    #[tokio::test]
    async fn parallel_join_probe_combines_worker_local_aggregate_state() {
        let builder = Arc::new(MultiFileFeedBuilder::new());
        let runner = DefaultTaskExecutor::new(builder.clone());
        let input_schema = int64_schema();
        let leaf = |files: &[&str]| {
            plan_node(
                Arc::clone(&input_schema),
                PhysicalPlanOperator::Pipeline {
                    source: Arc::new(DataSource::File(FileScanTask {
                        files: files.iter().map(|file| (*file).to_owned()).collect(),
                        format: FileFormat::Parquet,
                        byte_range: None,
                        scan_unit_range: None,
                        projected: Arc::clone(&input_schema),
                        dictionary_columns: Vec::new(),
                        pushdown: None,
                        task_id: TaskId::new(),
                        catalog_snapshot: None,
                    })),
                    input_schema: Arc::clone(&input_schema),
                    program: Arc::new(MicroProgram::empty()),
                },
            )
        };
        let join_output_schema = schema_for_types(&[
            VeinDataType::Int64,
            VeinDataType::Int64,
        ]);
        let join = plan_node(
            Arc::clone(&join_output_schema),
            PhysicalPlanOperator::Join {
                probe: leaf(&["3.parquet", "1.parquet", "2.parquet", "2.parquet"]),
                build: leaf(&[
                    "1.parquet",
                    "2.parquet",
                    "2.parquet",
                    "3.parquet",
                ]),
                plan: JoinPlan {
                    join_type: JoinKind::Inner,
                    probe_keys: vec![0],
                    build_keys: vec![0],
                    residual: None,
                    outputs: vec![JoinOutput::Probe(0), JoinOutput::Probe(0)],
                    probe_types: vec![VeinDataType::Int64],
                    build_types: vec![VeinDataType::Int64],
                },
            },
        );
        let aggregate = plan_node(
            schema_for_types(&[VeinDataType::Int64, VeinDataType::Int64]),
            PhysicalPlanOperator::Aggregate {
                input: join,
                plan: AggregatePlan {
                    groups: 1,
                    estimated_groups: None,
                    grouping_sets: None,
                    emit_empty_group_on_empty_input: false,
                    aggs: vec![AggOpCode::Count],
                    aggregate_input_types: vec![VeinDataType::Int64],
                    types: vec![VeinDataType::Int64, VeinDataType::Int64],
                },
            },
        );
        let mut task = task();
        task.plan = plan_with_input(&task, aggregate);

        let result = runner
            .execute(&task, CancellationToken::new())
            .await
            .expect("parallel join probe and aggregate should execute");

        assert_eq!(result.row_count, 3);
        assert_eq!(builder.max_parallelism(), 2);
        assert_eq!(builder.peak_sources.load(Ordering::Relaxed), 2);
        let mut counts = std::collections::BTreeMap::new();
        for batch in &result.output {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("aggregate key");
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("aggregate count");
            for row in 0..batch.num_rows() {
                counts.insert(keys.value(row), values.value(row));
            }
        }
        assert_eq!(counts, std::collections::BTreeMap::from([(1, 1), (2, 4), (3, 1)]));
    }

    #[tokio::test]
    async fn multi_file_worker_failure_is_returned_without_waiting_on_blocked_senders() {
        let builder = Arc::new(MultiFileFeedBuilder::new());
        let runner = DefaultTaskExecutor::new(builder);
        let mut task = task();
        let schema = int64_schema();
        let input = plan_node(
            Arc::clone(&schema),
            PhysicalPlanOperator::Pipeline {
                source: Arc::new(DataSource::File(FileScanTask {
                    files: vec![
                        "1.parquet".into(),
                        "invalid.parquet".into(),
                        "2.parquet".into(),
                    ],
                    format: FileFormat::Parquet,
                    byte_range: None,
                    scan_unit_range: None,
                    projected: Arc::clone(&schema),
                    dictionary_columns: Vec::new(),
                    pushdown: None,
                    task_id: TaskId::new(),
                    catalog_snapshot: None,
                })),
                input_schema: Arc::clone(&schema),
                program: Arc::new(MicroProgram::empty()),
            },
        );
        task.plan = plan_with_input(&task, input);

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            runner.execute(&task, CancellationToken::new()),
        )
        .await
        .expect("failed scan workers must not leave peers blocked")
        .expect_err("an invalid source work item should fail the task");
        assert!(result.to_string().contains("invalid value"));
    }

    #[tokio::test]
    async fn multi_file_scan_cancellation_releases_workers_and_channel_senders() {
        let builder = Arc::new(MultiFileFeedBuilder::new());
        let runner = DefaultTaskExecutor::new(builder.clone());
        let mut task = task();
        let schema = int64_schema();
        let input = plan_node(
            Arc::clone(&schema),
            PhysicalPlanOperator::Pipeline {
                source: Arc::new(DataSource::File(FileScanTask {
                    files: vec!["1.parquet".into(), "2.parquet".into(), "3.parquet".into()],
                    format: FileFormat::Parquet,
                    byte_range: None,
                    scan_unit_range: None,
                    projected: Arc::clone(&schema),
                    dictionary_columns: Vec::new(),
                    pushdown: None,
                    task_id: TaskId::new(),
                    catalog_snapshot: None,
                })),
                input_schema: Arc::clone(&schema),
                program: Arc::new(MicroProgram::empty()),
            },
        );
        task.plan = plan_with_input(&task, input);
        let cancel = CancellationToken::new();
        let cancel_worker = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            cancel_worker.cancel();
        });

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            runner.execute(&task, cancel),
        )
        .await
        .expect("cancelled scans must release bounded-channel senders");
        assert!(result.is_err());
        assert_eq!(builder.active_sources.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn fused_source_program_matches_recursive_transform_execution() {
        let program = Arc::new(MicroProgram::new(
            3,
            vec![
                MicroOp::LoadConstant {
                    dst: 1,
                    value: types::LogicalLiteral::Int64(2),
                },
                MicroOp::CmpGt {
                    dst: 2,
                    src1: 0,
                    src2: 1,
                },
                MicroOp::FilterMask { bool_reg: 2 },
                MicroOp::ProjectRegisters { out_regs: vec![0] },
            ],
        ));
        let empty_program = Arc::new(MicroProgram::empty());

        let mut fused_task = task();
        set_input_plan(
            &mut fused_task,
            InputPlanFixture::Pipeline {
                source: DataSource::Memory,
                program: Arc::clone(&program),
            },
        );
        let mut recursive_task = task();
        set_input_plan(
            &mut recursive_task,
            InputPlanFixture::Transform {
                input: Box::new(InputPlanFixture::Pipeline {
                    source: DataSource::Memory,
                    program: empty_program,
                }),
                program,
            },
        );

        let values = vec![Some(1), None, Some(3), Some(4), Some(5)];
        let fused_runner = DefaultTaskExecutor::new(Arc::new(
            MemoryFeedBuilder::with_nullable_values(values.clone()),
        ));
        let recursive_runner =
            DefaultTaskExecutor::new(Arc::new(MemoryFeedBuilder::with_nullable_values(values)));
        let fused = fused_runner
            .execute(&fused_task, CancellationToken::new())
            .await
            .expect("fused source program should execute");
        let recursive = recursive_runner
            .execute(&recursive_task, CancellationToken::new())
            .await
            .expect("recursive transform should execute");

        assert_eq!(fused.row_count, recursive.row_count);
        assert_eq!(fused.output.len(), recursive.output.len());
        for (fused_batch, recursive_batch) in fused.output.iter().zip(&recursive.output) {
            assert_eq!(fused_batch.schema(), recursive_batch.schema());
            assert_eq!(fused_batch.num_rows(), recursive_batch.num_rows());
            for column_index in 0..fused_batch.num_columns() {
                assert_eq!(
                    fused_batch.column(column_index).to_data(),
                    recursive_batch.column(column_index).to_data(),
                    "fused and recursive execution must produce identical column values"
                );
            }
        }
        let values = fused.output[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("projected values should remain Int64");
        assert_eq!(
            (0..values.len())
                .map(|row| values.value(row))
                .collect::<Vec<_>>(),
            vec![3, 4, 5]
        );
    }

    #[tokio::test]
    async fn fused_and_recursive_programs_preserve_null_outputs() {
        let program = Arc::new(MicroProgram::new(
            1,
            vec![MicroOp::ProjectRegisters { out_regs: vec![0] }],
        ));
        let values = vec![Some(1), None, Some(3)];
        let nullable_schema = Arc::new(types::VeinSchema::new(vec![Arc::new(
            types::ResolvedColumn::new(types::ColumnId(0), "column_0", None, VeinDataType::Int64)
                .with_nullable(true),
        )]));
        let mut fused_task = task();
        fused_task.plan = plan_with_input(
            &fused_task,
            plan_node(
                Arc::clone(&nullable_schema),
                PhysicalPlanOperator::Pipeline {
                    source: Arc::new(DataSource::Memory),
                    input_schema: Arc::clone(&nullable_schema),
                    program: Arc::clone(&program),
                },
            ),
        );
        let mut recursive_task = task();
        recursive_task.plan = plan_with_input(
            &recursive_task,
            plan_node(
                Arc::clone(&nullable_schema),
                PhysicalPlanOperator::Transform {
                    input: plan_node(
                        Arc::clone(&nullable_schema),
                        PhysicalPlanOperator::Pipeline {
                            source: Arc::new(DataSource::Memory),
                            input_schema: nullable_schema,
                            program: Arc::new(MicroProgram::empty()),
                        },
                    ),
                    program,
                },
            ),
        );
        let fused = DefaultTaskExecutor::new(Arc::new(MemoryFeedBuilder::with_nullable_values(
            values.clone(),
        )))
        .execute(&fused_task, CancellationToken::new())
        .await
        .expect("fused projection should preserve NULL values");
        let recursive =
            DefaultTaskExecutor::new(Arc::new(MemoryFeedBuilder::with_nullable_values(values)))
                .execute(&recursive_task, CancellationToken::new())
                .await
                .expect("recursive projection should preserve NULL values");

        let fused_values = fused.output[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("fused projection should return Int64 values");
        let recursive_values = recursive.output[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("recursive projection should return Int64 values");
        assert_eq!(fused_values, recursive_values);
        assert!(fused_values.is_null(1));
    }

    #[tokio::test]
    async fn fused_and_recursive_programs_return_the_same_vm_error() {
        let invalid_program = Arc::new(MicroProgram::new(
            2,
            vec![MicroOp::CmpGt {
                dst: 1,
                src1: 0,
                src2: 99,
            }],
        ));
        let mut fused_task = task();
        set_input_plan(
            &mut fused_task,
            InputPlanFixture::Pipeline {
                source: DataSource::Memory,
                program: Arc::clone(&invalid_program),
            },
        );
        let mut recursive_task = task();
        set_input_plan(
            &mut recursive_task,
            InputPlanFixture::Transform {
                input: Box::new(InputPlanFixture::Pipeline {
                    source: DataSource::Memory,
                    program: Arc::new(MicroProgram::empty()),
                }),
                program: invalid_program,
            },
        );

        let fused_error = runner()
            .execute(&fused_task, CancellationToken::new())
            .await
            .expect_err("fused program must reject an out-of-range register");
        let recursive_error = runner()
            .execute(&recursive_task, CancellationToken::new())
            .await
            .expect_err("recursive program must reject an out-of-range register");
        assert_eq!(fused_error.to_string(), recursive_error.to_string());
    }

    fn schema_for_types(type_list: &[types::VeinDataType]) -> Arc<types::VeinSchema> {
        Arc::new(types::VeinSchema::new(
            type_list
                .iter()
                .enumerate()
                .map(|(index, data_type)| {
                    Arc::new(types::ResolvedColumn::new(
                        types::ColumnId(index),
                        format!("column_{index}"),
                        None,
                        data_type.clone(),
                    ))
                })
                .collect(),
        ))
    }

    fn int64_schema() -> Arc<types::VeinSchema> {
        schema_for_types(&[types::VeinDataType::Int64])
    }

    fn plan_node(
        output_schema: Arc<types::VeinSchema>,
        operator: PhysicalPlanOperator,
    ) -> Arc<PhysicalPlanNode> {
        Arc::new(PhysicalPlanNode {
            output_schema,
            operator,
        })
    }

    fn canonical_input(input: InputPlanFixture) -> Arc<PhysicalPlanNode> {
        match input {
            InputPlanFixture::Pipeline { source, program } => {
                let input_schema = int64_schema();
                plan_node(
                    Arc::clone(&input_schema),
                    PhysicalPlanOperator::Pipeline {
                        source: Arc::new(source),
                        input_schema,
                        program,
                    },
                )
            }
            InputPlanFixture::Transform { input, program } => {
                let input = canonical_input(*input);
                let projected = program.instructions.iter().rev().find_map(|instruction| {
                    if let MicroOp::ProjectRegisters { out_regs } = instruction {
                        Some(out_regs)
                    } else {
                        None
                    }
                });
                let output_schema = if let Some(out_regs) = projected {
                    Arc::new(types::VeinSchema::new(
                        out_regs
                            .iter()
                            .map(|&register| {
                                let column = usize::try_from(register)
                                    .expect("test projection register fits address space");
                                Arc::clone(
                                    input
                                        .output_schema
                                        .fields
                                        .get(column)
                                        .expect("test projection selects an input column"),
                                )
                            })
                            .collect(),
                    ))
                } else {
                    Arc::clone(&input.output_schema)
                };
                plan_node(
                    output_schema,
                    PhysicalPlanOperator::Transform { input, program },
                )
            }
            InputPlanFixture::Aggregate { input, plan } => {
                let output_schema = schema_for_types(&plan.types);
                plan_node(
                    output_schema,
                    PhysicalPlanOperator::Aggregate {
                        input: canonical_input(*input),
                        plan,
                    },
                )
            }
            InputPlanFixture::Distinct { input, plan } => {
                let output_schema = schema_for_types(&plan.types);
                plan_node(
                    output_schema,
                    PhysicalPlanOperator::Distinct {
                        input: canonical_input(*input),
                        plan,
                    },
                )
            }
            InputPlanFixture::Join { probe, build, plan } => {
                let probe = canonical_input(*probe);
                let build = canonical_input(*build);
                let output_schema = Arc::new(types::VeinSchema::new(
                    plan.outputs
                        .iter()
                        .map(|column| {
                            let (field, null_extended) = match column {
                                JoinOutput::Probe(index) => (
                                    Arc::clone(
                                        probe
                                            .output_schema
                                            .fields
                                            .get(*index as usize)
                                            .expect("test join probe output exists"),
                                    ),
                                    matches!(plan.join_type, JoinKind::Right | JoinKind::Full),
                                ),
                                JoinOutput::Build(index) => (
                                    Arc::clone(
                                        build
                                            .output_schema
                                            .fields
                                            .get(*index as usize)
                                            .expect("test join build output exists"),
                                    ),
                                    matches!(
                                        plan.join_type,
                                        JoinKind::Left | JoinKind::Single | JoinKind::Full
                                    ),
                                ),
                            };
                            if null_extended && !field.nullable {
                                Arc::new(field.as_ref().clone().with_nullable(true))
                            } else {
                                field
                            }
                        })
                        .collect(),
                ));
                plan_node(
                    output_schema,
                    PhysicalPlanOperator::Join { probe, build, plan },
                )
            }
            InputPlanFixture::Sort {
                input,
                plan,
                key_program,
                output_columns,
            } => {
                let input = canonical_input(*input);
                plan_node(
                    Arc::clone(&input.output_schema),
                    PhysicalPlanOperator::Sort {
                        input,
                        plan,
                        key_program,
                        output_columns,
                    },
                )
            }
            InputPlanFixture::Limit { input, spec } => {
                let input = canonical_input(*input);
                plan_node(
                    Arc::clone(&input.output_schema),
                    PhysicalPlanOperator::Limit { input, spec },
                )
            }
        }
    }

    fn plan_with_input(task: &ExecuteTask, input: Arc<PhysicalPlanNode>) -> PhysicalPlan {
        let PhysicalPlanOperator::Sink { sink, .. } = &task.plan.root.operator else {
            panic!("task fixture must have a sink root");
        };
        let sink = match sink.as_ref() {
            DataSink::Client { .. } => Arc::new(DataSink::Client {
                schema: Arc::clone(&input.output_schema),
            }),
            _ => Arc::clone(sink),
        };
        PhysicalPlan {
            root: plan_node(
                Arc::clone(&input.output_schema),
                PhysicalPlanOperator::Sink { input, sink },
            ),
        }
    }

    fn set_input_plan(task: &mut ExecuteTask, input: InputPlanFixture) {
        task.plan = plan_with_input(task, canonical_input(input));
    }

    fn task() -> ExecuteTask {
        let schema = int64_schema();
        let input = plan_node(
            Arc::clone(&schema),
            PhysicalPlanOperator::Pipeline {
                source: Arc::new(DataSource::Memory),
                input_schema: Arc::clone(&schema),
                program: Arc::new(MicroProgram {
                    register_count: 3,
                    instructions: vec![
                        MicroOp::LoadConstant {
                            dst: 1,
                            value: types::LogicalLiteral::Int64(2),
                        },
                        MicroOp::CmpGt {
                            dst: 2,
                            src1: 0,
                            src2: 1,
                        },
                        MicroOp::FilterMask { bool_reg: 2 },
                        MicroOp::ProjectRegisters { out_regs: vec![0] },
                    ],
                }),
            },
        );
        ExecuteTask {
            request_id: RequestId::new(),
            mode: ExecutionMode::Batch,
            task_id: TaskId::new(),
            attempt: 0,
            query_id: QueryId::new(),
            stage_id: StageId::new(),
            plan: PhysicalPlan {
                root: plan_node(
                    Arc::clone(&schema),
                    PhysicalPlanOperator::Sink {
                        input,
                        sink: Arc::new(DataSink::Client {
                            schema: Arc::clone(&schema),
                        }),
                    },
                ),
            },
        }
    }

    fn aggregate_task_with_post_filter(threshold: i64) -> ExecuteTask {
        let mut task = task();
        let schema = int64_schema();
        let input = plan_node(
            Arc::clone(&schema),
            PhysicalPlanOperator::Pipeline {
                source: Arc::new(DataSource::Memory),
                input_schema: Arc::clone(&schema),
                program: Arc::new(MicroProgram {
                    register_count: 1,
                    instructions: vec![MicroOp::ProjectRegisters { out_regs: vec![0] }],
                }),
            },
        );
        let aggregate = plan_node(
            Arc::clone(&schema),
            PhysicalPlanOperator::Aggregate {
                input,
                plan: AggregatePlan {
                    groups: 0,
                    estimated_groups: None,
                    grouping_sets: Some(vec![vec![]]),
                    emit_empty_group_on_empty_input: true,
                    aggs: vec![AggOpCode::Count],
                    aggregate_input_types: vec![VeinDataType::Int64],
                    types: vec![VeinDataType::Int64],
                },
            },
        );
        let filtered = plan_node(
            Arc::clone(&schema),
            PhysicalPlanOperator::Transform {
                input: aggregate,
                program: Arc::new(MicroProgram {
                    register_count: 3,
                    instructions: vec![
                        MicroOp::LoadConstant {
                            dst: 1,
                            value: types::LogicalLiteral::Int64(threshold),
                        },
                        MicroOp::CmpGt {
                            dst: 2,
                            src1: 0,
                            src2: 1,
                        },
                        MicroOp::FilterMask { bool_reg: 2 },
                    ],
                }),
            },
        );
        task.plan = plan_with_input(&task, filtered);
        task
    }

    #[tokio::test]
    async fn filters_aggregate_output_in_the_post_aggregate_vm() {
        let task = aggregate_task_with_post_filter(4);
        let result = runner()
            .execute(&task, CancellationToken::new())
            .await
            .expect("post-aggregate filter should execute");

        assert_eq!(result.row_count, 1);
        let count = result.output[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("aggregate count output");
        assert_eq!(count.value(0), 5);
    }

    #[tokio::test]
    async fn post_aggregate_filter_can_remove_every_group() {
        let task = aggregate_task_with_post_filter(5);
        let result = runner()
            .execute(&task, CancellationToken::new())
            .await
            .expect("post-aggregate filter should execute");

        assert_eq!(result.row_count, 0);
        assert!(result.output.is_empty());
    }

    #[tokio::test]
    async fn sorts_task_output_before_applying_limit() {
        let mut task = task();
        let schema = int64_schema();
        let input = plan_node(
            Arc::clone(&schema),
            PhysicalPlanOperator::Pipeline {
                source: Arc::new(DataSource::Memory),
                input_schema: Arc::clone(&schema),
                program: Arc::new(MicroProgram {
                    register_count: 1,
                    instructions: vec![MicroOp::ProjectRegisters { out_regs: vec![0] }],
                }),
            },
        );
        let sorted = plan_node(
            Arc::clone(&schema),
            PhysicalPlanOperator::Sort {
                input,
                plan: SortPlan {
                    keys: vec![common::protocol::SortKey {
                        column: 0,
                        ascending: false,
                        nulls_first: false,
                    }],
                },
                key_program: None,
                output_columns: 1,
            },
        );
        let limited = plan_node(
            Arc::clone(&schema),
            PhysicalPlanOperator::Limit {
                input: sorted,
                spec: LimitSpec {
                    offset: 1,
                    count: 2,
                },
            },
        );
        task.plan = plan_with_input(&task, limited);

        let result = runner_with_values(vec![4, 1, 3, 2])
            .execute(&task, CancellationToken::new())
            .await
            .expect("sort before limit");

        assert_eq!(result.row_count, 2);
        let values = result.output[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("sorted int64 output");
        assert_eq!(values.values(), &[3, 2]);
    }

    #[tokio::test]
    async fn runs_memory_filter_to_client() {
        let result = runner()
            .execute(&task(), CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(result.row_count, 3);
        assert_eq!(result.output.len(), 1);
        let col = result.output[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(col.values(), &[3, 4, 5]);
        assert!(result.metrics.duration_ns > 0);
    }

    #[tokio::test]
    async fn runs_empty_global_count_as_one_zero_row() {
        let mut task = task();
        let schema = int64_schema();
        let input = plan_node(
            Arc::clone(&schema),
            PhysicalPlanOperator::Pipeline {
                source: Arc::new(DataSource::Memory),
                input_schema: Arc::clone(&schema),
                program: Arc::new(MicroProgram {
                    register_count: 1,
                    instructions: vec![MicroOp::ProjectRegisters { out_regs: vec![0] }],
                }),
            },
        );
        let aggregate = plan_node(
            Arc::clone(&schema),
            PhysicalPlanOperator::Aggregate {
                input,
                plan: AggregatePlan {
                    groups: 0,
                    estimated_groups: None,
                    grouping_sets: None,
                    emit_empty_group_on_empty_input: true,
                    aggs: vec![common::protocol::AggOpCode::Count],
                    aggregate_input_types: vec![types::VeinDataType::Int64],
                    types: vec![types::VeinDataType::Int64],
                },
            },
        );
        task.plan = plan_with_input(&task, aggregate);

        let result = runner_with_values(Vec::new())
            .execute(&task, CancellationToken::new())
            .await
            .expect("empty global aggregate should execute");

        assert_eq!(result.row_count, 1);
        assert_eq!(result.output.len(), 1);
        let count = result.output[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("count output");
        assert_eq!(count.values(), &[0]);
    }

    #[tokio::test]
    async fn streams_client_batches_without_retaining_output() {
        let (sender, receiver) = flume::bounded(1);
        let result = runner()
            .execute_streaming(&task(), CancellationToken::new(), sender)
            .await
            .unwrap();
        let batch = receiver.recv_async().await.unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert!(receiver.is_empty());
        assert_eq!(result.row_count, 3);
        assert!(result.output.is_empty());
    }

    #[tokio::test]
    async fn view_and_arrow_results_share_execution_for_join_sort_limit() {
        use arrow::array::Int64Array;

        let builder = Arc::new(MultiFileFeedBuilder::new());
        let runner = DefaultTaskExecutor::new(builder.clone());
        let schema = int64_schema();
        let leaf = || {
            plan_node(
                Arc::clone(&schema),
                PhysicalPlanOperator::Pipeline {
                    source: Arc::new(DataSource::File(FileScanTask {
                        files: vec!["3.parquet".into(), "1.parquet".into(), "2.parquet".into()],
                        format: FileFormat::Parquet,
                        byte_range: None,
                        scan_unit_range: None,
                        projected: Arc::clone(&schema),
                        dictionary_columns: Vec::new(),
                        pushdown: None,
                        task_id: TaskId::new(),
                        catalog_snapshot: None,
                    })),
                    input_schema: Arc::clone(&schema),
                    program: Arc::new(MicroProgram::empty()),
                },
            )
        };
        let join = plan_node(
            Arc::clone(&schema),
            PhysicalPlanOperator::Join {
                probe: leaf(),
                build: leaf(),
                plan: JoinPlan {
                    join_type: JoinKind::Inner,
                    probe_keys: vec![0],
                    build_keys: vec![0],
                    residual: None,
                    outputs: vec![JoinOutput::Probe(0)],
                    probe_types: vec![VeinDataType::Int64],
                    build_types: vec![VeinDataType::Int64],
                },
            },
        );
        let sort = plan_node(
            Arc::clone(&schema),
            PhysicalPlanOperator::Sort {
                input: join,
                plan: SortPlan {
                    keys: vec![SortKey {
                        column: 0,
                        ascending: true,
                        nulls_first: false,
                    }],
                },
                key_program: None,
                output_columns: 1,
            },
        );
        let input = plan_node(
            schema,
            PhysicalPlanOperator::Limit {
                input: sort,
                spec: LimitSpec {
                    offset: 1,
                    count: 2,
                },
            },
        );
        let mut task = task();
        task.plan = plan_with_input(&task, input);
        let views = runner
            .execute_views(&task, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(views.row_count, 2);
        let mut actual = Vec::new();
        let source_arrays = builder.source_arrays.lock().unwrap();
        for batch in &views.output {
            for row in 0..batch.num_rows() {
                let (array, source_row) = batch.view().resolve(0, row).unwrap().unwrap();
                let values = array.as_any().downcast_ref::<Int64Array>().unwrap();
                assert!(
                    source_arrays
                        .iter()
                        .any(|source| std::ptr::eq(source.as_ref(), values))
                );
                actual.push(values.value(source_row));
            }
        }
        assert_eq!(actual, vec![2, 3]);
        drop(source_arrays);
        let arrow = runner
            .execute(&task, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(arrow.row_count, views.row_count);
        assert_eq!(arrow.output.len(), views.output.len());
        for (batch, view) in arrow.output.iter().zip(&views.output) {
            let (converted, report) = view.to_record_batch_with_report().unwrap();
            assert_eq!(&converted, batch);
            assert_eq!(
                report.gathered_columns, 1,
                "payloads gather only at the Arrow boundary"
            );
        }
    }

    #[tokio::test]
    async fn view_results_stream_without_retention_and_propagate_cancellation_and_channel_errors() {
        let (sender, receiver) = flume::bounded(1);
        let result = runner()
            .execute_views_streaming(&task(), CancellationToken::new(), sender)
            .await
            .unwrap();
        let batch = receiver.recv().unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(result.row_count, 3);
        assert!(result.output.is_empty());
        assert!(receiver.is_empty());
        let (sender, receiver) = flume::bounded(1);
        drop(receiver);
        let error = runner()
            .execute_views_streaming(&task(), CancellationToken::new(), sender)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("client result channel closed"));
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(matches!(
            runner().execute_views(&task(), cancel).await,
            Err(VeinError::Cancelled)
        ));
    }

    #[tokio::test]
    async fn view_results_reject_non_client_destinations() {
        let mut task = task();
        let PhysicalPlanOperator::Sink { input, .. } = &task.plan.root.operator else {
            panic!("client task fixture requires a sink");
        };
        let input = Arc::clone(input);
        task.plan = PhysicalPlan {
            root: plan_node(
                Arc::clone(&input.output_schema),
                PhysicalPlanOperator::Sink {
                    input,
                    sink: Arc::new(DataSink::Discard),
                },
            ),
        };
        let error = runner()
            .execute_views(&task, CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(error.category(), error::ErrorCategory::Planning);
        assert!(
            error
                .to_string()
                .contains("view results require a client sink")
        );
    }

    #[tokio::test]
    async fn both_result_apis_preserve_zero_column_counts_and_empty_input() {
        let mut task = task();
        let input = plan_node(
            Arc::new(types::VeinSchema::empty()),
            PhysicalPlanOperator::Pipeline {
                source: Arc::new(DataSource::Memory),
                input_schema: int64_schema(),
                program: Arc::new(MicroProgram::new(
                    1,
                    vec![MicroOp::ProjectRegisters {
                        out_regs: Vec::new(),
                    }],
                )),
            },
        );
        task.plan = plan_with_input(&task, input);
        for values in [vec![], vec![1, 2, 3]] {
            let runner = runner_with_values(values.clone());
            let arrow = runner
                .execute(&task, CancellationToken::new())
                .await
                .unwrap();
            let views = runner
                .execute_views(&task, CancellationToken::new())
                .await
                .unwrap();
            assert_eq!(arrow.row_count, values.len() as u64);
            assert_eq!(views.row_count, arrow.row_count);
            assert_eq!(arrow.output.len(), usize::from(!values.is_empty()));
            assert_eq!(views.output.len(), arrow.output.len());
            for (batch, view) in arrow.output.iter().zip(&views.output) {
                assert_eq!(batch.num_columns(), 0);
                assert_eq!(batch.num_rows(), values.len());
                assert_eq!(view.view().num_columns(), 0);
                assert_eq!(view.num_rows(), values.len());
            }
        }
    }

    #[tokio::test]
    async fn cancellation_aborts() {
        let mut task = task();
        let input = InputPlanFixture::Pipeline {
            source: DataSource::Memory,
            program: Arc::new(MicroProgram::empty()),
        };
        set_input_plan(&mut task, input);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let err = runner().execute(&task, cancel).await.unwrap_err();
        assert!(matches!(err, VeinError::Cancelled));
    }

    /// A join task runs: the runtime reads the build side to completion, then
    /// streams the probe side against the table it produced.
    ///
    /// Both sides are the same `dummy` batch of `1..=5`, so every row matches
    /// itself exactly once.
    #[tokio::test]
    async fn runs_a_join_task() {
        let mut task = task();
        set_input_plan(
            &mut task,
            InputPlanFixture::Sort {
                input: Box::new(InputPlanFixture::Join {
                    probe: Box::new(InputPlanFixture::Pipeline {
                        source: DataSource::Memory,
                        program: Arc::new(MicroProgram::empty()),
                    }),
                    build: Box::new(InputPlanFixture::Pipeline {
                        source: DataSource::Memory,
                        program: Arc::new(MicroProgram::empty()),
                    }),
                    plan: JoinPlan {
                        join_type: JoinKind::Inner,
                        probe_keys: vec![0],
                        build_keys: vec![0],
                        residual: None,
                        outputs: vec![JoinOutput::Probe(0)],
                        probe_types: vec![types::VeinDataType::Int64],
                        build_types: vec![types::VeinDataType::Int64],
                    },
                }),
                plan: SortPlan {
                    keys: vec![common::protocol::SortKey {
                        column: 0,
                        ascending: false,
                        nulls_first: false,
                    }],
                },
                key_program: None,
                output_columns: 1,
            },
        );

        let result = runner()
            .execute(&task, CancellationToken::new())
            .await
            .expect("join task");

        assert_eq!(result.row_count, 5);
        let rows: Vec<i64> = result
            .output
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .expect("int64 output")
                    .clone();
                (0..column.len())
                    .map(|row| column.value(row))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(rows, vec![5, 4, 3, 2, 1]);
    }

    #[tokio::test]
    async fn streams_chained_joins_between_hash_join_nodes() {
        let leaf = || InputPlanFixture::Pipeline {
            source: DataSource::Memory,
            program: Arc::new(MicroProgram::empty()),
        };
        let join_plan = || JoinPlan {
            join_type: JoinKind::Inner,
            probe_keys: vec![0],
            build_keys: vec![0],
            residual: None,
            outputs: vec![JoinOutput::Probe(0), JoinOutput::Build(0)],
            probe_types: vec![types::VeinDataType::Int64],
            build_types: vec![types::VeinDataType::Int64],
        };
        let projected_inner = InputPlanFixture::Transform {
            input: Box::new(InputPlanFixture::Join {
                probe: Box::new(leaf()),
                build: Box::new(leaf()),
                plan: join_plan(),
            }),
            program: Arc::new(MicroProgram {
                register_count: 2,
                instructions: vec![common::protocol::MicroOp::ProjectRegisters {
                    out_regs: vec![0],
                }],
            }),
        };
        let input = InputPlanFixture::Join {
            probe: Box::new(projected_inner),
            build: Box::new(leaf()),
            plan: join_plan(),
        };
        let mut task = task();
        set_input_plan(&mut task, input);

        let result = runner()
            .execute(&task, CancellationToken::new())
            .await
            .expect("chained hash joins should execute");

        assert_eq!(result.row_count, 5);
    }

    #[tokio::test]
    async fn recursive_left_join_applies_residual_before_preserving_unmatched_probe_rows() {
        let leaf = || InputPlanFixture::Pipeline {
            source: DataSource::Memory,
            program: Arc::new(MicroProgram::empty()),
        };
        let mut task = task();
        set_input_plan(
            &mut task,
            InputPlanFixture::Join {
                probe: Box::new(leaf()),
                build: Box::new(leaf()),
                plan: JoinPlan {
                    join_type: JoinKind::Left,
                    probe_keys: vec![0],
                    build_keys: vec![0],
                    residual: Some(common::protocol::JoinResidualPlan {
                        input_columns: vec![JoinOutput::Build(0)],
                        program: Arc::new(MicroProgram::new(
                            3,
                            vec![
                                common::protocol::MicroOp::LoadConstant {
                                    dst: 1,
                                    value: types::LogicalLiteral::Int64(3),
                                },
                                common::protocol::MicroOp::CmpLt {
                                    dst: 2,
                                    src1: 0,
                                    src2: 1,
                                },
                                common::protocol::MicroOp::FilterMask { bool_reg: 2 },
                            ],
                        )),
                    }),
                    outputs: vec![JoinOutput::Probe(0), JoinOutput::Build(0)],
                    probe_types: vec![types::VeinDataType::Int64],
                    build_types: vec![types::VeinDataType::Int64],
                },
            },
        );

        let result = runner()
            .execute(&task, CancellationToken::new())
            .await
            .expect("recursive left join with residual predicate should execute");

        assert_eq!(result.row_count, 5);
        let mut probe_rows = Vec::new();
        let mut build_rows = Vec::new();
        for batch in &result.output {
            let probe = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("probe output");
            let build = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("build output");
            for row in 0..batch.num_rows() {
                probe_rows.push(probe.value(row));
                build_rows.push(build.is_valid(row).then(|| build.value(row)));
            }
        }
        assert_eq!(probe_rows, vec![1, 2, 3, 4, 5]);
        assert_eq!(build_rows, vec![Some(1), Some(2), None, None, None]);
    }

    #[tokio::test]
    async fn recursive_right_join_does_not_mark_residual_rejections_as_matches() {
        let leaf = || InputPlanFixture::Pipeline {
            source: DataSource::Memory,
            program: Arc::new(MicroProgram::empty()),
        };
        let mut task = task();
        set_input_plan(
            &mut task,
            InputPlanFixture::Join {
                probe: Box::new(leaf()),
                build: Box::new(leaf()),
                plan: JoinPlan {
                    join_type: JoinKind::Right,
                    probe_keys: vec![0],
                    build_keys: vec![0],
                    residual: Some(common::protocol::JoinResidualPlan {
                        input_columns: vec![JoinOutput::Build(0)],
                        program: Arc::new(MicroProgram::new(
                            3,
                            vec![
                                common::protocol::MicroOp::LoadConstant {
                                    dst: 1,
                                    value: types::LogicalLiteral::Int64(3),
                                },
                                common::protocol::MicroOp::CmpLt {
                                    dst: 2,
                                    src1: 0,
                                    src2: 1,
                                },
                                common::protocol::MicroOp::FilterMask { bool_reg: 2 },
                            ],
                        )),
                    }),
                    outputs: vec![JoinOutput::Probe(0), JoinOutput::Build(0)],
                    probe_types: vec![types::VeinDataType::Int64],
                    build_types: vec![types::VeinDataType::Int64],
                },
            },
        );

        let result = runner()
            .execute(&task, CancellationToken::new())
            .await
            .expect("recursive right join with residual predicate should execute");

        assert_eq!(result.row_count, 5);
        let probe_rows = result
            .output
            .iter()
            .flat_map(|batch| {
                let probe = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .expect("probe output");
                (0..probe.len())
                    .map(|i| probe.is_valid(i).then(|| probe.value(i)))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let build_rows = result
            .output
            .iter()
            .flat_map(|batch| {
                let build = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .expect("build output");
                (0..build.len()).map(|i| build.value(i)).collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(probe_rows, vec![Some(1), Some(2), None, None, None]);
        assert_eq!(build_rows, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn aggregates_recursive_input_before_forwarding_batches() {
        let mut task = task();
        set_input_plan(
            &mut task,
            InputPlanFixture::Aggregate {
                input: Box::new(InputPlanFixture::Pipeline {
                    source: DataSource::Memory,
                    program: Arc::new(MicroProgram::empty()),
                }),
                plan: AggregatePlan {
                    groups: 0,
                    estimated_groups: None,
                    grouping_sets: Some(vec![vec![]]),
                    emit_empty_group_on_empty_input: true,
                    aggs: vec![AggOpCode::Count],
                    aggregate_input_types: vec![types::VeinDataType::Int64],
                    types: vec![types::VeinDataType::Int64],
                },
            },
        );

        let result = runner()
            .execute(&task, CancellationToken::new())
            .await
            .expect("recursive aggregate should execute");

        assert_eq!(result.row_count, 1);
        let count = result.output[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("count result should be int64");
        assert_eq!(count.value(0), 5);
    }

    #[tokio::test]
    async fn distinct_operator_removes_duplicate_rows_from_recursive_join() {
        let leaf = || InputPlanFixture::Pipeline {
            source: DataSource::Memory,
            program: Arc::new(MicroProgram::empty()),
        };
        let mut task = task();
        set_input_plan(
            &mut task,
            InputPlanFixture::Distinct {
                input: Box::new(InputPlanFixture::Join {
                    probe: Box::new(leaf()),
                    build: Box::new(leaf()),
                    plan: JoinPlan {
                        join_type: JoinKind::Inner,
                        probe_keys: vec![0],
                        build_keys: vec![0],
                        residual: None,
                        outputs: vec![JoinOutput::Probe(0), JoinOutput::Build(0)],
                        probe_types: vec![types::VeinDataType::Int64],
                        build_types: vec![types::VeinDataType::Int64],
                    },
                }),
                plan: AggregatePlan {
                    groups: 2,
                    estimated_groups: None,
                    grouping_sets: Some(vec![vec![0, 1]]),
                    emit_empty_group_on_empty_input: false,
                    aggs: Vec::new(),
                    aggregate_input_types: Vec::new(),
                    types: vec![types::VeinDataType::Int64, types::VeinDataType::Int64],
                },
            },
        );

        let result = runner_with_values(vec![1, 1, 2])
            .execute(&task, CancellationToken::new())
            .await
            .expect("distinct operator should consume duplicate recursive join rows");

        assert_eq!(result.row_count, 2);
        let left = result.output[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("left distinct key");
        let right = result.output[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("right distinct key");
        let rows: Vec<_> = (0..result.output[0].num_rows())
            .map(|row| (left.value(row), right.value(row)))
            .collect();
        assert_eq!(rows, vec![(1, 1), (2, 2)]);
    }

    #[tokio::test]
    async fn recursive_chained_joins_wait_for_each_build_pipeline() {
        let leaf = || InputPlanFixture::Pipeline {
            source: DataSource::Memory,
            program: Arc::new(MicroProgram::empty()),
        };
        let join = || InputPlanFixture::Join {
            probe: Box::new(leaf()),
            build: Box::new(leaf()),
            plan: JoinPlan {
                join_type: JoinKind::Inner,
                probe_keys: vec![0],
                build_keys: vec![0],
                residual: None,
                outputs: vec![JoinOutput::Probe(0)],
                probe_types: vec![VeinDataType::Int64],
                build_types: vec![VeinDataType::Int64],
            },
        };
        let mut task = task();
        set_input_plan(
            &mut task,
            InputPlanFixture::Join {
                probe: Box::new(join()),
                build: Box::new(leaf()),
                plan: JoinPlan {
                    join_type: JoinKind::Inner,
                    probe_keys: vec![0],
                    build_keys: vec![0],
                    residual: None,
                    outputs: vec![JoinOutput::Probe(0)],
                    probe_types: vec![VeinDataType::Int64],
                    build_types: vec![VeinDataType::Int64],
                },
            },
        );

        let result = runner_with_values(vec![3, 1, 2])
            .execute(&task, CancellationToken::new())
            .await
            .expect("chained joins should complete their build dependencies before probing");

        assert_eq!(result.row_count, 3);
        let values = result.output[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("chained join output");
        assert_eq!(
            (0..values.len())
                .map(|row| values.value(row))
                .collect::<Vec<_>>(),
            vec![3, 1, 2]
        );
    }

    #[tokio::test]
    async fn recursive_sort_and_limit_run_in_logical_order_over_join_output() {
        let leaf = || InputPlanFixture::Pipeline {
            source: DataSource::Memory,
            program: Arc::new(MicroProgram::empty()),
        };
        let join = || InputPlanFixture::Join {
            probe: Box::new(leaf()),
            build: Box::new(leaf()),
            plan: JoinPlan {
                join_type: JoinKind::Inner,
                probe_keys: vec![0],
                build_keys: vec![0],
                residual: None,
                outputs: vec![JoinOutput::Probe(0)],
                probe_types: vec![VeinDataType::Int64],
                build_types: vec![VeinDataType::Int64],
            },
        };
        let sort = |input| InputPlanFixture::Sort {
            input: Box::new(input),
            plan: SortPlan {
                keys: vec![SortKey {
                    column: 0,
                    ascending: true,
                    nulls_first: false,
                }],
            },
            key_program: None,
            output_columns: 1,
        };
        let mut task = task();
        set_input_plan(
            &mut task,
            InputPlanFixture::Limit {
                input: Box::new(sort(join())),
                spec: LimitSpec {
                    offset: 1,
                    count: 2,
                },
            },
        );

        let result = runner_with_values(vec![4, 1, 5, 2, 3])
            .execute(&task, CancellationToken::new())
            .await
            .expect("recursive sort then limit should execute");

        assert_eq!(result.row_count, 2);
        let values = result.output[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("sorted output column");
        assert_eq!(
            (0..values.len())
                .map(|row| values.value(row))
                .collect::<Vec<_>>(),
            vec![2, 3]
        );

        // A limit below the sort selects the first two input rows, then orders
        // only those rows. This must produce a different result from above.
        set_input_plan(
            &mut task,
            sort(InputPlanFixture::Limit {
                input: Box::new(join()),
                spec: LimitSpec {
                    offset: 0,
                    count: 2,
                },
            }),
        );
        let result = runner_with_values(vec![4, 1, 5, 2, 3])
            .execute(&task, CancellationToken::new())
            .await
            .expect("recursive limit then sort should execute");
        let values = result.output[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("limited sorted output column");
        assert_eq!(
            (0..values.len())
                .map(|row| values.value(row))
                .collect::<Vec<_>>(),
            vec![1, 4]
        );
    }

    #[tokio::test]
    async fn recursive_aggregate_filter_sort_limit_execute_once_in_order() {
        let leaf = || InputPlanFixture::Pipeline {
            source: DataSource::Memory,
            program: Arc::new(MicroProgram::empty()),
        };
        let join = InputPlanFixture::Join {
            probe: Box::new(leaf()),
            build: Box::new(leaf()),
            plan: JoinPlan {
                join_type: JoinKind::Inner,
                probe_keys: vec![0],
                build_keys: vec![0],
                residual: None,
                outputs: vec![JoinOutput::Probe(0), JoinOutput::Probe(0)],
                probe_types: vec![VeinDataType::Int64],
                build_types: vec![VeinDataType::Int64],
            },
        };
        let aggregate = InputPlanFixture::Aggregate {
            input: Box::new(join),
            plan: AggregatePlan {
                groups: 1,
                estimated_groups: None,
                grouping_sets: Some(vec![vec![0]]),
                emit_empty_group_on_empty_input: false,
                aggs: vec![AggOpCode::Count],
                aggregate_input_types: vec![VeinDataType::Int64],
                types: vec![VeinDataType::Int64, VeinDataType::Int64],
            },
        };
        let filtered = InputPlanFixture::Transform {
            input: Box::new(aggregate),
            program: Arc::new(MicroProgram::new(
                4,
                vec![
                    MicroOp::LoadConstant {
                        dst: 2,
                        value: types::LogicalLiteral::Int64(1),
                    },
                    MicroOp::CmpGt {
                        dst: 3,
                        src1: 1,
                        src2: 2,
                    },
                    MicroOp::FilterMask { bool_reg: 3 },
                ],
            )),
        };
        let sorted = InputPlanFixture::Sort {
            input: Box::new(filtered),
            plan: SortPlan {
                keys: vec![SortKey {
                    column: 1,
                    ascending: false,
                    nulls_first: false,
                }],
            },
            key_program: None,
            output_columns: 2,
        };
        let mut task = task();
        set_input_plan(
            &mut task,
            InputPlanFixture::Limit {
                input: Box::new(sorted),
                spec: LimitSpec {
                    offset: 0,
                    count: 1,
                },
            },
        );

        let result = runner_with_values(vec![3, 1, 1, 2])
            .execute(&task, CancellationToken::new())
            .await
            .expect("recursive aggregate, filter, sort, and limit should execute");

        assert_eq!(result.row_count, 1);
        let key = result.output[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("group key");
        let count = result.output[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("group count");
        assert_eq!(key.value(0), 1);
        assert_eq!(count.value(0), 4);
    }

    #[tokio::test]
    async fn configured_memory_limit_reaches_recursive_aggregate_state() {
        let input = InputPlanFixture::Pipeline {
            source: DataSource::Memory,
            program: Arc::new(MicroProgram::empty()),
        };
        let aggregate = InputPlanFixture::Aggregate {
            input: Box::new(input),
            plan: AggregatePlan {
                groups: 0,
                estimated_groups: None,
                grouping_sets: Some(vec![Vec::new()]),
                emit_empty_group_on_empty_input: true,
                aggs: vec![AggOpCode::Count],
                aggregate_input_types: vec![VeinDataType::Int64],
                types: vec![VeinDataType::Int64],
            },
        };
        let mut task = task();
        set_input_plan(&mut task, aggregate);
        let runner = DefaultTaskExecutor::with_memory_limit(
            Arc::new(MemoryFeedBuilder::with_values(vec![1, 2, 3])),
            0,
        );

        let error = runner
            .execute(&task, CancellationToken::new())
            .await
            .expect_err("recursive aggregate must use the configured execution budget");
        assert_eq!(error.category(), error::ErrorCategory::Resource);
        let message = error.to_string();
        assert!(
            matches!(
                &error,
                VeinError::ResourceExhausted {
                    resource,
                    requested,
                    limit: 0,
                    unit: error::ResourceUnit::Bytes,
                } if resource == "aggregate group state" && *requested > 0
            ),
            "expected a bounded aggregate-state memory error, received {error:?}"
        );
        assert!(
            message.starts_with("Resource exhausted: aggregate group state, requested "),
            "resource errors should identify the bounded state concisely: {message}"
        );
        assert!(message.ends_with("bytes, limit 0 bytes"), "{message}");
        assert!(!message.contains("PhysicalPlanOperator"), "{message}");
    }

    fn full_join_spill_task() -> ExecuteTask {
        let input = || InputPlanFixture::Pipeline {
            source: DataSource::Memory,
            program: Arc::new(MicroProgram::empty()),
        };
        let plan = JoinPlan {
            join_type: JoinKind::Full,
            probe_keys: vec![0],
            build_keys: vec![0],
            residual: None,
            outputs: vec![JoinOutput::Probe(0), JoinOutput::Build(0)],
            probe_types: vec![VeinDataType::Int64],
            build_types: vec![VeinDataType::Int64],
        };
        let mut task = task();
        let join = InputPlanFixture::Join {
            probe: Box::new(input()),
            build: Box::new(input()),
            plan,
        };
        set_input_plan(&mut task, join);
        task
    }

    fn full_join_feed() -> Arc<MemoryFeedBuilder> {
        let mut values = (0..128).map(Some).collect::<Vec<_>>();
        values.extend([Some(20), None]);
        Arc::new(MemoryFeedBuilder::with_nullable_values(values))
    }

    fn join_result_pairs(output: &[RecordBatch]) -> Vec<(Option<i64>, Option<i64>)> {
        let mut pairs = output
            .iter()
            .flat_map(|batch| {
                let probe = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .expect("probe output");
                let build = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .expect("build output");
                (0..batch.num_rows()).map(|row| {
                    (
                        probe.is_valid(row).then(|| probe.value(row)),
                        build.is_valid(row).then(|| build.value(row)),
                    )
                })
            })
            .collect::<Vec<_>>();
        pairs.sort_unstable();
        pairs
    }

    #[tokio::test]
    async fn full_join_spill_matches_in_memory_result_under_configured_budgets() {
        let task = full_join_spill_task();
        let in_memory = DefaultTaskExecutor::new(full_join_feed())
            .execute(&task, CancellationToken::new())
            .await
            .expect("in-memory full join");
        let spilling = DefaultTaskExecutor::with_resource_limits(
            full_join_feed(),
            ExecutionResourceLimits::new(20_000, 1_000_000),
        )
        .execute(&task, CancellationToken::new())
        .await
        .expect("full join spills within configured disk quota");

        assert_eq!(in_memory.row_count, 133);
        assert_eq!(spilling.row_count, in_memory.row_count);
        assert_eq!(
            join_result_pairs(&spilling.output),
            join_result_pairs(&in_memory.output),
            "spilled output preserves duplicate matches and null extensions"
        );
    }

    #[tokio::test]
    async fn full_join_spill_respects_query_disk_quota() {
        let task = full_join_spill_task();
        let factory_calls = Arc::new(AtomicUsize::new(0));
        let executor = DefaultTaskExecutor::with_resource_limits(
            full_join_feed(),
            ExecutionResourceLimits::new(20_000, 0),
        )
        .with_spill_storage_factory(Arc::new(CountingSpillStorageFactory(Arc::clone(
            &factory_calls,
        ))));

        let error = executor
            .execute(&task, CancellationToken::new())
            .await
            .expect_err("zero spill quota rejects a build-side run");

        assert_eq!(error.category(), error::ErrorCategory::Resource);
        assert!(
            matches!(error, VeinError::ResourceExhausted { .. }),
            "expected spill-quota exhaustion, received {error:?}"
        );
        assert_eq!(factory_calls.load(Ordering::Relaxed), 1);
    }
}
