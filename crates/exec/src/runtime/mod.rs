//! The executor's execution seam: the traits that decouple pipeline
//! construction and task execution from the wire protocol and the concrete
//! stage implementations.
//!
//! These are the abstractions the rest of the engine programs against:
//! a [`ExecutorBuilder`] selects concrete [`Source`]/[`Transform`]/[`Sink`]
//! implementations from a wire task, and a [`TaskExecutor`] runs a single
//! task end-to-end (see `crate::pipeline`).

use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::Instrument;

use crate::operators::{LimitTransform, SortTransform};
use crate::operators::aggregate;
use crate::operators::join::{BuildSink, HashJoin, HashTable, JoinKeys, JoinType, OutputColumn};
use crate::operators::sink::WriteReceipt;
use crate::pipeline::{DataChunk, PipelineNode, Sink, Source, Transform};
use common::Result;
use common::cancellation::CancellationToken;
use common::identifiers::TaskId;
use common::protocol::{
    AggregatePlan, DataSink, DataSource, ExecuteTask, JoinKind, JoinOutput, JoinPlan, LimitSpec,
    MicroProgram, PhysicalInputPlan, SortPlan,
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
#[derive(Debug, Clone, Default)]
pub struct TaskResult {
    /// Number of rows produced by the pipeline.
    pub row_count: u64,
    /// Captured output batches (populated by a [`Sink`] that retains results).
    pub output: Vec<RecordBatch>,
    /// Execution metrics (bytes read, duration).
    pub metrics: TaskMetrics,
    /// Present only after a storage sink has committed successfully.
    pub write_receipt: Option<WriteReceipt>,
}

/// Builds pipeline stages for a task from the wire protocol.
///
/// Decouples `exec` from `common::protocol`: the executor's own
/// [`Source`], [`Transform`], and [`Sink`] implementations are selected here
/// from the self-contained task description a worker receives.
pub trait ExecutorBuilder: Send + Sync {
    /// Build a source from the task's [`DataSource`] and attempt identity.
    fn build_source(
        &self,
        source: &DataSource,
        task_id: TaskId,
        attempt: u32,
    ) -> Result<Box<dyn Source<DataChunk>>>;

    /// Build a Vector VM transform interpreting the task's [`MicroProgram`].
    fn build_vm(
        &self,
        program: &Arc<MicroProgram>,
        next: Box<dyn PipelineNode<DataChunk>>,
    ) -> Result<Box<dyn Transform<DataChunk>>>;

    /// Build a sink from the task's [`DataSink`]. The sink is the terminal
    /// stage; a sink that returns data (e.g. `DataSink::Client`) exposes it via
    /// [`Sink::collect`].
    fn build_sink(&self, sink: &DataSink) -> Result<Box<dyn Sink<DataChunk>>>;

    /// Build a sink that forwards client batches through a bounded output channel.
    ///
    /// Non-client sinks may use the regular sink implementation because their
    /// results are persisted or discarded rather than returned to the client.
    fn build_streaming_sink(
        &self,
        sink: &DataSink,
        output: flume::Sender<RecordBatch>,
    ) -> Result<Box<dyn Sink<DataChunk>>> {
        let _ = output;
        self.build_sink(sink)
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
struct CancelGuard<B> {
    inner: Box<dyn PipelineNode<B>>,
    cancel: CancellationToken,
}

impl<B> CancelGuard<B> {
    fn new(inner: Box<dyn PipelineNode<B>>, cancel: CancellationToken) -> Self {
        Self { inner, cancel }
    }
}

impl<B> PipelineNode<B> for CancelGuard<B> {
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
struct SinkHandle<B>(Arc<Mutex<Box<dyn Sink<B>>>>);

impl<B> PipelineNode<B> for SinkHandle<B> {
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
struct TransformHandle(Box<dyn Transform<DataChunk>>);

impl PipelineNode<DataChunk> for TransformHandle {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        self.0.push(chunk)
    }

    fn flush(&mut self) -> Result<()> {
        self.0.flush()
    }

    fn finished(&self) -> bool {
        self.0.finished()
    }
}

/// Shares a build sink with the join executor while a source owns its pipeline.
struct SharedBuildSink(Arc<Mutex<Option<BuildSink>>>);

impl PipelineNode<DataChunk> for SharedBuildSink {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        self.0.lock().map_err(|_| VeinError::internal("join build mutex poisoned"))?
            .as_mut().ok_or_else(|| VeinError::internal("join build sink already finished"))?
            .push(chunk)
    }
    fn flush(&mut self) -> Result<()> {
        self.0.lock().map_err(|_| VeinError::internal("join build mutex poisoned"))?
            .as_mut().ok_or_else(|| VeinError::internal("join build sink already finished"))?
            .flush()
    }
}

/// Serializes pushes from concurrent file readers into one downstream node.
struct SharedPipelineNode(Arc<Mutex<Box<dyn PipelineNode<DataChunk>>>>);

impl PipelineNode<DataChunk> for SharedPipelineNode {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        self.0.lock().map_err(|_| VeinError::internal("pipeline node mutex poisoned"))?.push(chunk)
    }
    fn flush(&mut self) -> Result<()> {
        self.0.lock().map_err(|_| VeinError::internal("pipeline node mutex poisoned"))?.flush()
    }
    fn finished(&self) -> bool {
        self.0.lock().map(|node| node.finished()).unwrap_or(false)
    }
}

/// Counts active rows and chunks entering a join without inspecting or cloning columns.
struct JoinProbeCounter {
    inner: Box<dyn PipelineNode<DataChunk>>,
    rows: Arc<AtomicU64>,
    batches: Arc<AtomicU64>,
}

impl PipelineNode<DataChunk> for JoinProbeCounter {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        self.rows.fetch_add(chunk.num_rows() as u64, Ordering::Relaxed);
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.inner.push(chunk)
    }
    fn flush(&mut self) -> Result<()> { self.inner.flush() }
    fn finished(&self) -> bool { self.inner.finished() }
}

/// A [`PipelineNode`] adapter that forwards pushes and swallows the flush.
///
/// A join's probe side is split into one chain per file, all feeding the one
/// shared sink, so each chain's flush is swallowed and the task flushes the sink
/// exactly once — rather than once per file, which would commit a write sink
/// repeatedly.
struct SuppressFlush<B> {
    inner: Box<dyn PipelineNode<B>>,
}

impl<B> SuppressFlush<B> {
    fn new(inner: Box<dyn PipelineNode<B>>) -> Self {
        Self { inner }
    }
}

impl<B> PipelineNode<B> for SuppressFlush<B> {
    fn push(&mut self, batch: B) -> Result<()> {
        self.inner.push(batch)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
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
}

/// What the second half of a breaker task — a join's probe or an aggregate —
/// cost to build and how much it read.
struct StageTiming {
    bytes_read: u64,
    vm_build_us: u64,
    source_build_us: u64,
}

impl DefaultTaskExecutor {
    /// Create a task executor that builds pipelines with `builder`.
    pub fn new(builder: Arc<dyn ExecutorBuilder>) -> Self {
        Self { builder }
    }

    /// Execute a recursive input tree with bounded batches between operators.
    fn execute_input<'a>(
        &'a self,
        input: &'a PhysicalInputPlan,
        mut next: Box<dyn PipelineNode<DataChunk>>,
        task: &'a ExecuteTask,
        cancel: &'a CancellationToken,
        span: &'a tracing::Span,
        depth: usize,
    ) -> Pin<Box<dyn Future<Output=Result<u64>> + Send + 'a>> {
        Box::pin(async move {
            if cancel.is_cancelled() { return Err(VeinError::cancelled()); }
            match input {
                PhysicalInputPlan::Pipeline { source: data_source, program } => {
                    let sources = split_scan(data_source);
                    if sources.len() > 1 {
                        let shared = Arc::new(Mutex::new(next));
                        let mut workers = Vec::with_capacity(sources.len());
                        for source_spec in sources {
                            let builder = Arc::clone(&self.builder);
                            let shared = Arc::clone(&shared);
                            let program = Arc::clone(program);
                            let task_id = task.task_id;
                            let attempt = task.attempt;
                            let cancel = cancel.clone();
                            let span = span.clone();
                            workers.push(tokio::spawn(async move {
                                let mut source = builder.build_source(&source_spec, task_id, attempt)?;
                                let next: Box<dyn PipelineNode<DataChunk>> = Box::new(
                                    SuppressFlush::new(Box::new(SharedPipelineNode(shared))),
                                );
                                if program.instructions.is_empty() {
                                    let mut next = next;
                                    tokio::select! {
                                        biased;
                                        _ = cancel.cancelled() => Err(VeinError::cancelled()),
                                        result = source.execute(&mut *next).instrument(span) => {
                                            result?;
                                            Ok(source.bytes_read())
                                        }
                                    }
                                } else {
                                    let mut vm = builder.build_vm(&program, next)?;
                                    tokio::select! {
                                        biased;
                                        _ = cancel.cancelled() => Err(VeinError::cancelled()),
                                        result = source.execute(&mut *vm).instrument(span) => {
                                            result?;
                                            Ok(source.bytes_read())
                                        }
                                    }
                                }
                            }));
                        }
                        let mut bytes = 0u64;
                        let mut worker_error = None;
                        for worker in workers {
                            match worker.await {
                                Ok(Ok(worker_bytes)) => bytes = bytes.saturating_add(worker_bytes),
                                Ok(Err(error)) => { worker_error.get_or_insert(error); }
                                Err(error) => {
                                    worker_error.get_or_insert_with(|| {
                                        VeinError::internal(format!("input scan task failed: {error}"))
                                    });
                                }
                            };
                        }
                        if let Some(error) = worker_error {
                            return Err(error);
                        }
                        shared.lock().map_err(|_| VeinError::internal("pipeline node mutex poisoned"))?.flush()?;
                        return Ok(bytes);
                    }
                    let mut source = self.builder.build_source(data_source, task.task_id, task.attempt)?;
                    if program.instructions.is_empty() {
                        tokio::select! {
                            biased;
                            _ = cancel.cancelled() => Err(VeinError::cancelled()),
                            result = source.execute(&mut *next).instrument(span.clone()) => {
                                result?;
                                Ok(source.bytes_read())
                            }
                        }
                    } else {
                        let mut vm = self.builder.build_vm(program, next)?;
                        tokio::select! {
                            biased;
                            _ = cancel.cancelled() => Err(VeinError::cancelled()),
                            result = source.execute(&mut *vm).instrument(span.clone()) => {
                                result?;
                                Ok(source.bytes_read())
                            }
                        }
                    }
                }
                PhysicalInputPlan::Transform { input, program } => {
                    let transform = self.builder.build_vm(program, next)?;
                    let bytes = self.execute_input(
                        input,
                        Box::new(TransformHandle(transform)),
                        task,
                        cancel,
                        span,
                        depth + 1,
                    ).await?;
                    Ok(bytes)
                }
                PhysicalInputPlan::Aggregate { input, plan } => {
                    let (accumulator, handle) = aggregate::shared(plan.clone())?;
                    let bytes = self
                        .execute_input(input, handle, task, cancel, span, depth + 1)
                        .await?;
                    let groups = accumulator
                        .lock()
                        .map_err(|_| VeinError::internal("aggregate mutex poisoned"))?
                        .finish()?;
                    for batch in groups {
                        next.push(batch)?;
                    }
                    next.flush()?;
                    tracing::debug!(
                        target: "vein.audit",
                        event = "recursive_aggregate_finished",
                        depth,
                        input_bytes = bytes,
                        "recursive_aggregate_finished"
                    );
                    Ok(bytes)
                }
                PhysicalInputPlan::Join { probe, build, plan } => {
                    let started = Instant::now();
                    let build_sink = Arc::new(Mutex::new(Some(BuildSink::new(
                        join_keys(&plan.probe_keys),
                        join_keys(&plan.build_keys),
                    ))));
                    let build_bytes = self.execute_input(
                        build,
                        Box::new(SharedBuildSink(Arc::clone(&build_sink))),
                        task,
                        cancel,
                        span,
                        depth + 1,
                    ).await?;
                    let build_sink = build_sink.lock()
                        .map_err(|_| VeinError::internal("join build mutex poisoned"))?
                        .take().ok_or_else(|| VeinError::internal("join build sink missing after execution"))?;
                    let table = build_sink.finish();
                    let build_rows = table.len() as u64;
                    let build_us = started.elapsed().as_micros() as u64;

                    let kind = join_type(plan.join_type)?;
                    let join = HashJoin::new(
                        Arc::new(table),
                        kind,
                        join_outputs(&plan.outputs),
                        join_types(&plan.probe_types),
                        join_types(&plan.build_types),
                        next,
                    );
                    let probe_rows = Arc::new(AtomicU64::new(0));
                    let probe_batches = Arc::new(AtomicU64::new(0));
                    let probe_started = Instant::now();
                    let probe_bytes = self.execute_input(
                        probe,
                        Box::new(JoinProbeCounter {
                            inner: Box::new(join),
                            rows: Arc::clone(&probe_rows),
                            batches: Arc::clone(&probe_batches),
                        }),
                        task,
                        cancel,
                        span,
                        depth + 1,
                    ).await?;
                    let probe_us = probe_started.elapsed().as_micros() as u64;
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
        })
    }

    /// The probe half of a join: one chain per file of the probe source, sharing
    /// the table the build phase produced.
    ///
    /// A join compiles to a single task, so without this its probe files would be
    /// read and probed one after another — while an ordinary scan already spreads
    /// a table's files across the worker pool. The chain construction is
    /// sequential, so the per-chain build times are summed rather than sampled.
    ///
    /// Only the probe-local flavours fan out. A right or full join owes unmatched
    /// *build* rows, which each chain tracks in its own table; splitting the
    /// probe would make every chain emit the same unmatched rows.
    async fn probe_join(
        &self,
        task: &ExecuteTask,
        table: HashTable,
        plan: &JoinPlan,
        sink: Arc<Mutex<Box<dyn Sink<DataChunk>>>>,
        cancel: CancellationToken,
        span: &tracing::Span,
    ) -> Result<StageTiming> {
        let kind = join_type(plan.join_type)?;
        let columns = join_outputs(&plan.outputs);
        let probe_types = join_types(&plan.probe_types);
        let build_types = join_types(&plan.build_types);
        let table = Arc::new(table);

        // A window counts rows for the whole task, so a limited probe is not
        // fanned out: every chain would otherwise emit the window on its own.
        let sources = if task.limit.is_none()
            && task.sort.is_none()
            && matches!(kind, JoinType::Inner | JoinType::Left)
        {
            split_scan(&task.source)
        } else {
            vec![task.source.clone()]
        };

        let mut vm_build_us = 0u64;
        let mut source_build_us = 0u64;
        let mut chains = Vec::with_capacity(sources.len());
        for source in sources {
            let head = with_window(
                Box::new(CancelGuard::new(
                    Box::new(SuppressFlush::new(Box::new(SinkHandle(sink.clone())))),
                    cancel.clone(),
                )),
                task.limit,
            );
            let head = ordered_head(
                task.sort.clone(),
                task.sort_output_columns,
                task.limit,
                head,
            );
            let head = sort_key_head(
                self.builder.as_ref(),
                task.sort_key_program.as_ref(),
                head,
            )?;

            let vm_started = Instant::now();
            let mut vm = HashJoin::new(
                Arc::clone(&table),
                kind,
                columns.clone(),
                probe_types.clone(),
                build_types.clone(),
                head,
            );
            vm_build_us += vm_started.elapsed().as_micros() as u64;

            let source_started = Instant::now();
            let mut source = self
                .builder
                .build_source(&source, task.task_id, task.attempt)?;
            source_build_us += source_started.elapsed().as_micros() as u64;

            let cancel = cancel.clone();
            let span = span.clone();
            chains.push(tokio::spawn(async move {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Err(VeinError::cancelled()),
                    result = source.execute(&mut vm).instrument(span) => result?,
                }
                Ok::<_, VeinError>(source.bytes_read())
            }));
        }

        let mut bytes_read = 0u64;
        for chain in chains {
            bytes_read += chain.await.map_err(|error| {
                VeinError::internal(format!("join probe task failed: {error}"))
            })??;
        }

        // The chains suppressed their own flushes, so the sink is flushed once
        // here, after every file has been probed.
        sink.lock()
            .map_err(|_| VeinError::internal("sink mutex poisoned"))?
            .flush()?;

        Ok(StageTiming {
            bytes_read,
            vm_build_us,
            source_build_us,
        })
    }

    /// The aggregate half of a task: fold the whole program into the
    /// accumulator, then emit the groups.
    ///
    /// The program still runs, so the filters and the projections below the
    /// aggregate happen in the VM; what the accumulator receives are the
    /// program's output registers, which the compiler laid out as the group keys
    /// followed by one column per aggregate argument.
    async fn run_aggregate(
        &self,
        task: &ExecuteTask,
        plan: AggregatePlan,
        sink: &Arc<Mutex<Box<dyn Sink<DataChunk>>>>,
        cancel: &CancellationToken,
        span: &tracing::Span,
    ) -> Result<StageTiming> {
        let post_aggregate_program = task.post_aggregate_program.as_ref();
        let (accumulator, handle) = aggregate::shared(plan)?;

        let vm_started = Instant::now();
        let mut vm = self.builder.build_vm(&task.program, handle)?;
        let mut vm_build_us = vm_started.elapsed().as_micros() as u64;

        let source_started = Instant::now();
        let mut source = self
            .builder
            .build_source(&task.source, task.task_id, task.attempt)?;
        let source_build_us = source_started.elapsed().as_micros() as u64;

        // The window applies to the groups, not to the rows that produced them.
        let head: Box<dyn PipelineNode<DataChunk>> = with_window(
            Box::new(CancelGuard::new(
                Box::new(SinkHandle(sink.clone())),
                cancel.clone(),
            )),
            task.limit,
        );
        let mut head = ordered_head(
            task.sort.clone(),
            task.sort_output_columns,
            task.limit,
            head,
        );
        head = sort_key_head(
            self.builder.as_ref(),
            task.sort_key_program.as_ref(),
            head,
        )?;
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(VeinError::cancelled()),
            result = source.execute(&mut *vm).instrument(span.clone()) => result?,
        }

        let finish_started = Instant::now();
        let (groups, aggregate_metrics, group_count) = {
            let mut accumulator = accumulator
                .lock()
                .map_err(|_| VeinError::internal("aggregate mutex poisoned"))?;
            let groups = accumulator.finish()?;
            let group_count = accumulator.group_count();
            (groups, accumulator.metrics(), group_count)
        };
        let finish_ns = finish_started.elapsed().as_nanos() as u64;
        tracing::info!(
            target: "vein.audit",
            event = "aggregate_finished",
            input_batches = aggregate_metrics.input_batches,
            input_rows = aggregate_metrics.input_rows,
            grouping_sets = aggregate_metrics.grouping_sets,
            grouping_probes = aggregate_metrics.grouping_probes,
            group_index_candidate_checks = aggregate_metrics.group_index_candidate_checks,
            group_index_reserve_ns = aggregate_metrics.group_index_reserve_ns,
            group_index_capacity_growth_events = aggregate_metrics.group_index_capacity_growth_events,
            group_index_entries_at_capacity_growth = aggregate_metrics.group_index_entries_at_capacity_growth,
            group_index_capacity = aggregate_metrics.group_index_capacity,
            group_index_entries_ns = aggregate_metrics.group_index_entries_ns,
            full_hash_matches = aggregate_metrics.full_hash_matches,
            group_key_comparisons = aggregate_metrics.group_key_comparisons,
            group_collision_retries = aggregate_metrics.group_collision_retries,
            group_index_hits = aggregate_metrics.group_index_hits,
            group_index_misses = aggregate_metrics.group_index_misses,
            new_groups = aggregate_metrics.new_groups,
            groups = group_count,
            retained_key_slots_initialized = aggregate_metrics.retained_key_slots_initialized,
            string_key_bytes_copied = aggregate_metrics.string_key_bytes_copied,
            hasher_setup_ns = aggregate_metrics.hasher_setup_ns,
            key_hash_ns = aggregate_metrics.key_hash_ns,
            hash_finalize_ns = aggregate_metrics.hash_finalize_ns,
            hash_generation_ns = aggregate_metrics.hash_generation_ns,
            group_resolution_ns = aggregate_metrics.group_resolution_ns,
            group_materialize_ns = aggregate_metrics.group_materialize_ns,
            group_ids_distribution_ns = aggregate_metrics.group_ids_distribution_ns,
            state_update_ns = aggregate_metrics.state_update_ns,
            output_materialize_ns = aggregate_metrics.output_materialize_ns,
            finish_ns,
            "aggregate_finished"
        );
        if let Some(program) = post_aggregate_program {
            let vm_started = Instant::now();
            let mut post_vm = self.builder.build_vm(program, head)?;
            vm_build_us += vm_started.elapsed().as_micros() as u64;
            for chunk in groups {
                post_vm.push(chunk)?;
            }
            post_vm.flush()?;
        } else {
            for chunk in groups {
                head.push(chunk)?;
            }
            head.flush()?;
        }

        Ok(StageTiming {
            bytes_read: source.bytes_read(),
            vm_build_us,
            source_build_us,
        })
    }

    async fn execute_with_output(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
        output_sender: Option<flume::Sender<RecordBatch>>,
    ) -> Result<TaskResult> {
        let start = Instant::now();
        if cancel.is_cancelled() {
            return Err(VeinError::cancelled());
        }

        // A join reads its build side to completion before the probe starts.
        // That ordering is what makes it a pipeline breaker, and it is the
        // runtime's job because only the runtime knows when a source is done.
        let join = if task.input_plan.is_some() { None } else {
            match (&task.build, &task.join) {
                (Some(build_source), Some(plan)) => {
                    let build_started = Instant::now();
                    let probe_keys = join_keys(&plan.probe_keys);
                    let build_keys = join_keys(&plan.build_keys);

                    // One build per file, run concurrently, then merged. Reading the
                    // build side as a single source walked its files one after
                    // another, which is the largest serial cost in a join — an
                    // ordinary scan already spreads a table's files across workers.
                    let sources = split_scan(build_source);
                    let builds_used = sources.len();
                    let mut builds = Vec::with_capacity(sources.len());
                    for source in sources {
                        let builder = self.builder.clone();
                        let sink = BuildSink::new(probe_keys.clone(), build_keys.clone());
                        let task_id = task.task_id;
                        let attempt = task.attempt;
                        builds.push(tokio::spawn(async move {
                            let mut sink = sink;
                            let mut source = builder.build_source(&source, task_id, attempt)?;
                            source.execute(&mut sink).await?;
                            Ok::<_, VeinError>(sink.finish())
                        }));
                    }

                    let mut table: Option<HashTable> = None;
                    for build in builds {
                        let partial = build.await.map_err(|error| {
                            VeinError::internal(format!("join build task failed: {error}"))
                        })??;
                        match &mut table {
                            Some(table) => table.merge(partial)?,
                            None => table = Some(partial),
                        }
                    }
                    let table =
                        table.ok_or_else(|| VeinError::internal("a join build produced no chunks"))?;

                    // The whole build phase — reading it *and* hashing it — so the
                    // join's two halves can be compared without summing scan events.
                    let build_rows = table.len() as u64;
                    let build_us = build_started.elapsed().as_micros() as u64;
                    tracing::info!(
                    target: "vein.audit",
                    event = "join_build_phase",
                    rows = build_rows,
                    builds = builds_used,
                    build_us,
                    "join_build_phase"
                );
                    Some((table, plan))
                }
                _ => None,
            }
        };

        let sink_started = Instant::now();
        let sink = if let Some(sender) = output_sender {
            self.builder.build_streaming_sink(&task.sink, sender)?
        } else {
            self.builder.build_sink(&task.sink)?
        };
        let sink: Arc<Mutex<Box<dyn Sink<DataChunk>>>> = Arc::new(Mutex::new(sink));
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
        let run_started = Instant::now();
        // A group by breaks the pipeline the way a join does — nothing can be
        // emitted until every row has been folded — so it is the runtime's job,
        // and it takes precedence over the join path a task can never also be on.
        let aggregate_timing = match task.aggregate.clone() {
            Some(plan) => Some(
                self.run_aggregate(task, plan, &sink, &cancel, &task_span)
                    .await?,
            ),
            None => None,
        };

        let (bytes_read, vm_build_us, source_build_us) = match aggregate_timing {
            Some(timing) => (
                timing.bytes_read,
                timing.vm_build_us,
                timing.source_build_us,
            ),
            None => match join {
                // A join probes with the table the build phase produced. Its probe
                // source is fanned out over its files, all sharing that one table.
                Some((table, plan)) => {
                    let probe = self
                        .probe_join(task, table, plan, sink.clone(), cancel.clone(), &task_span)
                        .await?;
                    (probe.bytes_read, probe.vm_build_us, probe.source_build_us)
                }
                None if task.input_plan.is_some() => {
                    let head = with_window(
                        Box::new(CancelGuard::new(
                            Box::new(SinkHandle(sink.clone())),
                            cancel.clone(),
                        )),
                        task.limit,
                    );
                    let head = ordered_head(task.sort.clone(), task.sort_output_columns, task.limit, head);
                    let head = sort_key_head(
                        self.builder.as_ref(),
                        task.sort_key_program.as_ref(),
                        head,
                    )?;
                    let source_build_started = Instant::now();
                    let input_plan = task.input_plan.as_ref().expect("guarded above");
                    let bytes = self.execute_input(
                        input_plan,
                        head,
                        task,
                        &cancel,
                        &task_span,
                        0,
                    ).await?;
                    (bytes, 0, source_build_started.elapsed().as_micros() as u64)
                }
                None => {
                    let head = with_window(
                        Box::new(CancelGuard::new(
                            Box::new(SinkHandle(sink.clone())),
                            cancel.clone(),
                        )),
                        task.limit,
                    );
                    let head = ordered_head(
                        task.sort.clone(),
                        task.sort_output_columns,
                        task.limit,
                        head,
                    );
                    let head = sort_key_head(
                        self.builder.as_ref(),
                        task.sort_key_program.as_ref(),
                        head,
                    )?;
                    let vm_started = Instant::now();
                    let mut vm: Box<dyn Transform<DataChunk>> =
                        self.builder.build_vm(&task.program, head)?;
                    let vm_build_us = vm_started.elapsed().as_micros() as u64;

                    let source_started = Instant::now();
                    let mut source: Box<dyn Source<DataChunk>> =
                        self.builder
                            .build_source(&task.source, task.task_id, task.attempt)?;
                    let source_build_us = source_started.elapsed().as_micros() as u64;

                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return Err(VeinError::cancelled()),
                        result = source.execute(&mut *vm).instrument(task_span.clone()) => result?,
                    }
                    (source.bytes_read(), vm_build_us, source_build_us)
                }
            },
        };
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
            vm_build_us,
            source_build_us,
            run_us,
            collect_us,
            rows = row_count,
            output_batches = output.len(),
            bytes_read,
            total_us = duration_ns / 1000,
            "task_finished"
        );

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

#[async_trait]
impl TaskExecutor for DefaultTaskExecutor {
    async fn execute(&self, task: &ExecuteTask, cancel: CancellationToken) -> Result<TaskResult> {
        self.execute_with_output(task, cancel, None).await
    }

    async fn execute_streaming(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
        output: flume::Sender<RecordBatch>,
    ) -> Result<TaskResult> {
        self.execute_with_output(task, cancel, Some(output)).await
    }
}

/// A plan's key columns as chunk column positions.
fn join_keys(columns: &[u32]) -> JoinKeys {
    JoinKeys::new(columns.iter().map(|column| *column as usize).collect())
}

/// Wraps a task's terminal chain in its row window, when it has one.
///
/// The window sits immediately below the program so it counts rows the query
/// actually emits, and above the sink so a single window covers the whole task.
fn with_window(
    next: Box<dyn PipelineNode<DataChunk>>,
    limit: Option<LimitSpec>,
) -> Box<dyn PipelineNode<DataChunk>> {
    match limit {
        Some(limit) => Box::new(LimitTransform::new(limit.offset, limit.count, next)),
        None => next,
    }
}

/// Places a blocking global sort before the task's output window.
fn ordered_head(
    sort: Option<SortPlan>,
    output_columns: Option<u32>,
    limit: Option<LimitSpec>,
    next: Box<dyn PipelineNode<DataChunk>>,
) -> Box<dyn PipelineNode<DataChunk>> {
    match sort {
        Some(plan) => Box::new(SortTransform::with_limit(
            plan,
            output_columns.map(|columns| columns as usize),
            limit,
            next,
        )),
        None => next,
    }
}

fn sort_key_head(
    builder: &dyn ExecutorBuilder,
    program: Option<&Arc<MicroProgram>>,
    next: Box<dyn PipelineNode<DataChunk>>,
) -> Result<Box<dyn PipelineNode<DataChunk>>> {
    match program {
        Some(program) => Ok(Box::new(TransformHandle(
            builder.build_vm(program, next)?,
        ))),
        None => Ok(next),
    }
}

/// One source per file, so a join's build side can be read in parallel.
///
/// An ordinary scan spreads a table's files across the worker pool; the build
/// side of a join was read as a single source, so its files were walked one
/// after another — the largest serial cost in a join. The split mirrors the
/// physical builder's, which is `pub(crate)` there.
fn split_scan(source: &DataSource) -> Vec<DataSource> {
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
/// The semi and anti flavours are refused rather than mapped onto the nearest
/// outer join: they emit a probe row *once*, with no build columns, which is a
/// different output shape, not a variation of this one.
fn join_type(kind: JoinKind) -> Result<JoinType> {
    Ok(match kind {
        JoinKind::Inner => JoinType::Inner,
        JoinKind::Left => JoinType::Left,
        JoinKind::Right => JoinType::Right,
        JoinKind::Full => JoinType::Full,
        JoinKind::Semi | JoinKind::Anti => {
            return Err(VeinError::internal(format!(
                "the hash join does not implement {kind:?} yet"
            )));
        }
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
    use common::identifiers::{QueryId, RequestId, StageId, TaskId};
    use common::protocol::{
        AggOpCode, AggregatePlan, ExecutionMode, JoinKind, JoinOutput, JoinPlan, MicroOp,
        PhysicalInputPlan,
    };

    /// A builder whose only difference from the default is that it can
    /// materialize a `DataSource::Memory` task, which the default builder
    /// rejects because the wire variant carries no payload.
    struct MemoryFeedBuilder {
        inner: crate::factory::DefaultExecutorBuilder,
        values: Vec<i64>,
    }

    impl MemoryFeedBuilder {
        fn new() -> Self {
            Self::with_values(vec![1, 2, 3, 4, 5])
        }

        fn with_values(values: Vec<i64>) -> Self {
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
        ) -> Result<Box<dyn Source<DataChunk>>> {
            if !matches!(source, DataSource::Memory) {
                return self.inner.build_source(source, task_id, attempt);
            }
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("dummy", arrow::datatypes::DataType::Int64, false),
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

        fn build_sink(&self, sink: &DataSink) -> Result<Box<dyn Sink<DataChunk>>> {
            self.inner.build_sink(sink)
        }

        fn build_streaming_sink(
            &self,
            sink: &DataSink,
            output: flume::Sender<RecordBatch>,
        ) -> Result<Box<dyn Sink<DataChunk>>> {
            self.inner.build_streaming_sink(sink, output)
        }
    }

    fn runner() -> DefaultTaskExecutor {
        DefaultTaskExecutor::new(Arc::new(MemoryFeedBuilder::new()))
    }

    fn runner_with_values(values: Vec<i64>) -> DefaultTaskExecutor {
        DefaultTaskExecutor::new(Arc::new(MemoryFeedBuilder::with_values(values)))
    }

    fn task() -> ExecuteTask {
        ExecuteTask {
            request_id: RequestId::new(),
            mode: ExecutionMode::Batch,
            task_id: TaskId::new(),
            attempt: 0,
            query_id: QueryId::new(),
            stage_id: StageId::new(),
            input_plan: None,
            source: DataSource::Memory, // supplied by MemoryFeedBuilder: Int64 [1,2,3,4,5]
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
            sink: Arc::new(DataSink::Client {
                schema: Arc::new(types::VeinSchema::empty()),
            }),
            build: None,
            join: None,
            limit: None,
            sort: None,
            sort_key_program: None,
            sort_output_columns: None,
            aggregate: None,
            post_aggregate_program: None,
        }
    }

    fn aggregate_task_with_post_filter(threshold: i64) -> ExecuteTask {
        let mut task = task();
        task.program = Arc::new(MicroProgram {
            register_count: 1,
            instructions: vec![MicroOp::ProjectRegisters { out_regs: vec![0] }],
        });
        task.aggregate = Some(AggregatePlan {
            groups: 0,
            estimated_groups: None,
            grouping_sets: Some(vec![vec![]]),
            aggs: vec![AggOpCode::Count],
            aggregate_input_types: vec![VeinDataType::Int64],
            types: vec![VeinDataType::Int64],
        });
        task.post_aggregate_program = Some(Arc::new(MicroProgram {
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
        }));
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
        task.program = Arc::new(MicroProgram {
            register_count: 1,
            instructions: vec![MicroOp::ProjectRegisters { out_regs: vec![0] }],
        });
        task.sort = Some(SortPlan {
            keys: vec![common::protocol::SortKey {
                column: 0,
                ascending: false,
                nulls_first: false,
            }],
        });
        task.limit = Some(LimitSpec { offset: 1, count: 2 });

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
        task.aggregate = Some(AggregatePlan {
            groups: 0,
            estimated_groups: None,
            grouping_sets: None,
            aggs: vec![common::protocol::AggOpCode::Count],
            aggregate_input_types: vec![types::VeinDataType::Int64],
            types: vec![types::VeinDataType::Int64],
        });

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
    async fn cancellation_aborts() {
        let mut task = task();
        task.program = Arc::new(MicroProgram::empty());
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
        task.program = Arc::new(MicroProgram::empty());
        task.build = Some(DataSource::Memory);
        task.join = Some(JoinPlan {
            join_type: JoinKind::Inner,
            probe_keys: vec![0],
            build_keys: vec![0],
            outputs: vec![JoinOutput::Probe(0)],
            probe_types: vec![types::VeinDataType::Int64],
            build_types: vec![types::VeinDataType::Int64],
        });
        task.sort = Some(SortPlan {
            keys: vec![common::protocol::SortKey {
                column: 0,
                ascending: false,
                nulls_first: false,
            }],
        });

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
        let leaf = || PhysicalInputPlan::Pipeline {
            source: DataSource::Memory,
            program: Arc::new(MicroProgram::empty()),
        };
        let join_plan = || JoinPlan {
            join_type: JoinKind::Inner,
            probe_keys: vec![0],
            build_keys: vec![0],
            outputs: vec![JoinOutput::Probe(0)],
            probe_types: vec![types::VeinDataType::Int64],
            build_types: vec![types::VeinDataType::Int64],
        };
        let projected_inner = PhysicalInputPlan::Transform {
            input: Box::new(PhysicalInputPlan::Join {
                probe: Box::new(leaf()),
                build: Box::new(leaf()),
                plan: join_plan(),
            }),
            program: Arc::new(MicroProgram {
                register_count: 1,
                instructions: vec![common::protocol::MicroOp::ProjectRegisters {
                    out_regs: vec![0],
                }],
            }),
        };
        let input = PhysicalInputPlan::Join {
            probe: Box::new(projected_inner),
            build: Box::new(leaf()),
            plan: join_plan(),
        };
        let mut task = task();
        task.program = Arc::new(MicroProgram::empty());
        task.input_plan = Some(Arc::new(input));

        let result = runner()
            .execute(&task, CancellationToken::new())
            .await
            .expect("chained hash joins should execute");

        assert_eq!(result.row_count, 5);
    }

    #[tokio::test]
    async fn aggregates_recursive_input_before_forwarding_batches() {
        let mut task = task();
        task.program = Arc::new(MicroProgram::empty());
        task.input_plan = Some(Arc::new(PhysicalInputPlan::Aggregate {
            input: Box::new(PhysicalInputPlan::Pipeline {
                source: DataSource::Memory,
                program: Arc::new(MicroProgram::empty()),
            }),
            plan: AggregatePlan {
                groups: 0,
                estimated_groups: None,
                grouping_sets: Some(vec![vec![]]),
                aggs: vec![AggOpCode::Count],
                aggregate_input_types: vec![types::VeinDataType::Int64],
                types: vec![types::VeinDataType::Int64],
            },
        }));

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
}
