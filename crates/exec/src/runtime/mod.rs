//! The executor's execution seam: the traits that decouple pipeline
//! construction and task execution from the wire protocol and the concrete
//! stage implementations.
//!
//! These are the abstractions the rest of the engine programs against:
//! a [`ExecutorBuilder`] selects concrete [`Source`]/[`Transform`]/[`Sink`]
//! implementations from a wire task, and a [`TaskExecutor`] runs a single
//! task end-to-end (see `crate::pipeline`).

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::Instrument;

use crate::pipeline::{DataChunk, PipelineNode, Sink, Source, Transform};
use crate::operators::sink::WriteReceipt;
use common::Result;
use common::cancellation::CancellationToken;
use common::identifiers::TaskId;
use common::protocol::{DataSink, DataSource, ExecuteTask, MicroProgram};
use error::VeinError;

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

impl DefaultTaskExecutor {
    /// Create a task executor that builds pipelines with `builder`.
    pub fn new(builder: Arc<dyn ExecutorBuilder>) -> Self {
        Self { builder }
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

        let sink_started = Instant::now();
        let sink = if let Some(sender) = output_sender {
            self.builder.build_streaming_sink(&task.sink, sender)?
        } else {
            self.builder.build_sink(&task.sink)?
        };
        let sink: Arc<Mutex<Box<dyn Sink<DataChunk>>>> = Arc::new(Mutex::new(sink));
        let head: Box<dyn PipelineNode<DataChunk>> = Box::new(CancelGuard::new(
            Box::new(SinkHandle(sink.clone())),
            cancel.clone(),
        ));
        let sink_build_us = sink_started.elapsed().as_micros() as u64;

        let vm_started = Instant::now();
        let mut vm = self.builder.build_vm(&task.program, head)?;
        let vm_build_us = vm_started.elapsed().as_micros() as u64;

        let source_started = Instant::now();
        let mut source: Box<dyn Source<DataChunk>> =
            self.builder
                .build_source(&task.source, task.task_id, task.attempt)?;
        let source_build_us = source_started.elapsed().as_micros() as u64;

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
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(VeinError::cancelled()),
            result = source.execute(&mut *vm).instrument(task_span) => result?,
        }
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

        let bytes_read = source.bytes_read();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operators::source::MemorySource;
    use common::identifiers::{QueryId, RequestId, StageId, TaskId};
    use common::protocol::{ExecutionMode, MicroOp};

    /// A builder whose only difference from the default is that it can
    /// materialize a `DataSource::Memory` task, which the default builder
    /// rejects because the wire variant carries no payload.
    struct MemoryFeedBuilder {
        inner: crate::factory::DefaultExecutorBuilder,
    }

    impl MemoryFeedBuilder {
        fn new() -> Self {
            Self {
                inner: crate::factory::DefaultExecutorBuilder::new(Arc::new(
                    crate::udf::FunctionRegistry::new(),
                )),
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
            let values = Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3, 4, 5]));
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

    fn task() -> ExecuteTask {
        ExecuteTask {
            request_id: RequestId::new(),
            mode: ExecutionMode::Batch,
            task_id: TaskId::new(),
            attempt: 0,
            query_id: QueryId::new(),
            stage_id: StageId::new(),
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
        }
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
}
