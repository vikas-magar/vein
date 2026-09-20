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
use bytes::Bytes;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::pipeline::{DataChunk, PipelineNode, Sink, Source, Transform};
use common::Result;
use common::cancellation::CancellationToken;
use common::identifiers::TaskId;
use common::protocol::{DataSink, DataSource, ExecuteTask, MicroProgram, VeinControlEvent};
use error::VeinError;

/// Operator telemetry and profiling.
pub mod profile;
pub use profile::{OperatorMetrics, OperatorProfile, OperatorSnapshot, ProfileNode};

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
        program: &MicroProgram,
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
/// Marked `?Send` because driving a [`Source`] awaits `Source::execute`, whose
/// future is `!Send`; the executor runs these on a Tokio `LocalSet`, which
/// supports `!Send` tasks.
#[async_trait(?Send)]
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

/// Serializes produced [`RecordBatch`]es into wire bytes for the data plane
/// (e.g. Arrow IPC). Backs `DataSink::Client` when results cross the network.
pub trait OutputEncoder: Send + Sync {
    /// Encode a slice of batches into a single byte payload.
    fn encode(&self, batches: &[RecordBatch]) -> Result<Bytes>;
}

/// Default [`OutputEncoder`] backed by the Arrow IPC streaming format.
#[derive(Debug, Default, Clone)]
pub struct ArrowIpcOutputEncoder;

impl ArrowIpcOutputEncoder {
    /// Create a new [`ArrowIpcOutputEncoder`].
    pub fn new() -> Self {
        Self
    }

    /// Encodes exactly one batch as a self-contained IPC stream.
    ///
    /// Result frames use this unit so the receiver can decode each batch
    /// independently while preserving bounded transport memory.
    pub fn encode_batch(&self, batch: &RecordBatch) -> Result<Bytes> {
        let mut encoder = common::arrow::IpcStreamEncoder::new();
        encoder.encode(batch)?;
        Ok(Bytes::from(encoder.finalize()?))
    }
}

impl OutputEncoder for ArrowIpcOutputEncoder {
    fn encode(&self, batches: &[RecordBatch]) -> Result<Bytes> {
        if batches.is_empty() {
            return Ok(Bytes::new());
        }
        let mut encoder = common::arrow::IpcStreamEncoder::new();
        for batch in batches {
            encoder.encode(batch)?;
        }
        let bytes = encoder.finalize()?;
        Ok(Bytes::from(bytes))
    }
}

/// Sends control-plane events (completion, failure, cancel) to the master.
#[async_trait]
pub trait ControlTransport: Send + Sync {
    /// Send one control event to the master.
    async fn send(&self, event: VeinControlEvent) -> Result<()>;
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

        let sink = if let Some(sender) = output_sender {
            self.builder.build_streaming_sink(&task.sink, sender)?
        } else {
            self.builder.build_sink(&task.sink)?
        };
        let sink: Arc<Mutex<Box<dyn Sink<DataChunk>>>> = Arc::new(Mutex::new(sink));
        let head: Box<dyn PipelineNode<DataChunk>> =
            Box::new(CancelGuard::new(Box::new(SinkHandle(sink.clone())), cancel));

        let mut vm = self.builder.build_vm(&task.program, head)?;
        let mut source: Box<dyn Source<DataChunk>> =
            self.builder
                .build_source(&task.source, task.task_id, task.attempt)?;
        source.execute(&mut *vm).await?;

        let mut sink = sink
            .lock()
            .map_err(|_| VeinError::internal("sink mutex poisoned"))?;
        let output = sink.collect();
        let row_count = sink
            .row_count()
            .max(output.iter().map(|batch| batch.num_rows() as u64).sum());
        let duration_ns = start.elapsed().as_nanos() as u64;

        Ok(TaskResult {
            row_count,
            output,
            metrics: TaskMetrics {
                bytes_read: source.bytes_read(),
                duration_ns,
            },
        })
    }
}

#[async_trait(?Send)]
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
    use crate::factory::TaskExecutorBuilder;
    use common::identifiers::{QueryId, StageId, TaskId};
    use common::protocol::{ExecutionMode, MicroOp, ScalarConstant};

    fn runner() -> DefaultTaskExecutor {
        TaskExecutorBuilder::new().build()
    }

    fn task() -> ExecuteTask {
        ExecuteTask {
            mode: ExecutionMode::Batch,
            task_id: TaskId::new(),
            attempt: 0,
            query_id: QueryId::new(),
            stage_id: StageId::new(),
            source: DataSource::Memory, // dummy: Int64 [1,2,3,4,5]
            program: MicroProgram {
                register_count: 3,
                instructions: vec![
                    MicroOp::LoadConstant {
                        dst: 1,
                        value: ScalarConstant::Int64(2),
                    },
                    MicroOp::CmpGt {
                        dst: 2,
                        src1: 0,
                        src2: 1,
                    },
                    MicroOp::FilterMask { bool_reg: 2 },
                    MicroOp::ProjectRegisters { out_regs: vec![0] },
                ],
            },
            sink: DataSink::Client { schema: vec![] },
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
        task.program = MicroProgram::empty();
        let cancel = CancellationToken::new();
        cancel.cancel();
        let err = runner().execute(&task, cancel).await.unwrap_err();
        assert!(matches!(err, VeinError::Cancelled));
    }

    #[tokio::test]
    async fn runs_exo_native_table_scan_to_client() {
        use common::protocol::{FieldSchema, FileFormat, FileScanTask};
        use exo::PartWriter;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let table_path = dir.path();

        // Write a partition with "id" and "amount"
        {
            let mut writer = PartWriter::create(table_path, 0, 256).unwrap();
            let ids: Vec<i64> = (1..=10).collect();
            let amounts: Vec<f64> = (1..=10).map(|i| (i * 10) as f64).collect();
            writer.write_primitive_column("id", &ids).unwrap();
            writer.write_primitive_column("amount", &amounts).unwrap();
            writer.finish().unwrap();
        }

        let scan_task = FileScanTask {
            files: vec![table_path.to_string_lossy().to_string()],
            format: FileFormat::Exo,
            byte_range: None,
            column_ids: vec![0, 1],
            projected_schema: vec![
                FieldSchema {
                    field_id: 0,
                    name: "id".to_string(),
                    data_type: "Int64".to_string(),
                },
                FieldSchema {
                    field_id: 1,
                    name: "amount".to_string(),
                    data_type: "Float64".to_string(),
                },
            ],
            scan_predicates: Vec::new(),
            task_id: TaskId::new(),
            shuffle_target_path: None,
        };

        let task = ExecuteTask {
            mode: ExecutionMode::Batch,
            task_id: TaskId::new(),
            attempt: 0,
            query_id: QueryId::new(),
            stage_id: StageId::new(),
            source: DataSource::File(scan_task),
            program: MicroProgram {
                register_count: 3,
                instructions: vec![
                    // Filter: amount > 50.0 (reg 1 is amount, reg 2 is const)
                    MicroOp::LoadConstant {
                        dst: 2,
                        value: ScalarConstant::Float64(50.0),
                    },
                    MicroOp::CmpGt {
                        dst: 2,
                        src1: 1,
                        src2: 2,
                    },
                    MicroOp::FilterMask { bool_reg: 2 },
                    MicroOp::ProjectRegisters {
                        out_regs: vec![0, 1],
                    },
                ],
            },
            sink: DataSink::Client { schema: vec![] },
        };

        let result = runner()
            .execute(&task, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(result.row_count, 5); // 60.0, 70.0, 80.0, 90.0, 100.0
        assert_eq!(result.output.len(), 1);
        let batch = &result.output[0];
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 5);

        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(id_col.values(), &[6, 7, 8, 9, 10]);

        let amount_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(amount_col.values(), &[60.0, 70.0, 80.0, 90.0, 100.0]);
    }
}
