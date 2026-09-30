//! The atomic unit of work executed by the local engine.

use common::identifiers::{PartitionId, QueryId, RequestId, StageId, TaskId};
use common::protocol::{DataSink, DataSource, ExecuteTask, ExecutionMode, MicroProgram};
use std::sync::Arc;

/// The smallest executable unit of a physical plan.
///
/// A [`Task`] describes one self-contained slice of work — a [`DataSource`],
/// a [`MicroProgram`], and a [`DataSink`] — that a single executor runs
/// to completion. The local runtime converts it into an [`ExecuteTask`].
#[derive(Debug, Clone)]
pub struct Task {
    /// The unique identifier of this task.
    pub task_id: TaskId,
    /// Stable partition identity for this input task.
    pub partition: PartitionId,
    /// The data source this task reads from.
    pub source: DataSource,
    /// The micro-IR bytecode program executed by the Vector VM.
    pub program: Arc<MicroProgram>,
    /// Where the task's output is written.
    pub sink: Arc<DataSink>,
}

impl Task {
    /// Create a new task.
    pub fn new(
        task_id: TaskId,
        partition: PartitionId,
        source: DataSource,
        program: Arc<MicroProgram>,
        sink: Arc<DataSink>,
    ) -> Self {
        Self {
            task_id,
            partition,
            source,
            program,
            sink,
        }
    }

    /// Convert this task into a dispatchable [`ExecuteTask`], filling in the
    /// owning query and stage from the caller.
    pub fn into_execute_task(
        self,
        request_id: RequestId,
        query_id: QueryId,
        stage_id: StageId,
        mode: ExecutionMode,
    ) -> ExecuteTask {
        let sink = match &*self.sink {
            DataSink::Storage(target) => {
                let mut target = target.clone();
                target.set_option("partition_id", self.partition.into_raw().to_string());
                Arc::new(DataSink::Storage(target))
            }
            _ => self.sink.clone(),
        };

        ExecuteTask {
            request_id,
            mode,
            task_id: self.task_id,
            attempt: 0,
            query_id,
            stage_id,
            source: self.source,
            program: self.program,
            sink,
        }
    }
}
