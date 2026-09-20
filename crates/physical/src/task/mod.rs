//! The atomic unit of work dispatched to a single executor.

use common::identifiers::{PartitionId, QueryId, StageId, TaskId};
use common::protocol::{DataSink, DataSource, ExecuteTask, ExecutionMode, MicroProgram};

/// The smallest schedulable unit of a physical plan.
///
/// A [`Task`] describes one self-contained slice of work — a [`DataSource`],
/// a [`MicroProgram`], and a [`DataSink`] — that a single executor runs
/// to completion. It maps directly onto the wire [`ExecuteTask`] the master
/// dispatches.
#[derive(Debug, Clone)]
pub struct Task {
    /// The unique identifier of this task.
    pub task_id: TaskId,
    /// The shuffle partition this task produces or consumes.
    pub partition: PartitionId,
    /// The data source this task reads from.
    pub source: DataSource,
    /// The micro-IR bytecode program executed by the Vector VM.
    pub program: MicroProgram,
    /// Where the task's output is written.
    pub sink: DataSink,
}

impl Task {
    /// Create a new task.
    pub fn new(
        task_id: TaskId,
        partition: PartitionId,
        source: DataSource,
        program: MicroProgram,
        sink: DataSink,
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
        query_id: QueryId,
        stage_id: StageId,
        mode: ExecutionMode,
    ) -> ExecuteTask {
        ExecuteTask {
            mode,
            task_id: self.task_id,
            attempt: 0,
            query_id,
            stage_id,
            source: self.source,
            program: self.program,
            sink: self.sink,
        }
    }
}
