//! The atomic unit of work executed by the local engine.

use common::identifiers::{PartitionId, QueryId, RequestId, StageId, TaskId};
use common::protocol::{
    AggregatePlan, DataSink, DataSource, ExecuteTask, ExecutionMode, JoinPlan, LimitSpec,
    MicroProgram,
};
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
    /// The build side of a join, when this task joins.
    pub build: Option<DataSource>,
    /// How `source` and `build` are joined, when they are.
    pub join: Option<JoinPlan>,
    /// The row window this task emits, when the query has a `LIMIT`.
    pub limit: Option<LimitSpec>,
    /// How this task aggregates, when the query has a `GROUP BY` or `DISTINCT`.
    pub aggregate: Option<AggregatePlan>,
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
            build: None,
            join: None,
            limit: None,
            aggregate: None,
        }
    }

    /// A task that joins `build` against `source`.
    ///
    /// The two halves travel in one task because the executor must sequence
    /// them: the build side is read to completion, and only then is the probe
    /// side streamed against the table it produced.
    pub fn join(
        task_id: TaskId,
        partition: PartitionId,
        source: DataSource,
        build: DataSource,
        join: JoinPlan,
        program: Arc<MicroProgram>,
        sink: Arc<DataSink>,
    ) -> Self {
        Self {
            task_id,
            partition,
            source,
            program,
            sink,
            build: Some(build),
            join: Some(join),
            limit: None,
            aggregate: None,
        }
    }

    /// The row window this task emits.
    ///
    /// Separate from [`Task::new`] because most tasks have no window, and a
    /// limited plan is deliberately not split across files: a limit counts rows
    /// globally, so a per-file task would emit the limit once per file.
    pub fn with_limit(mut self, limit: Option<LimitSpec>) -> Self {
        self.limit = limit;
        self
    }

    /// Groups this task's rows before they reach the sink.
    ///
    /// Like a limit, an aggregate is a property of the whole task, so a grouped
    /// plan is never split across files: each split would group its own rows and
    /// emit its own groups.
    pub fn with_aggregate(mut self, aggregate: Option<AggregatePlan>) -> Self {
        self.aggregate = aggregate;
        self
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
            build: self.build,
            join: self.join,
            limit: self.limit,
            aggregate: self.aggregate,
        }
    }
}
