//! The atomic unit of work executed by the local engine.

use common::identifiers::{PartitionId, QueryId, RequestId, StageId, TaskId};
use common::protocol::{
    AggregatePlan, DataSink, DataSource, ExecuteTask, ExecutionMode, JoinPlan, LimitSpec,
    MicroProgram, PhysicalInputPlan, SortPlan,
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
    /// Global row ordering, applied before the task's output window.
    pub sort: Option<SortPlan>,
    /// VM program that appends computed sort keys after the main row program.
    pub sort_key_program: Option<Arc<MicroProgram>>,
    /// Number of user-visible columns before helper sort keys are appended.
    pub sort_output_columns: Option<u32>,
    /// How this task aggregates, when the query has a `GROUP BY` or `DISTINCT`.
    pub aggregate: Option<AggregatePlan>,
    /// VM program applied to aggregate output, when the plan has post-aggregate filters.
    pub post_aggregate_program: Option<Arc<MicroProgram>>,
    /// Recursive source/operator tree for chained joins.
    pub input_plan: Option<Arc<PhysicalInputPlan>>,
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
            sort: None,
            sort_key_program: None,
            sort_output_columns: None,
            aggregate: None,
            post_aggregate_program: None,
            input_plan: None,
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
            sort: None,
            sort_key_program: None,
            sort_output_columns: None,
            aggregate: None,
            post_aggregate_program: None,
            input_plan: None,
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

    /// Orders the complete input before applying its output window.
    pub fn with_sort(mut self, sort: Option<SortPlan>) -> Self {
        self.sort = sort;
        self
    }

    /// Appends computed sort keys and records the visible output width.
    pub fn with_sort_key_program(
        mut self,
        program: Option<Arc<MicroProgram>>,
        output_columns: Option<u32>,
    ) -> Self {
        self.sort_key_program = program;
        self.sort_output_columns = output_columns;
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

    /// Applies a separately compiled VM program after this task's aggregate.
    pub fn with_post_aggregate_program(mut self, program: Option<Arc<MicroProgram>>) -> Self {
        self.post_aggregate_program = program;
        self
    }

    /// Uses a recursive physical input tree for chained joins.
    pub fn with_input_plan(mut self, input_plan: Arc<PhysicalInputPlan>) -> Self {
        self.input_plan = Some(input_plan);
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
            input_plan: self.input_plan,
            source: self.source,
            program: self.program,
            sink,
            build: self.build,
            join: self.join,
            limit: self.limit,
            sort: self.sort,
            sort_key_program: self.sort_key_program,
            sort_output_columns: self.sort_output_columns,
            aggregate: self.aggregate,
            post_aggregate_program: self.post_aggregate_program,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Task;
    use common::identifiers::{PartitionId, QueryId, RequestId, StageId, TaskId};
    use common::protocol::{
        DataSink, DataSource, ExecutionMode, MicroProgram, SortKey, SortPlan,
    };
    use plan::schema::VeinSchema;
    use std::sync::Arc;

    #[test]
    fn carries_sort_plan_into_execute_task() {
        let sort = SortPlan {
            keys: vec![SortKey {
                column: 2,
                ascending: false,
                nulls_first: true,
            }],
        };
        let task = Task::new(
            TaskId::new(),
            PartitionId::from_raw(0),
            DataSource::Memory,
            Arc::new(MicroProgram::empty()),
            Arc::new(DataSink::Client {
                schema: Arc::new(VeinSchema::empty()),
            }),
        )
            .with_sort(Some(sort.clone()));

        let execute_task = task.into_execute_task(
            RequestId::new(),
            QueryId::new(),
            StageId::new(),
            ExecutionMode::Batch,
        );

        assert_eq!(execute_task.sort, Some(sort));
    }
}
