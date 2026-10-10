//! The atomic unit of work executed by the local engine.

use common::identifiers::{PartitionId, QueryId, RequestId, StageId, TaskId};
use common::protocol::{
    DataSink, ExecuteTask, ExecutionMode, PhysicalPlan, PhysicalPlanNode, PhysicalPlanOperator,
};
use std::sync::Arc;

/// The smallest executable unit of a physical plan.
///
/// A [`Task`] describes one schedulable slice of work and its canonical physical
/// plan. The local runtime adds query lineage when converting it to an
/// [`ExecuteTask`].
#[derive(Debug, Clone)]
pub struct Task {
    /// The unique identifier of this task.
    pub task_id: TaskId,
    /// Stable partition identity for this input task.
    pub partition: PartitionId,
    /// Immutable physical operator tree executed by this task.
    pub plan: PhysicalPlan,
}

impl Task {
    /// Create a task for an already lowered canonical physical plan.
    pub fn new(task_id: TaskId, partition: PartitionId, plan: PhysicalPlan) -> Self {
        Self { task_id, partition, plan }
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
        let plan = plan_with_partition_sink(self.plan, self.partition);

        ExecuteTask {
            request_id,
            mode,
            task_id: self.task_id,
            attempt: 0,
            query_id,
            stage_id,
            plan,
        }
    }
}

fn plan_with_partition_sink(plan: PhysicalPlan, partition: PartitionId) -> PhysicalPlan {
    let PhysicalPlanOperator::Sink { input, sink } = &plan.root.operator else {
        return plan;
    };
    let sink = match sink.as_ref() {
        DataSink::Storage(target) => {
            let mut target = target.clone();
            target.set_option("partition_id", partition.into_raw().to_string());
            Arc::new(DataSink::Storage(target))
        }
        _ => Arc::clone(sink),
    };
    PhysicalPlan {
        root: Arc::new(PhysicalPlanNode {
            output_schema: Arc::clone(&plan.root.output_schema),
            operator: PhysicalPlanOperator::Sink {
                input: Arc::clone(input),
                sink,
            },
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{plan_with_partition_sink, Task};
    use common::identifiers::{PartitionId, QueryId, RequestId, StageId, TaskId};
    use common::protocol::{
        DataSink, DataSource, ExecutionMode, MicroProgram, PhysicalPlan, PhysicalPlanNode,
        PhysicalPlanOperator, SortKey, SortPlan,
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
        let schema = Arc::new(VeinSchema::empty());
        let input = Arc::new(PhysicalPlanNode {
            output_schema: Arc::clone(&schema),
            operator: PhysicalPlanOperator::Pipeline {
                source: Arc::new(DataSource::Memory),
                input_schema: Arc::clone(&schema),
                program: Arc::new(MicroProgram::empty()),
            },
        });
        let plan = PhysicalPlan {
            root: Arc::new(PhysicalPlanNode {
                output_schema: Arc::clone(&schema),
                operator: PhysicalPlanOperator::Sink {
                    input: Arc::new(PhysicalPlanNode {
                        output_schema: Arc::clone(&schema),
                        operator: PhysicalPlanOperator::Sort {
                            input,
                            plan: sort.clone(),
                            key_program: None,
                            output_columns: 0,
                        },
                    }),
                    sink: Arc::new(DataSink::Client { schema }),
                },
            }),
        };
        let task = Task::new(TaskId::new(), PartitionId::from_raw(0), plan);

        let execute_task = task.into_execute_task(
            RequestId::new(),
            QueryId::new(),
            StageId::new(),
            ExecutionMode::Batch,
        );

        let PhysicalPlanOperator::Sink { input, .. } = &execute_task.plan.root.operator else {
            panic!("task plan should have a sink root");
        };
        let PhysicalPlanOperator::Sort { plan: actual, .. } = &input.operator else {
            panic!("sort should remain an operator in the canonical plan");
        };
        assert_eq!(actual, &sort);
    }

    #[test]
    fn partition_sink_metadata_update_shares_the_operator_subtree() {
        let schema = Arc::new(VeinSchema::empty());
        let input = Arc::new(PhysicalPlanNode {
            output_schema: Arc::clone(&schema),
            operator: PhysicalPlanOperator::Pipeline {
                source: Arc::new(DataSource::Memory),
                input_schema: Arc::clone(&schema),
                program: Arc::new(MicroProgram::empty()),
            },
        });
        let plan = PhysicalPlan {
            root: Arc::new(PhysicalPlanNode {
                output_schema: Arc::clone(&schema),
                operator: PhysicalPlanOperator::Sink {
                    input: Arc::clone(&input),
                    sink: Arc::new(DataSink::Storage(common::protocol::StorageTarget::new(
                        "out.parquet",
                        Vec::new(),
                        schema,
                    ))),
                },
            }),
        };

        let dispatched = plan_with_partition_sink(plan, PartitionId::from_raw(7));
        let PhysicalPlanOperator::Sink { input: dispatched_input, sink } =
            &dispatched.root.operator
        else {
            panic!("task plan should have a sink root");
        };
        assert!(Arc::ptr_eq(&input, dispatched_input));
        let DataSink::Storage(target) = sink.as_ref() else {
            panic!("storage sink should remain a storage sink");
        };
        assert_eq!(target.get_option("partition_id"), Some("7"));
    }
}
