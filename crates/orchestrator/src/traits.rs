use crate::planner::dag::StageDag;
use async_trait::async_trait;
use common::Result;
use planner::plan::LogicalPlan;

/// Abstraction for the Physical Planner.
/// Responsible for lowering a bound Logical Plan into a distributed physical Execution DAG.
pub trait PhysicalPlanner: Send + Sync {
    /// Takes a Logical Plan and generates a distributed StageDag of MorselTasks.
    fn create_physical_plan(&self, logical_plan: LogicalPlan) -> Result<StageDag>;
}

use crate::executor::NodeInfo;
use common::identifiers::{QueryId, TaskId};
use common::protocol::ExecuteTask;
use error::VeinError;

/// Highly abstract Task Dispatcher.
/// Allows swapping our QUIC network layer with TCP, gRPC, or an in-memory mock for local testing.
#[async_trait]
pub trait TaskDispatcher: Send + Sync {
    /// Dispatch a task to the specified executor over QUIC/gRPC.
    async fn dispatch(
        &self,
        executor: &NodeInfo,
        task: ExecuteTask,
    ) -> std::result::Result<(), VeinError>;

    /// Block until the task completes (Wait for TaskCompleted or TaskFailed event).
    async fn wait_for_completion(
        &self,
        query_id: QueryId,
        task_id: TaskId,
    ) -> std::result::Result<(), VeinError>;

    /// Cancel a running task on the executor.
    async fn cancel_task(
        &self,
        executor: &NodeInfo,
        query_id: QueryId,
        task_id: TaskId,
    ) -> std::result::Result<(), VeinError>;
}
