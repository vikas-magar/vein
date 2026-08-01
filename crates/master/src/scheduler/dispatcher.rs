use async_trait::async_trait;
use common::identifiers::QueryId;
use common::protocol::ExecuteTask;
use error::VeinError;

use crate::executor::NodeInfo;

/// Abstracts the network layer for dispatching tasks to executors.
/// This allows us to unit test the scheduler by mocking the network.
#[async_trait]
pub trait TaskDispatcher: Send + Sync {
    /// Dispatch a task to the specified executor over QUIC/gRPC.
    /// Returns successfully when the executor ACKs receipt, or fails if the network drops.
    async fn dispatch(&self, executor: &NodeInfo, task: ExecuteTask) -> Result<(), VeinError>;

    /// Block until the task completes (Wait for TaskCompleted or TaskFailed event).
    /// In a real system, this listens for a datagram on a completion channel.
    async fn wait_for_completion(
        &self,
        query_id: QueryId,
        task_id: common::identifiers::TaskId,
    ) -> Result<(), VeinError>;

    /// Cancel a running task on the executor.
    async fn cancel_task(
        &self,
        executor: &NodeInfo,
        query_id: QueryId,
        task_id: common::identifiers::TaskId,
    ) -> Result<(), VeinError>;
}
