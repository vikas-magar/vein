use async_trait::async_trait;

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
    ) -> Result<(), VeinError>;

    /// Block until the task completes (Wait for TaskCompleted or TaskFailed event).
    async fn wait_for_completion(
        &self,
        query_id: QueryId,
        task_id: TaskId,
    ) -> Result<(), VeinError>;

    /// Cancel a running task on the executor.
    async fn cancel_task(
        &self,
        executor: &NodeInfo,
        query_id: QueryId,
        task_id: TaskId,
    ) -> std::result::Result<(), VeinError>;

    /// Retrieve the collected output bytes for a query (from DataSink::Client tasks).
    fn take_query_output(&self, query_id: QueryId) -> Option<bytes::Bytes>;
}
