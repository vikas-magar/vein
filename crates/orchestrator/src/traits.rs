use async_trait::async_trait;

use crate::executor::NodeInfo;
use distributed::TableWritePart;

/// Metrics returned after a task attempt completes successfully.
#[derive(Debug, Clone, Default)]
pub struct TaskCompletion {
    /// Rows produced or persisted by the task sink.
    pub row_count: u64,
    /// Bytes read by the task source.
    pub bytes_read: u64,
    /// Task wall-clock duration in nanoseconds.
    pub duration_ns: u64,
    /// Immutable physical parts published by an executor-side storage sink.
    pub write_parts: Vec<TableWritePart>,
}
use common::SendableResultStream;
use common::identifiers::{QueryId, TaskId};
use common::protocol::ExecuteTask;
use error::VeinError;

/// Highly abstract Task Dispatcher.
/// Allows swapping our QUIC network layer with TCP, gRPC, or an in-memory mock for local testing.
#[async_trait]
pub trait TaskDispatcher: Send + Sync {
    /// Dispatch a task to the specified executor over QUIC/gRPC.
    async fn dispatch(&self, executor: &NodeInfo, task: ExecuteTask) -> Result<(), VeinError>;

    /// Block until the task completes (Wait for TaskCompleted or TaskFailed event).
    async fn wait_for_completion(
        &self,
        query_id: QueryId,
        task_id: TaskId,
    ) -> Result<TaskCompletion, VeinError>;

    /// Cancel a running task on the executor.
    async fn cancel_task(
        &self,
        executor: &NodeInfo,
        query_id: QueryId,
        task_id: TaskId,
    ) -> std::result::Result<(), VeinError>;

    /// Open a bounded result stream before dispatching a query's tasks.
    fn open_result_stream(&self, query_id: QueryId) -> SendableResultStream;

    /// Close the result stream after all query tasks have completed.
    fn close_result_stream(&self, query_id: QueryId);
}
