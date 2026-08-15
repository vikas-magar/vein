use crate::executor::NodeInfo;
use crate::traits::TaskDispatcher;
use async_trait::async_trait;
use common::identifiers::{QueryId, TaskId};
use common::protocol::ExecuteTask;
use error::VeinError;

/// The QUIC Task Dispatcher.
/// Responsible for serializing `ExecuteTask`s and streaming them to the appropriate
/// Executor node via a highly concurrent, multiplexed s2n-quic stream.
pub struct QuicTaskDispatcher;

impl QuicTaskDispatcher {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl TaskDispatcher for QuicTaskDispatcher {
    async fn dispatch(
        &self,
        _executor: &NodeInfo,
        _task: ExecuteTask,
    ) -> std::result::Result<(), VeinError> {
        // TODO: Actually open a QUIC stream to the Executor node and serialize the ExecuteTask
        Err(VeinError::internal(
            "QuicTaskDispatcher::dispatch is not yet implemented",
        ))
    }

    async fn wait_for_completion(
        &self,
        _query_id: QueryId,
        _task_id: TaskId,
    ) -> std::result::Result<(), VeinError> {
        Err(VeinError::internal("wait_for_completion not implemented"))
    }

    async fn cancel_task(
        &self,
        _executor: &NodeInfo,
        _query_id: QueryId,
        _task_id: TaskId,
    ) -> std::result::Result<(), VeinError> {
        Err(VeinError::internal("cancel_task not implemented"))
    }
}
