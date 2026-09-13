use common::cancellation::CancellationToken;
use common::identifiers::{NodeId, TaskId};
use common::protocol::ExecuteTask;
use crate::runtime::TaskResult;
use dashmap::DashMap;
use s2n_quic::Client;
use std::sync::Arc;
use tokio::sync::oneshot;

/// A task dispatched to the worker pool along with its cancellation token
/// and completion channel.
pub struct QueuedTask {
    /// The physical task specification.
    pub task: ExecuteTask,
    /// Cancellation token that can interrupt this specific task.
    pub cancel: CancellationToken,
    /// Channel to return the execution result back to the dispatch handler.
    pub responder: oneshot::Sender<common::Result<TaskResult>>,
}

/// Shared state for the Executor Daemon.
pub struct DaemonContext {
    /// Unique identity of this node.
    pub node_id: NodeId,
    /// Active tasks that can be cancelled in O(1) time.
    pub active_tasks: DashMap<TaskId, CancellationToken>,
    /// Global queue for unassigned tasks (Work-Stealing).
    pub task_queue: flume::Sender<QueuedTask>,
    /// Outbound QUIC client to talk to the Master.
    pub master_client: Arc<Client>,
}

impl DaemonContext {
    /// Create a new DaemonContext.
    pub fn new(
        node_id: NodeId,
        task_queue: flume::Sender<QueuedTask>,
        master_client: Arc<Client>,
    ) -> Arc<Self> {
        Arc::new(Self {
            node_id,
            active_tasks: DashMap::new(),
            task_queue,
            master_client,
        })
    }

    /// Register a new task.
    pub fn register_task(&self, task_id: TaskId, token: CancellationToken) {
        self.active_tasks.insert(task_id, token);
    }

    /// Cancel a running task.
    pub fn cancel_task(&self, task_id: TaskId) {
        if let Some((_, token)) = self.active_tasks.remove(&task_id) {
            token.cancel();
        }
    }
}
