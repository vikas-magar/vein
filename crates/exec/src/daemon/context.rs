use std::sync::Arc;
use dashmap::DashMap;
use common::cancellation::CancellationToken;
use common::identifiers::{TaskId, NodeId};
use s2n_quic::Client;
use crate::daemon::pipeline::MorselTask;

/// Shared state for the Executor Daemon.
pub struct DaemonContext {
    /// Unique identity of this node.
    pub node_id: NodeId,
    /// Active tasks that can be cancelled in O(1) time.
    pub active_tasks: DashMap<TaskId, CancellationToken>,
    /// Global queue for unassigned Morsels (Work-Stealing).
    pub morsel_queue: flume::Sender<MorselTask>,
    /// Outbound QUIC client to talk to the Master.
    pub master_client: Arc<Client>,
}

impl DaemonContext {
    /// Create a new DaemonContext.
    pub fn new(
        node_id: NodeId,
        morsel_queue: flume::Sender<MorselTask>,
        master_client: Arc<Client>,
    ) -> Arc<Self> {
        Arc::new(Self {
            node_id,
            active_tasks: DashMap::new(),
            morsel_queue,
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
