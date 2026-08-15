use common::identifiers::NodeId;
use common::protocol::{NodeKind, NodeLoad};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

/// Information about a registered node.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub node_id: NodeId,
    pub addr: SocketAddr,
    pub kind: NodeKind,
    pub load: NodeLoad,
}

/// A node entry with its last seen timestamp.
#[derive(Debug)]
pub struct NodeEntry {
    pub info: NodeInfo,
    pub last_seen: Instant,
}

/// Registry for all active Executor nodes in the cluster.
#[derive(Debug)]
pub struct ExecutorRegistry {
    inner: Arc<RwLock<HashMap<NodeId, NodeEntry>>>,
}

impl Default for ExecutorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutorRegistry {
    /// Create a new empty executor registry.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Record a heartbeat from an executor node.
    pub async fn record_heartbeat(
        &self,
        node_id: NodeId,
        addr: SocketAddr,
        kind: NodeKind,
        load: NodeLoad,
    ) {
        let mut map = self.inner.write().await;
        map.insert(
            node_id,
            NodeEntry {
                info: NodeInfo {
                    node_id,
                    addr,
                    kind,
                    load,
                },
                last_seen: Instant::now(),
            },
        );
    }

    /// Start a background task that evicts nodes whose heartbeats have timed out.
    /// Returns a JoinHandle that can be used to monitor or cancel the task.
    pub fn start_health_check(&self, timeout: Duration) -> JoinHandle<()> {
        let registry = self.inner.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(2));
            loop {
                interval.tick().await;
                let mut map = registry.write().await;
                let now = Instant::now();
                // Retain only nodes that have been seen within the timeout window
                map.retain(|_, entry| now.duration_since(entry.last_seen) < timeout);
            }
        })
    }

    /// Return a list of available nodes, sorted by load (least loaded first).
    /// Filters out any nodes that have already exceeded a safe heartbeat threshold
    /// even if the sweeper hasn't removed them yet.
    pub async fn available(&self, timeout: Duration) -> Vec<NodeInfo> {
        let map = self.inner.read().await;
        let now = Instant::now();
        let mut nodes: Vec<_> = map
            .values()
            .filter(|e| now.duration_since(e.last_seen) < timeout)
            .map(|e| e.info.clone())
            .collect();

        // Sort by load (least loaded first). Simple heuristic based on active tasks.
        nodes.sort_by(|a, b| a.load.active_tasks.cmp(&b.load.active_tasks));
        nodes
    }
}
