//! The executor node: a ready-to-run bundle of the worker pool, the QUIC
//! control plane, and the task executor that runs received tasks.
//!
//! Use [`ExecutorNodeBuilder`] to assemble a node, then [`ExecutorNode::start`]
//! to run it until shutdown.

use crate::daemon::{DaemonContext, ExecutorDaemon, WorkerPool};
use crate::factory::TaskExecutorBuilder;
use crate::runtime::TaskExecutor;
use common::identifiers::NodeId;
use common::Result;
use common::transport::build_quic_client;
use error::VeinError;
use std::net::SocketAddr;
use std::sync::Arc;

/// Configuration for an [`ExecutorNode`].
pub struct ExecutorNodeConfig {
    /// The address the control-plane QUIC listener binds to.
    pub bind_addr: SocketAddr,
    /// Path to the TLS certificate.
    pub cert_path: String,
    /// Path to the TLS private key.
    pub key_path: String,
    /// Address of the master node (for future heartbeats/completion reports).
    pub master_addr: String,
    /// Unique identity of this node.
    pub node_id: NodeId,
    /// Number of worker threads.
    pub workers: usize,
}

/// A fully-assembled executor node: a worker pool that executes received tasks
/// and a QUIC control plane that feeds it.
pub struct ExecutorNode {
    config: ExecutorNodeConfig,
    _workers: WorkerPool,
    daemon: ExecutorDaemon,
}

impl ExecutorNode {
    /// Run the node: start the control-plane listener and wait for Ctrl-C.
    pub async fn start(self) -> Result<()> {
        tracing::info!(
            "Starting Vein Executor node on {} (Master: {})",
            self.config.bind_addr,
            self.config.master_addr
        );

        let ExecutorNode {
            config,
            _workers,
            daemon,
        } = self;
        let bind = config.bind_addr;
        let cert = config.cert_path;
        let key = config.key_path;

        tokio::spawn(async move {
            tracing::info!("Initializing Control Plane on {}", bind);
            if let Err(e) = daemon.run_control_plane(bind, &cert, &key).await {
                tracing::error!("Executor control plane failed: {}", e);
            }
        });

        tokio::signal::ctrl_c().await?;
        tracing::info!("Shutting down Executor node");
        Ok(())
    }

    /// Start a builder for an [`ExecutorNode`].
    pub fn builder() -> ExecutorNodeBuilder {
        ExecutorNodeBuilder::new()
    }
}

/// Fluent builder for an [`ExecutorNode`].
#[derive(Default)]
pub struct ExecutorNodeBuilder {
    bind_addr: Option<SocketAddr>,
    cert_path: Option<String>,
    key_path: Option<String>,
    master_addr: Option<String>,
    node_id: NodeId,
    workers: usize,
}

impl ExecutorNodeBuilder {
    /// Create a builder with sane defaults (4 workers, random node id).
    pub fn new() -> Self {
        Self {
            bind_addr: None,
            cert_path: None,
            key_path: None,
            master_addr: None,
            node_id: NodeId::new(),
            workers: 4,
        }
    }

    /// The address the control-plane listener binds to.
    pub fn bind(mut self, addr: SocketAddr) -> Self {
        self.bind_addr = Some(addr);
        self
    }

    /// The TLS certificate and key paths.
    pub fn certificates(mut self, cert: &str, key: &str) -> Self {
        self.cert_path = Some(cert.to_string());
        self.key_path = Some(key.to_string());
        self
    }

    /// The address of the master node.
    pub fn master(mut self, addr: &str) -> Self {
        self.master_addr = Some(addr.to_string());
        self
    }

    /// The unique identity of this node.
    pub fn node_id(mut self, id: NodeId) -> Self {
        self.node_id = id;
        self
    }

    /// The number of worker threads.
    pub fn workers(mut self, n: usize) -> Self {
        self.workers = n;
        self
    }

    /// Assemble the node, wiring the task queue, executor abstraction, worker
    /// pool, and control plane together.
    pub fn build(self) -> Result<ExecutorNode> {
        let config = ExecutorNodeConfig {
            bind_addr: self
                .bind_addr
                .ok_or_else(|| VeinError::internal("bind address is required"))?,
            cert_path: self
                .cert_path
                .ok_or_else(|| VeinError::internal("certificate path is required"))?,
            key_path: self
                .key_path
                .ok_or_else(|| VeinError::internal("key path is required"))?,
            master_addr: self
                .master_addr
                .ok_or_else(|| VeinError::internal("master address is required"))?,
            node_id: self.node_id,
            workers: self.workers,
        };
        if config.workers == 0 {
            return Err(VeinError::internal("at least one worker is required"));
        }

        // Task queue: the control plane produces, the workers consume.
        let (tx, rx) = flume::unbounded();
        let master_client = Arc::new(build_quic_client(&config.cert_path)?);
        let ctx = DaemonContext::new(config.node_id, tx, master_client);

        // The task executor (default stages + expressions) that runs tasks.
        let executor: Arc<dyn TaskExecutor> = Arc::new(TaskExecutorBuilder::new().build());

        let _workers = WorkerPool::start(config.workers, rx, executor);
        let daemon = ExecutorDaemon::new(ctx);

        Ok(ExecutorNode {
            config,
            _workers,
            daemon,
        })
    }
}