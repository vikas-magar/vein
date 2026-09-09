//! The QUIC network listener and daemon lifecycle for the Executor.
//!
//! Provides [`ExecutorDaemon`], [`ExecutorDaemonBuilder`], and configuration
//! for running an executor worker node.

use std::net::SocketAddr;
use std::sync::Arc;

use tracing::{error, info};

use crate::daemon::{DaemonContext, WorkerPool};
use crate::factory::TaskExecutorBuilder;
use crate::runtime::TaskExecutor;
use common::identifiers::{NodeId, QueryId, TaskId};
use common::protocol::{ArchivedVeinControlEvent, ExecuteTask};
use common::transport::{build_quic_client, build_quic_server};
use common::Result;
use error::VeinError;

/// Configuration for an [`ExecutorDaemon`].
#[derive(Debug, Clone)]
pub struct ExecutorDaemonConfig {
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

/// The Executor Daemon: manages worker pool execution and QUIC network listeners.
pub struct ExecutorDaemon {
    config: Option<ExecutorDaemonConfig>,
    ctx: Arc<DaemonContext>,
    _workers: Option<WorkerPool>,
}

impl ExecutorDaemon {
    /// Creates a new network listener for the given context.
    pub fn new(ctx: Arc<DaemonContext>) -> Self {
        Self {
            config: None,
            ctx,
            _workers: None,
        }
    }

    /// Start a fluent builder for an [`ExecutorDaemon`].
    pub fn builder() -> ExecutorDaemonBuilder {
        ExecutorDaemonBuilder::new()
    }

    /// Run the daemon: start the control-plane listener and wait for Ctrl-C.
    pub async fn start(self) -> Result<()> {
        let config = self.config.as_ref().ok_or_else(|| {
            VeinError::internal("ExecutorDaemon was constructed without a full ExecutorDaemonConfig; use ExecutorDaemon::builder() instead")
        })?;

        info!(
            "Starting Vein Executor daemon on {} (Master: {})",
            config.bind_addr, config.master_addr
        );

        let bind = config.bind_addr;
        let cert = config.cert_path.clone();
        let key = config.key_path.clone();
        let ctx = self.ctx.clone();

        tokio::spawn(async move {
            info!("Initializing Control Plane on {}", bind);
            let daemon = Self::new(ctx);
            if let Err(e) = daemon.run_control_plane(bind, &cert, &key).await {
                error!("Executor control plane failed: {}", e);
            }
        });

        tokio::signal::ctrl_c().await?;
        info!("Shutting down Executor daemon");
        Ok(())
    }

    /// Run the Control Plane (Port 8080).
    /// Listens for `ExecuteTask` and `CancelQuery` payloads via QUIC streams.
    pub async fn run_control_plane(
        &self,
        bind_addr: SocketAddr,
        cert_path: &str,
        key_path: &str,
    ) -> Result<()> {
        info!("Starting Control Plane QUIC server on {}", bind_addr);

        let mut server = build_quic_server(bind_addr, cert_path, key_path)?;

        while let Some(mut connection) = server.accept().await {
            let ctx = self.ctx.clone();

            // Handle multiplexed control streams
            tokio::spawn(async move {
                while let Ok(Some(mut stream)) = connection.accept_bidirectional_stream().await {
                    let ctx = ctx.clone();
                    tokio::spawn(async move {
                        let mut buffer = rkyv::util::AlignedVec::<16>::new();
                        while let Ok(Some(chunk)) = stream.receive().await {
                            buffer.extend_from_slice(&chunk);
                        }

                        if buffer.is_empty() {
                            return;
                        }

                        let archived = match rkyv::access::<
                            ArchivedVeinControlEvent,
                            rkyv::rancor::Error,
                        >(&buffer)
                        {
                            Ok(e) => e,
                            Err(e) => {
                                error!("Failed to validate control event bytes: {}", e);
                                return;
                            }
                        };

                        match archived {
                            ArchivedVeinControlEvent::DispatchTask(archived_task) => {
                                let task = ExecuteTask::from(archived_task);
                                info!(
                                    "Received DispatchTask for Task {:?} (Query {:?})",
                                    task.task_id, task.query_id
                                );

                                if let Err(e) = ctx.task_queue.send_async(task).await {
                                    error!("Failed to push task to worker queue: {}", e);
                                }
                            }
                            ArchivedVeinControlEvent::CancelTask { task_id, query_id } => {
                                let task_id = TaskId::from_raw(task_id.0.to_native());
                                let query_id = QueryId::from_raw(query_id.0.to_native());
                                info!(
                                    "Received CancelTask for Task {:?} (Query {:?})",
                                    task_id, query_id
                                );
                                ctx.cancel_task(task_id);
                            }
                            ArchivedVeinControlEvent::Heartbeat {
                                node_id,
                                timestamp_ms,
                                ..
                            } => {
                                let node_id = NodeId::from_raw(node_id.0.to_native());
                                info!(
                                    "Received Heartbeat from Node {:?} at {}",
                                    node_id,
                                    timestamp_ms.to_native()
                                );
                            }
                            _ => {
                                info!("Received other VeinControlEvent");
                            }
                        }
                    });
                }
            });
        }
        Ok(())
    }

    /// Run the Data Plane (Port 8081).
    /// Listens for massive Arrow IPC exchange streams.
    pub async fn run_data_plane(
        &self,
        bind_addr: SocketAddr,
        cert_path: &str,
        key_path: &str,
    ) -> Result<()> {
        info!("Starting Data Plane QUIC server on {}", bind_addr);

        let mut server = build_quic_server(bind_addr, cert_path, key_path)?;

        while let Some(mut connection) = server.accept().await {
            // Hand off the entire connection straight to the arena allocators / LocalSets
            tokio::spawn(async move {
                while let Ok(Some(stream)) = connection.accept_receive_stream().await {
                    // TODO: parse Arrow IPC bytes and push directly to operators
                    let _ = stream;
                }
            });
        }
        Ok(())
    }
}

/// Fluent builder for an [`ExecutorDaemon`].
#[derive(Default)]
pub struct ExecutorDaemonBuilder {
    bind_addr: Option<SocketAddr>,
    cert_path: Option<String>,
    key_path: Option<String>,
    master_addr: Option<String>,
    node_id: NodeId,
    workers: usize,
}

impl ExecutorDaemonBuilder {
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

    /// Assemble the daemon, wiring the task queue, executor abstraction, worker
    /// pool, and control plane together.
    pub fn build(self) -> Result<ExecutorDaemon> {
        let config = ExecutorDaemonConfig {
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
        let workers = WorkerPool::start(config.workers, rx, executor);

        Ok(ExecutorDaemon {
            config: Some(config),
            ctx,
            _workers: Some(workers),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::identifiers::{NodeId, QueryId, StageId, TaskId};
    use common::protocol::{
        DataSink, DataSource, ExecuteTask, ExecutionMode, FileFormat, FileScanTask, MicroOp,
        MicroProgram, NodeKind, NodeLoad, ScalarConstant, VeinControlEvent,
    };
    use rkyv::rancor::Error;

    #[test]
    fn test_zero_copy_dispatch_task() {
        let task = ExecuteTask {
            mode: ExecutionMode::Batch,
            task_id: TaskId::new(),
            query_id: QueryId::new(),
            stage_id: StageId::new(),
            source: DataSource::File(FileScanTask {
                files: vec!["test.parquet".to_string()],
                format: FileFormat::Parquet,
                byte_range: None,
                column_ids: vec![0, 1],
                projected_schema: vec![],
                task_id: TaskId::new(),
                shuffle_target_path: None,
            }),
            program: MicroProgram {
                register_count: 3,
                instructions: vec![
                    MicroOp::LoadConstant {
                        dst: 1,
                        value: ScalarConstant::Int64(100),
                    },
                    MicroOp::CmpGt {
                        dst: 2,
                        src1: 0,
                        src2: 1,
                    },
                    MicroOp::FilterMask { bool_reg: 2 },
                ],
            },
            sink: DataSink::Discard,
        };

        let event = VeinControlEvent::DispatchTask(task.clone());
        let bytes = rkyv::to_bytes::<Error>(&event).expect("serialization succeeds");

        let mut aligned = rkyv::util::AlignedVec::<16>::new();
        aligned.extend_from_slice(&bytes);

        let archived = rkyv::access::<ArchivedVeinControlEvent, Error>(&aligned)
            .expect("in-place access succeeds");

        match archived {
            ArchivedVeinControlEvent::DispatchTask(archived_task) => {
                let roundtrip = ExecuteTask::from(archived_task);
                assert_eq!(roundtrip.task_id, task.task_id);
                assert_eq!(roundtrip.query_id, task.query_id);
                assert_eq!(roundtrip.stage_id, task.stage_id);
                assert_eq!(roundtrip.mode, task.mode);
                match (&roundtrip.source, &task.source) {
                    (DataSource::File(f1), DataSource::File(f2)) => {
                        assert_eq!(f1.files, f2.files);
                        assert_eq!(f1.column_ids, f2.column_ids);
                    }
                    _ => panic!("source mismatch"),
                }
                assert_eq!(roundtrip.program.instructions.len(), 3);
            }
            _ => panic!("expected DispatchTask"),
        }
    }

    #[test]
    fn test_zero_copy_control_in_place() {
        let task_id = TaskId::new();
        let query_id = QueryId::new();
        let event = VeinControlEvent::CancelTask { task_id, query_id };
        let bytes = rkyv::to_bytes::<Error>(&event).expect("serialization succeeds");

        let mut aligned = rkyv::util::AlignedVec::<16>::new();
        aligned.extend_from_slice(&bytes);

        let archived = rkyv::access::<ArchivedVeinControlEvent, Error>(&aligned)
            .expect("in-place access succeeds");

        match archived {
            ArchivedVeinControlEvent::CancelTask {
                task_id: archived_tid,
                query_id: archived_qid,
            } => {
                let parsed_tid = TaskId::from_raw(archived_tid.0.to_native());
                let parsed_qid = QueryId::from_raw(archived_qid.0.to_native());
                assert_eq!(parsed_tid, task_id);
                assert_eq!(parsed_qid, query_id);
            }
            _ => panic!("expected CancelTask"),
        }

        let node_id = NodeId::new();
        let hb = VeinControlEvent::Heartbeat {
            node_id,
            node_kind: NodeKind::Executor,
            timestamp_ms: 123456789,
            load: NodeLoad {
                active_tasks: 2,
                cpu_percent: 12.5,
                memory_bytes: 1024,
            },
        };
        let hb_bytes = rkyv::to_bytes::<Error>(&hb).expect("serialization succeeds");
        let mut hb_aligned = rkyv::util::AlignedVec::<16>::new();
        hb_aligned.extend_from_slice(&hb_bytes);

        let archived_hb = rkyv::access::<ArchivedVeinControlEvent, Error>(&hb_aligned)
            .expect("in-place access succeeds");

        match archived_hb {
            ArchivedVeinControlEvent::Heartbeat {
                node_id: archived_nid,
                timestamp_ms,
                ..
            } => {
                assert_eq!(NodeId::from_raw(archived_nid.0.to_native()), node_id);
                assert_eq!(timestamp_ms.to_native(), 123456789);
            }
            _ => panic!("expected Heartbeat"),
        }
    }
}

