//! The QUIC network listener and daemon lifecycle for the Executor.
//!
//! Provides [`ExecutorDaemon`], [`ExecutorDaemonBuilder`], and configuration
//! for running an executor worker node.

use std::net::SocketAddr;
use std::sync::Arc;

use tracing::{error, info};

use crate::daemon::{DaemonContext, QueuedTask, WorkerPool};
use crate::exchange::VcsExchangeMetadata;
use crate::factory::TaskExecutorBuilder;
use crate::runtime::{ArrowIpcOutputEncoder, TaskExecutor};
use common::Result;
use common::cancellation::CancellationToken;
use common::identifiers::{NodeId, QueryId, TaskId};
use common::protocol::{
    ArchivedVeinControlEvent, DataSink, ExecuteTask, NodeKind, NodeLoad, VeinControlEvent,
};
use common::transport::{
    VcsExchangeClient, build_quic_client, build_quic_server, extract_control_payload,
    frame_control_message,
};
use distributed::RaftRpcHandler;
use error::VeinError;
use vcs::RemoteVcsCatalog;

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
    raft_rpc_handler: Option<Arc<dyn RaftRpcHandler>>,
}

impl ExecutorDaemon {
    /// Creates a new network listener for the given context.
    pub fn new(ctx: Arc<DaemonContext>) -> Self {
        Self {
            config: None,
            ctx,
            _workers: None,
            raft_rpc_handler: None,
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
        let raft_rpc_handler = self.raft_rpc_handler.clone();

        tokio::spawn(async move {
            info!("Initializing Control Plane on {}", bind);
            let mut daemon = Self::new(ctx);
            daemon.raft_rpc_handler = raft_rpc_handler;
            if let Err(e) = daemon.run_control_plane(bind, &cert, &key).await {
                error!("Executor control plane failed: {}", e);
            }
        });

        // Spawn periodic heartbeat ticker to Master
        let hb_ctx = self.ctx.clone();
        let master_addr = config.master_addr.clone();
        let node_id = config.node_id;
        let advertise_addr = config.bind_addr.to_string();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(2));
            loop {
                interval.tick().await;
                let target_addr: std::result::Result<SocketAddr, _> = master_addr.parse();
                let target_addr = match target_addr {
                    Ok(addr) => addr,
                    Err(e) => {
                        tracing::debug!("Invalid master address for heartbeat: {e}");
                        continue;
                    }
                };

                let connect =
                    s2n_quic::client::Connect::new(target_addr).with_server_name("localhost");
                let mut conn = match hb_ctx.master_client.connect(connect).await {
                    Ok(conn) => conn,
                    Err(e) => {
                        tracing::trace!("Heartbeat connect to master {target_addr} failed: {e}");
                        continue;
                    }
                };

                let mut stream = match conn.open_bidirectional_stream().await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::trace!(
                            "Heartbeat open stream to master {target_addr} failed: {e}"
                        );
                        continue;
                    }
                };

                let hb = VeinControlEvent::Heartbeat {
                    node_id,
                    node_kind: NodeKind::Executor,
                    timestamp_ms: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64,
                    load: NodeLoad {
                        active_tasks: hb_ctx.active_tasks.len() as u16,
                        cpu_percent: 0.0,
                        memory_bytes: 0,
                    },
                    advertise_addr: advertise_addr.clone(),
                };

                if let Ok(bytes) = rkyv::to_bytes::<rkyv::rancor::Error>(&hb) {
                    let wire = frame_control_message(&bytes);
                    if stream.send(wire).await.is_ok() {
                        let _ = stream.finish();
                        let mut resp = Vec::new();
                        while let Ok(Some(chunk)) = stream.receive().await {
                            resp.extend_from_slice(&chunk);
                        }
                    }
                }
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
            let raft_rpc_handler = self.raft_rpc_handler.clone();

            // Handle multiplexed control streams
            tokio::spawn(async move {
                while let Ok(Some(mut stream)) = connection.accept_bidirectional_stream().await {
                    let ctx = ctx.clone();
                    let raft_rpc_handler = raft_rpc_handler.clone();
                    tokio::spawn(async move {
                        let mut buffer = Vec::new();
                        while let Ok(Some(chunk)) = stream.receive().await {
                            buffer.extend_from_slice(&chunk);
                        }

                        if buffer.is_empty() {
                            return;
                        }

                        let payload = match extract_control_payload(&buffer) {
                            Some(p) => p,
                            None => return,
                        };

                        let mut aligned = rkyv::util::AlignedVec::<16>::new();
                        aligned.extend_from_slice(payload);

                        let archived =
                            match rkyv::access::<ArchivedVeinControlEvent, rkyv::rancor::Error>(
                                &aligned,
                            ) {
                                Ok(e) => e,
                                Err(e) => {
                                    error!("Failed to validate control event bytes: {}", e);
                                    return;
                                }
                            };

                        match archived {
                            ArchivedVeinControlEvent::RaftRpcRequest(_) => {
                                let request = match VeinControlEvent::from(archived) {
                                    VeinControlEvent::RaftRpcRequest(request) => request,
                                    _ => unreachable!("validated Raft request changed variant"),
                                };
                                let response = match raft_rpc_handler.as_ref() {
                                    Some(handler) => handler.handle(request.clone()).await,
                                    None => VeinControlEvent::RaftRpcResponse {
                                        envelope: common::protocol::RaftRpcEnvelope {
                                            request_id: request.request_id,
                                            kind: request.kind,
                                            payload: Vec::new(),
                                        },
                                        error: Some(
                                            "Raft RPC service is not configured".to_string(),
                                        ),
                                    },
                                };
                                match rkyv::to_bytes::<rkyv::rancor::Error>(&response) {
                                    Ok(bytes) => {
                                        if let Err(error) =
                                            stream.send(frame_control_message(&bytes)).await
                                        {
                                            error!("Failed to send Raft RPC response: {}", error);
                                        }
                                    }
                                    Err(error) => {
                                        error!("Failed to serialize Raft RPC response: {}", error);
                                    }
                                }
                                let _ = stream.finish();
                                return;
                            }
                            ArchivedVeinControlEvent::DispatchTask(archived_task) => {
                                let task = ExecuteTask::from(archived_task);
                                let task_id = task.task_id;
                                let query_id = task.query_id;
                                let request_id = task.request_id;
                                tracing::info!(
                                    target: "vein.audit",
                                    event = "rpc_task_received",
                                    %request_id,
                                    %query_id,
                                    %task_id,
                                    attempt = task.attempt,
                                    executor_node = %ctx.node_id,
                                    "rpc_task_received"
                                );

                                let cancel = CancellationToken::new();
                                ctx.register_task(task_id, cancel.clone());
                                let is_client = matches!(&task.sink, DataSink::Client { .. });
                                let attempt = task.attempt;
                                let (output_sender, output_receiver) = if is_client {
                                    let (sender, receiver) = flume::bounded(8);
                                    (Some(sender), Some(receiver))
                                } else {
                                    (None, None)
                                };
                                let (tx, rx) = tokio::sync::oneshot::channel();
                                let queued = QueuedTask {
                                    task: task.clone(),
                                    cancel,
                                    responder: tx,
                                    output_sender,
                                };

                                if let Err(e) = ctx.task_queue.send_async(queued).await {
                                    tracing::error!(
                                        target: "vein.audit",
                                        event = "rpc_task_queue_failed",
                                        %request_id,
                                        %query_id,
                                        %task_id,
                                        attempt,
                                        error = %e,
                                        "rpc_task_queue_failed"
                                    );
                                    ctx.active_tasks.remove(&task_id);
                                    let fail_event = VeinControlEvent::TaskFailed {
                                        task_id,
                                        query_id,
                                        attempt,
                                        error: format!("Worker queue error: {e}"),
                                    };
                                    if let Ok(bytes) =
                                        rkyv::to_bytes::<rkyv::rancor::Error>(&fail_event)
                                    {
                                        let wire = frame_control_message(&bytes);
                                        let _ = stream.send(wire).await;
                                        let _ = stream.finish();
                                    }
                                    return;
                                }

                                tracing::info!(
                                    target: "vein.audit",
                                    event = "rpc_task_queued",
                                    %request_id,
                                    %query_id,
                                    %task_id,
                                    attempt,
                                    client_result = is_client,
                                    "rpc_task_queued"
                                );

                                if is_client {
                                    let start = VeinControlEvent::TaskResultStart {
                                        task_id,
                                        query_id,
                                        attempt,
                                    };
                                    let bytes = match rkyv::to_bytes::<rkyv::rancor::Error>(&start)
                                    {
                                        Ok(bytes) => bytes,
                                        Err(e) => {
                                            error!("Failed to serialize result start: {}", e);
                                            return;
                                        }
                                    };
                                    if let Err(e) = stream.send(frame_control_message(&bytes)).await
                                    {
                                        error!("Failed to stream result start: {}", e);
                                        return;
                                    }

                                    let output_receiver =
                                        output_receiver.expect("client output receiver");
                                    tokio::spawn(async move {
                                        let encoder = ArrowIpcOutputEncoder::new();
                                        let mut sequence = 0u64;
                                        while let Ok(batch) = output_receiver.recv_async().await {
                                            let payload = match encoder.encode_batch(&batch) {
                                                Ok(payload) => payload,
                                                Err(e) => {
                                                    error!(
                                                        "Failed to encode streamed result: {}",
                                                        e
                                                    );
                                                    return;
                                                }
                                            };
                                            let event = VeinControlEvent::TaskResultBatch {
                                                task_id,
                                                query_id,
                                                attempt,
                                                sequence,
                                                result_bytes: payload.to_vec(),
                                            };
                                            let bytes = match rkyv::to_bytes::<rkyv::rancor::Error>(
                                                &event,
                                            ) {
                                                Ok(bytes) => bytes,
                                                Err(e) => {
                                                    error!(
                                                        "Failed to serialize streamed result: {}",
                                                        e
                                                    );
                                                    return;
                                                }
                                            };
                                            if let Err(e) =
                                                stream.send(frame_control_message(&bytes)).await
                                            {
                                                error!("Failed to stream result batch: {}", e);
                                                return;
                                            }
                                            sequence += 1;
                                        }

                                        ctx.active_tasks.remove(&task_id);
                                        match rx.await {
                                            Ok(Ok(res)) => {
                                                let end = VeinControlEvent::TaskResultEnd {
                                                    task_id,
                                                    query_id,
                                                    attempt,
                                                    row_count: res.row_count,
                                                    bytes_read: res.metrics.bytes_read,
                                                    duration_ns: res.metrics.duration_ns,
                                                };
                                                if let Ok(bytes) =
                                                    rkyv::to_bytes::<rkyv::rancor::Error>(&end)
                                                {
                                                    let _ = stream
                                                        .send(frame_control_message(&bytes))
                                                        .await;
                                                }
                                            }
                                            Ok(Err(err)) => {
                                                let event = VeinControlEvent::TaskFailed {
                                                    task_id,
                                                    query_id,
                                                    attempt,
                                                    error: err.to_string(),
                                                };
                                                if let Ok(bytes) =
                                                    rkyv::to_bytes::<rkyv::rancor::Error>(&event)
                                                {
                                                    let _ = stream
                                                        .send(frame_control_message(&bytes))
                                                        .await;
                                                }
                                            }
                                            Err(_) => {}
                                        }
                                        let _ = stream.finish();
                                    });
                                    return;
                                }

                                // Await task execution completion from worker
                                let exec_result = rx.await;
                                ctx.active_tasks.remove(&task_id);

                                match exec_result {
                                    Ok(Ok(res))
                                        if matches!(&task.sink, DataSink::Client { .. }) =>
                                    {
                                        let start = VeinControlEvent::TaskResultStart {
                                            task_id,
                                            query_id,
                                            attempt,
                                        };
                                        let start_bytes =
                                            match rkyv::to_bytes::<rkyv::rancor::Error>(&start) {
                                                Ok(bytes) => bytes,
                                                Err(e) => {
                                                    error!(
                                                        "Failed to serialize result start: {}",
                                                        e
                                                    );
                                                    return;
                                                }
                                            };
                                        if let Err(e) =
                                            stream.send(frame_control_message(&start_bytes)).await
                                        {
                                            error!("Failed to stream result start: {}", e);
                                            return;
                                        }

                                        let encoder = ArrowIpcOutputEncoder::new();
                                        for (sequence, batch) in res.output.iter().enumerate() {
                                            let payload = match encoder.encode_batch(batch) {
                                                Ok(bytes) => bytes,
                                                Err(e) => {
                                                    error!(
                                                        "Failed to encode result batch {}: {}",
                                                        sequence, e
                                                    );
                                                    return;
                                                }
                                            };
                                            let event = VeinControlEvent::TaskResultBatch {
                                                task_id,
                                                query_id,
                                                attempt,
                                                sequence: sequence as u64,
                                                result_bytes: payload.to_vec(),
                                            };
                                            let bytes = match rkyv::to_bytes::<rkyv::rancor::Error>(
                                                &event,
                                            ) {
                                                Ok(bytes) => bytes,
                                                Err(e) => {
                                                    error!(
                                                        "Failed to serialize result batch {}: {}",
                                                        sequence, e
                                                    );
                                                    return;
                                                }
                                            };
                                            if let Err(e) =
                                                stream.send(frame_control_message(&bytes)).await
                                            {
                                                error!(
                                                    "Failed to stream result batch {}: {}",
                                                    sequence, e
                                                );
                                                return;
                                            }
                                        }

                                        let end = VeinControlEvent::TaskResultEnd {
                                            task_id,
                                            query_id,
                                            attempt,
                                            row_count: res.row_count,
                                            bytes_read: res.metrics.bytes_read,
                                            duration_ns: res.metrics.duration_ns,
                                        };
                                        match rkyv::to_bytes::<rkyv::rancor::Error>(&end) {
                                            Ok(bytes) => {
                                                if let Err(e) =
                                                    stream.send(frame_control_message(&bytes)).await
                                                {
                                                    error!("Failed to stream result end: {}", e);
                                                }
                                            }
                                            Err(e) => {
                                                error!("Failed to serialize result end: {}", e)
                                            }
                                        }
                                        let _ = stream.finish();
                                    }
                                    Ok(Ok(res)) => {
                                        let event = VeinControlEvent::TaskCompleted {
                                            task_id,
                                            query_id,
                                            attempt,
                                            row_count: res.row_count,
                                            bytes_read: res.metrics.bytes_read,
                                            duration_ns: res.metrics.duration_ns,
                                            result_bytes: None,
                                             write_parts: res.write_parts.iter().map(|part| common::protocol::TableWritePartDescriptor {
                                                 partition_id: part.partition_id,
                                                 artifact_id: part.artifact.id.to_string(),
                                                 artifact_uri: part.artifact.uri.clone(),
                                                 checksum: part.artifact.checksum.clone(),
                                                 size_bytes: part.artifact.size_bytes,
                                                 row_count: part.row_count,
                                             }).collect(),
                                        };
                                        match rkyv::to_bytes::<rkyv::rancor::Error>(&event) {
                                            Ok(bytes) => {
                                                let _ = stream
                                                    .send(frame_control_message(&bytes))
                                                    .await;
                                                let _ = stream.finish();
                                            }
                                            Err(e) => error!(
                                                "Failed to serialize TaskCompleted event: {}",
                                                e
                                            ),
                                        }
                                    }
                                    Ok(Err(err)) => {
                                        let event = VeinControlEvent::TaskFailed {
                                            task_id,
                                            query_id,
                                            attempt,
                                            error: err.to_string(),
                                        };
                                        if let Ok(bytes) =
                                            rkyv::to_bytes::<rkyv::rancor::Error>(&event)
                                        {
                                            let _ =
                                                stream.send(frame_control_message(&bytes)).await;
                                        }
                                        let _ = stream.finish();
                                    }
                                    Err(_) => {
                                        let event = VeinControlEvent::TaskFailed {
                                            task_id,
                                            query_id,
                                            attempt,
                                            error: "Worker dropped task channel without responding"
                                                .to_string(),
                                        };
                                        if let Ok(bytes) =
                                            rkyv::to_bytes::<rkyv::rancor::Error>(&event)
                                        {
                                            let _ =
                                                stream.send(frame_control_message(&bytes)).await;
                                        }
                                        let _ = stream.finish();
                                    }
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
    vcs_endpoint: Option<String>,
    vcs_ca_cert: Option<String>,
    allow_local_metadata: bool,
    raft_rpc_handler: Option<Arc<dyn RaftRpcHandler>>,
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
            vcs_endpoint: None,
            vcs_ca_cert: None,
            allow_local_metadata: false,
            raft_rpc_handler: None,
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

    /// Configure the VCS exchange metadata endpoint and its CA certificate.
    pub fn vcs_exchange(mut self, endpoint: &str, ca_cert: &str) -> Self {
        self.vcs_endpoint = Some(endpoint.to_string());
        self.vcs_ca_cert = Some(ca_cert.to_string());
        self
    }

    /// Allows an explicitly local development executor without a remote VCS.
    ///
    /// Production executors must use vcs_exchange so exchange and spill
    /// metadata are governed by the shared VCS authority.
    pub fn local_development(mut self) -> Self {
        self.allow_local_metadata = true;
        self
    }

    /// Installs the Raft RPC service used by the control-plane listener.
    pub fn raft_rpc_handler(mut self, handler: Arc<dyn RaftRpcHandler>) -> Self {
        self.raft_rpc_handler = Some(handler);
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

        if self.vcs_endpoint.is_none() && !self.allow_local_metadata {
            return Err(VeinError::internal(
                "remote VCS metadata endpoint is required; use local_development for local tests",
            ));
        }

        // The task executor (default stages + expressions) that runs tasks.
        let mut task_builder = TaskExecutorBuilder::new();
        if let Some(endpoint) = self.vcs_endpoint {
            let ca_cert = self.vcs_ca_cert.unwrap_or_else(|| config.cert_path.clone());
            let catalog = Arc::new(RemoteVcsCatalog::connect(endpoint.clone(), &ca_cert)?);
            let exchange_client = VcsExchangeClient::connect(endpoint, ca_cert)?;
            task_builder = task_builder
                .with_catalog_validator(catalog)
                .with_exchange_metadata(Arc::new(VcsExchangeMetadata::new(exchange_client)));
        }
        let executor: Arc<dyn TaskExecutor> = Arc::new(task_builder.build());
        let workers = WorkerPool::start(config.workers, rx, executor);

        Ok(ExecutorDaemon {
            config: Some(config),
            ctx,
            _workers: Some(workers),
            raft_rpc_handler: self.raft_rpc_handler,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::identifiers::{NodeId, QueryId, RequestId, StageId, TaskId};
    use common::protocol::{
        DataSink, DataSource, ExecuteTask, ExecutionMode, FileFormat, FileScanTask, MicroOp,
        MicroProgram, NodeKind, NodeLoad, ScalarConstant, VeinControlEvent,
    };
    use rkyv::rancor::Error;

    #[test]
    fn test_zero_copy_dispatch_task() {
        let task = ExecuteTask {
            request_id: RequestId::new(),
            mode: ExecutionMode::Batch,
            task_id: TaskId::new(),
            attempt: 0,
            query_id: QueryId::new(),
            stage_id: StageId::new(),
            source: DataSource::File(FileScanTask {
                files: vec!["test.parquet".to_string()],
                format: FileFormat::Parquet,
                byte_range: None,
                column_ids: vec![0, 1],
                projected_schema: vec![],
                scan_predicates: Vec::new(),
                task_id: TaskId::new(),
                catalog_snapshot: None,
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
            advertise_addr: "127.0.0.1:8081".to_string(),
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
