use crate::daemon::DaemonContext;
use common::Result;
use common::protocol::VeinControlEvent;
use common::transport::build_quic_server;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info};

/// The QUIC network listener for the Executor Daemon.
pub struct ExecutorDaemon {
    ctx: Arc<DaemonContext>,
}

impl ExecutorDaemon {
    /// Creates a new network listener for the given context.
    pub fn new(ctx: Arc<DaemonContext>) -> Self {
        Self { ctx }
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
                        let mut buffer = Vec::new();
                        while let Ok(Some(chunk)) = stream.receive().await {
                            buffer.extend_from_slice(&chunk);
                        }

                        if buffer.is_empty() {
                            return;
                        }

                        let event: VeinControlEvent = match rkyv::from_bytes::<
                            VeinControlEvent,
                            rkyv::rancor::Error,
                        >(&buffer)
                        {
                            Ok(e) => e,
                            Err(e) => {
                                error!("Failed to deserialize control event bytes: {}", e);
                                return;
                            }
                        };

                        match event {
                            VeinControlEvent::DispatchTask(task) => {
                                info!(
                                    "Received DispatchTask for Task {:?} (Query {:?})",
                                    task.task_id, task.query_id
                                );

                                if let Err(e) = ctx.task_queue.send_async(task).await {
                                    error!("Failed to push task to worker queue: {}", e);
                                }
                            }
                            VeinControlEvent::CancelTask { task_id, query_id } => {
                                info!(
                                    "Received CancelTask for Task {:?} (Query {:?})",
                                    task_id, query_id
                                );
                                // TODO: Cancel task in DaemonContext
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
