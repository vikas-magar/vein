use crate::engine::QueryEngine;
use common::Result;
use s2n_quic::Server;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{debug, error, info, trace, warn};

/// Upper bound on a single query payload (guards against unbounded memory
/// growth from a client streaming an arbitrarily large request).
const MAX_QUERY_BYTES: usize = 64 * 1024 * 1024;

/// Default cap on concurrently executing query handlers. Bounding this prevents
/// a flood of connections/streams from spawning an unbounded number of tasks.
const DEFAULT_MAX_CONCURRENT_QUERIES: usize = 1024;

/// The Master Daemon loop.
/// It listens for incoming QUIC connections from clients, extracts the SQL query string,
/// and delegates entirely to the heavily abstracted `QueryEngine`.
pub struct MasterDaemon {
    engine: Arc<dyn QueryEngine>,
    max_concurrent_queries: usize,
}

impl MasterDaemon {
    pub fn new(engine: Arc<dyn QueryEngine>) -> Self {
        Self {
            engine,
            max_concurrent_queries: DEFAULT_MAX_CONCURRENT_QUERIES,
        }
    }

    /// Override the maximum number of concurrently executing query handlers.
    pub fn with_max_concurrent_queries(mut self, max: usize) -> Self {
        self.max_concurrent_queries = max;
        self
    }

    pub async fn run(&self, bind_addr: std::net::SocketAddr, cert: &str, key: &str) -> Result<()> {
        let mut server = Server::builder()
            .with_tls((std::path::Path::new(cert), std::path::Path::new(key)))
            .map_err(|e| error::VeinError::internal(format!("TLS configuration error: {e}")))?
            .with_io(bind_addr)
            .map_err(|e| error::VeinError::internal(format!("Socket bind error on {bind_addr}: {e}")))?
            .start()
            .map_err(|e| error::VeinError::internal(format!("Server start error: {e}")))?;

        info!(target: "vein_master", %bind_addr, "Master QUIC Server listening for incoming queries");

        // Bound the number of concurrently executing query handlers. Every
        // handler acquires a permit for its whole lifetime; this prevents a
        // flood of connections/streams from spawning unbounded concurrent tasks.
        let limit = Arc::new(Semaphore::new(self.max_concurrent_queries));

        // Infinite event loop accepting connections
        while let Some(mut connection) = server.accept().await {
            let engine_clone = self.engine.clone();
            let limit_clone = limit.clone();
            debug!(target: "vein_master", "Accepted incoming QUIC connection");

            tokio::spawn(async move {
                // Accept bidirectional streams (client asking a question and waiting for an answer)
                while let Ok(Some(mut stream)) = connection.accept_bidirectional_stream().await {
                    let engine = engine_clone.clone();
                    let limit = limit_clone.clone();
                    debug!(target: "vein_master", "Accepted bidirectional query stream");

                    tokio::spawn(async move {
                        // Hold a permit for the lifetime of this handler to bound
                        // the total number of concurrent query executions.
                        let _permit = match limit.acquire_owned().await {
                            Ok(permit) => permit,
                            Err(_) => {
                                warn!(target: "vein_master", "Query handler aborted: semaphore closed during shutdown");
                                return;
                            }
                        };

                        // 1. Read rkyv payload from client stream, capping the
                        //    total payload size to avoid unbounded buffering.
                        let mut buf = Vec::new();
                        while let Ok(Some(chunk)) = stream.receive().await {
                            let bytes_chunk: bytes::Bytes = chunk;
                            trace!(target: "vein_master", chunk_bytes = bytes_chunk.len(), "Received query stream chunk");
                            buf.extend_from_slice(&bytes_chunk);
                            if buf.len() > MAX_QUERY_BYTES {
                                warn!(
                                    target: "vein_master",
                                    max_bytes = MAX_QUERY_BYTES,
                                    payload_bytes = buf.len(),
                                    "Query payload exceeds maximum allowed size; rejecting"
                                );
                                let err_resp = common::protocol::ProtocolResponse::Error {
                                    message: "Query payload exceeds maximum allowed size".to_string(),
                                };
                                if let Ok(err_bytes) = rkyv::to_bytes::<rkyv::rancor::Error>(&err_resp) {
                                    let _ = stream.send(bytes::Bytes::from(err_bytes.into_vec())).await;
                                }
                                return;
                            }
                        }

                        // 2. Zero-copy access the typed ProtocolRequest via rkyv
                        let request = match rkyv::access::<common::protocol::ArchivedProtocolRequest, rkyv::rancor::Error>(&buf) {
                            Ok(req) => req,
                            Err(e) => {
                                warn!(target: "vein_master", error = %e, "Failed to decode ProtocolRequest from client");
                                let err_resp = common::protocol::ProtocolResponse::Error {
                                    message: format!("Malformed ProtocolRequest payload: {e}"),
                                };
                                if let Ok(err_bytes) = rkyv::to_bytes::<rkyv::rancor::Error>(&err_resp) {
                                    let _ = stream.send(bytes::Bytes::from(err_bytes.into_vec())).await;
                                }
                                return;
                            }
                        };

                        // 3. Process the typed request & produce a typed ProtocolResponse
                        let response = match request {
                            common::protocol::ArchivedProtocolRequest::ExecuteSql { sql } => {
                                let sql_str = sql.as_str();
                                info!(target: "vein_master", query = %sql_str, "Processing ExecuteSql request");
                                let mut parser = parser::Parser::new(sql_str);
                                match parser.parse_statement() {
                                    Ok(statement) => match engine.submit_query(statement).await {
                                        Ok(result_bytes) => {
                                            if let Ok(text) = String::from_utf8(result_bytes.to_vec()) {
                                                common::protocol::ProtocolResponse::Plan { text }
                                            } else {
                                                common::protocol::ProtocolResponse::Data {
                                                    batch: result_bytes.to_vec(),
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            error!(target: "vein_master", error = %e, "Query execution failed");
                                            common::protocol::ProtocolResponse::Error {
                                                message: e.to_string(),
                                            }
                                        }
                                    },
                                    Err(e) => {
                                        error!(target: "vein_master", error = ?e, "Query parsing failed");
                                        common::protocol::ProtocolResponse::Error {
                                            message: format!("Parse error: {e:?}"),
                                        }
                                    }
                                }
                            }
                            common::protocol::ArchivedProtocolRequest::CancelQuery { query_id } => {
                                info!(target: "vein_master", %query_id, "Received CancelQuery request");
                                common::protocol::ProtocolResponse::QueryCancelled {
                                    query_id: common::identifiers::QueryId::from_raw(query_id.0.to_native()),
                                }
                            }
                        };

                        // 4. Serialize response via rkyv and stream back to client
                        match rkyv::to_bytes::<rkyv::rancor::Error>(&response) {
                            Ok(response_bytes) => {
                                info!(
                                    target: "vein_master",
                                    bytes_len = response_bytes.len(),
                                    "Streaming rkyv ProtocolResponse back to client"
                                );
                                if let Err(err) = stream.send(bytes::Bytes::from(response_bytes.into_vec())).await {
                                    warn!(
                                        target: "vein_master",
                                        error = %err,
                                        "Failed to send ProtocolResponse to client"
                                    );
                                }
                            }
                            Err(e) => {
                                error!(target: "vein_master", error = %e, "Failed to serialize ProtocolResponse via rkyv");
                            }
                        }
                    });
                }
            });
        }
        Ok(())
    }
}
