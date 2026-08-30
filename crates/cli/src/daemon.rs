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

                        // 1. Read query string from client stream, capping the
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
                                let _ = stream
                                    .send(bytes::Bytes::from("ERROR: query payload too large"))
                                    .await;
                                return;
                            }
                        }

                        // 2. Decode the payload; on invalid UTF-8 respond with an
                        //    explicit error instead of silently dropping the request.
                        let query_string = match String::from_utf8(buf) {
                            Ok(qs) => qs,
                            Err(_) => {
                                warn!(target: "vein_master", "Received query payload is not valid UTF-8");
                                let _ = stream
                                    .send(bytes::Bytes::from(
                                        "ERROR: query payload was not valid UTF-8",
                                    ))
                                    .await;
                                return;
                            }
                        };

                        // 3. Dispatch to the completely abstracted QueryEngine
                        info!(target: "vein_master", query = %query_string, "Processing query statement");
                        let mut parser = parser::Parser::new(&query_string);
                        match parser.parse_statement() {
                            Ok(statement) => {
                                match engine.submit_query(statement).await {
                                    Ok(result_bytes) => {
                                        info!(
                                            target: "vein_master",
                                            result_bytes = result_bytes.len(),
                                            "Query completed successfully; streaming result to client"
                                        );
                                        // 4. Stream Arrow IPC / text result back to the client
                                        if let Err(err) = stream.send(result_bytes).await {
                                            warn!(
                                                target: "vein_master",
                                                error = %err,
                                                "Failed to send query result to client"
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        error!(target: "vein_master", error = %e, "Query execution failed");
                                        if let Err(err) = stream
                                            .send(bytes::Bytes::from(format!("ERROR: {}", e)))
                                            .await
                                        {
                                            warn!(
                                                target: "vein_master",
                                                error = %err,
                                                "Failed to send query error payload to client"
                                            );
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                error!(target: "vein_master", error = ?e, "Query parsing failed");
                                if let Err(err) = stream
                                    .send(bytes::Bytes::from(format!("PARSE ERROR: {:?}", e)))
                                    .await
                                {
                                    warn!(
                                        target: "vein_master",
                                        error = %err,
                                        "Failed to send parse error payload to client"
                                    );
                                }
                            }
                        }
                    });
                }
            });
        }
        Ok(())
    }
}
