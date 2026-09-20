use crate::engine::QueryEngine;
use common::Result;
use common::transport::{extract_control_payload, frame_control_message};
use futures::StreamExt;
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
            .map_err(|e| {
                error::VeinError::internal(format!("Socket bind error on {bind_addr}: {e}"))
            })?
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
                                    message: "Query payload exceeds maximum allowed size"
                                        .to_string(),
                                };
                                if let Ok(err_bytes) =
                                    rkyv::to_bytes::<rkyv::rancor::Error>(&err_resp)
                                {
                                    let _ = stream.send(frame_control_message(&err_bytes)).await;
                                }
                                return;
                            }
                        }

                        if buf.is_empty() {
                            return;
                        }

                        // Check if this is an inter-node control event (0x02 opcode)
                        if let Some(payload) = extract_control_payload(&buf) {
                            let mut aligned = rkyv::util::AlignedVec::<16>::new();
                            aligned.extend_from_slice(payload);

                            if let Ok(archived) = rkyv::access::<
                                common::protocol::ArchivedVeinControlEvent,
                                rkyv::rancor::Error,
                            >(&aligned)
                            {
                                match archived {
                                    common::protocol::ArchivedVeinControlEvent::Heartbeat {
                                        node_id,
                                        node_kind,
                                        load,
                                        advertise_addr,
                                        ..
                                    } => {
                                        let nid = common::identifiers::NodeId::from_raw(
                                            node_id.0.to_native(),
                                        );
                                        let kind = common::protocol::NodeKind::from(node_kind);
                                        let load = common::protocol::NodeLoad::from(load);
                                        let addr_str = advertise_addr.as_str();
                                        debug!(target: "vein_master", %nid, %addr_str, "Received heartbeat from node");
                                        if let (Ok(addr), Some(registry)) = (
                                            addr_str.parse::<std::net::SocketAddr>(),
                                            engine.registry(),
                                        ) {
                                            registry.record_heartbeat(nid, addr, kind, load).await;
                                        }
                                        let ack = common::protocol::VeinControlEvent::HeartbeatAck;
                                        if let Ok(bytes) =
                                            rkyv::to_bytes::<rkyv::rancor::Error>(&ack)
                                        {
                                            let wire = frame_control_message(&bytes);
                                            let _ = stream.send(wire).await;
                                            let _ = stream.finish();
                                        }
                                        return;
                                    }
                                    _ => {
                                        debug!(target: "vein_master", "Received other control event on master");
                                        return;
                                    }
                                }
                            }
                        }

                        // 2. Zero-copy access the typed ProtocolRequest via rkyv
                        let client_payload = if buf.len() >= 5 && buf[0] == 0x01 {
                            let len = u32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
                            if buf.len() >= 5 + len {
                                &buf[5..5 + len]
                            } else {
                                &buf[5..]
                            }
                        } else {
                            &buf[..]
                        };

                        let request = match rkyv::access::<
                            common::protocol::ArchivedProtocolRequest,
                            rkyv::rancor::Error,
                        >(client_payload)
                        {
                            Ok(req) => req,
                            Err(e) => {
                                warn!(target: "vein_master", error = %e, "Failed to decode ProtocolRequest from client");
                                let err_resp = common::protocol::ProtocolResponse::Error {
                                    message: format!("Malformed ProtocolRequest payload: {e}"),
                                };
                                if let Ok(err_bytes) =
                                    rkyv::to_bytes::<rkyv::rancor::Error>(&err_resp)
                                {
                                    let _ = stream.send(frame_control_message(&err_bytes)).await;
                                }
                                return;
                            }
                        };

                        macro_rules! send_response {
                            ($response:expr) => {{
                                let response = $response;
                                match rkyv::to_bytes::<rkyv::rancor::Error>(&response) {
                                    Ok(response_bytes) => {
                                        if let Err(err) = stream
                                            .send(frame_control_message(&response_bytes))
                                            .await
                                        {
                                            warn!(
                                                target: "vein_master",
                                                error = %err,
                                                "Failed to send ProtocolResponse to client"
                                            );
                                            return;
                                        }
                                    }
                                    Err(error) => {
                                        error!(
                                            target: "vein_master",
                                            error = %error,
                                            "Failed to serialize ProtocolResponse via rkyv"
                                        );
                                        return;
                                    }
                                }
                            }};
                        }

                        match request {
                            common::protocol::ArchivedProtocolRequest::ExecuteSql { sql } => {
                                let sql_str = sql.as_str();
                                info!(target: "vein_master", query = %sql_str, "Processing ExecuteSql request");
                                let mut parser = parser::Parser::new(sql_str);
                                match parser.parse_statement() {
                                    Ok(statement) => match engine.submit_query(statement).await {
                                        Ok(crate::engine::QueryOutput::Plan(text)) => {
                                            send_response!(
                                                common::protocol::ProtocolResponse::Plan { text }
                                            );
                                        }
                                        Ok(crate::engine::QueryOutput::Write {
                                            table,
                                            row_count,
                                        }) => {
                                            send_response!(common::protocol::ProtocolResponse::WriteCompleted {
                                                table,
                                                row_count,
                                            });
                                        }
                                        Ok(crate::engine::QueryOutput::Data {
                                            query_id,
                                            stream: mut result_stream,
                                        }) => {
                                            send_response!(common::protocol::ProtocolResponse::QueryResultStart { query_id });
                                            let mut sequence = 0u64;
                                            let mut row_count = 0u64;
                                            while let Some(event) = result_stream.next().await {
                                                match event {
                                                    Ok(common::ResultEvent::Start { .. }) => {}
                                                    Ok(common::ResultEvent::Batch(
                                                        result_batch,
                                                    )) => {
                                                        let mut encoder =
                                                            common::arrow::IpcStreamEncoder::new();
                                                        if let Err(error) =
                                                            encoder.encode(&result_batch.batch)
                                                        {
                                                            send_response!(common::protocol::ProtocolResponse::Error {
                                                                message: error.to_string(),
                                                            });
                                                            return;
                                                        }
                                                        let payload = match encoder.finalize() {
                                                            Ok(payload) => payload,
                                                            Err(error) => {
                                                                send_response!(common::protocol::ProtocolResponse::Error {
                                                                    message: error.to_string(),
                                                                });
                                                                return;
                                                            }
                                                        };
                                                        row_count = match row_count.checked_add(
                                                            result_batch.row_count() as u64,
                                                        ) {
                                                            Some(value) => value,
                                                            None => {
                                                                send_response!(common::protocol::ProtocolResponse::Error {
                                                                    message: "result row count overflow".to_string(),
                                                                });
                                                                return;
                                                            }
                                                        };
                                                        send_response!(common::protocol::ProtocolResponse::QueryResultBatch {
                                                            query_id,
                                                            sequence,
                                                            batch: payload,
                                                        });
                                                        sequence += 1;
                                                    }
                                                    Ok(common::ResultEvent::End { .. }) => {}
                                                    Ok(common::ResultEvent::Error { message }) => {
                                                        send_response!(common::protocol::ProtocolResponse::Error { message });
                                                        return;
                                                    }
                                                    Err(error) => {
                                                        send_response!(common::protocol::ProtocolResponse::Error {
                                                            message: error.to_string(),
                                                        });
                                                        return;
                                                    }
                                                }
                                            }
                                            send_response!(common::protocol::ProtocolResponse::QueryResultEnd {
                                                query_id,
                                                row_count,
                                            });
                                        }
                                        Err(error) => {
                                            error!(target: "vein_master", error = %error, "Query execution failed");
                                            send_response!(
                                                common::protocol::ProtocolResponse::Error {
                                                    message: error.to_string(),
                                                }
                                            );
                                        }
                                    },
                                    Err(error) => {
                                        send_response!(common::protocol::ProtocolResponse::Error {
                                            message: format!("Parse error: {error:?}"),
                                        });
                                    }
                                }
                            }
                            common::protocol::ArchivedProtocolRequest::CancelQuery { query_id } => {
                                send_response!(
                                    common::protocol::ProtocolResponse::QueryCancelled {
                                        query_id: common::identifiers::QueryId::from_raw(
                                            query_id.0.to_native()
                                        ),
                                    }
                                );
                            }
                        }
                        let _ = stream.finish();
                    });
                }
            });
        }
        Ok(())
    }
}
