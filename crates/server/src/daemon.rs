use common::Result;
use s2n_quic::Server;
use std::sync::Arc;
use crate::engine::QueryEngine;

/// The Master Daemon loop.
/// It listens for incoming QUIC connections from clients, extracts the SQL query string,
/// and delegates entirely to the heavily abstracted `QueryEngine`.
pub struct MasterDaemon {
    engine: Arc<dyn QueryEngine>,
}

impl MasterDaemon {
    pub fn new(engine: Arc<dyn QueryEngine>) -> Self {
        Self { engine }
    }

    pub async fn run(&self, bind_addr: std::net::SocketAddr, cert: &str, key: &str) -> Result<()> {
        let mut server = Server::builder()
            .with_tls((std::path::Path::new(cert), std::path::Path::new(key)))
            .map_err(|e| error::VeinError::internal(e.to_string()))?
            .with_io(bind_addr)
            .map_err(|e| error::VeinError::internal(e.to_string()))?
            .start()
            .map_err(|e| error::VeinError::internal(e.to_string()))?;
            
        tracing::info!("Master QUIC Server listening for queries on {}", bind_addr);

        // Infinite event loop accepting connections
        while let Some(mut connection) = server.accept().await {
            let engine_clone = self.engine.clone();
            tokio::spawn(async move {
                // Accept bidirectional streams (client asking a question and waiting for an answer)
                while let Ok(Some(mut stream)) = connection.accept_bidirectional_stream().await {
                    let engine = engine_clone.clone();
                    tokio::spawn(async move {
                        // 1. Read query string from client stream
                        let mut buf = Vec::new();
                        while let Ok(Some(chunk)) = stream.receive().await {
                            let bytes_chunk: bytes::Bytes = chunk;
                            buf.extend_from_slice(&bytes_chunk);
                        }
                        
                        // 2. Dispatch to the completely abstracted QueryEngine
                        if let Ok(query_string) = String::from_utf8(buf) {
                            tracing::info!("Received query: {}", query_string);
                            
                            let mut parser = parser::Parser::new(&query_string);
                            match parser.parse_query() {
                                Ok(query_ast) => {
                                    match engine.submit_query(query_ast).await {
                                        Ok(result_bytes) => {
                                            // 3. Stream Arrow IPC result back to the client
                                            let _ = stream.send(result_bytes).await;
                                        }
                                        Err(e) => {
                                            tracing::error!("Query failed: {}", e);
                                            let _ = stream.send(bytes::Bytes::from(format!("ERROR: {}", e))).await;
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Parse failed: {:?}", e);
                                    let _ = stream.send(bytes::Bytes::from(format!("PARSE ERROR: {:?}", e))).await;
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
