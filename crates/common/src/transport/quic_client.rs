use error::{Result, VeinError};
use crate::transport::connection::ConnectionManager;
use async_trait::async_trait;
use s2n_quic::stream::BidirectionalStream;
use s2n_quic::{Client, Connection};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// A highly efficient QUIC client with built-in connection pooling.
#[derive(Clone)]
pub struct QuicClient {
    /// The underlying s2n_quic client provider
    client: Client,
    
    /// Thread-safe cache of active connections mapped by target address
    pool: Arc<Mutex<HashMap<String, Arc<Mutex<Connection>>>>>,
}

impl QuicClient {
    /// Creates a new QuicClient
    pub fn new(client: Client) -> Self {
        Self {
            client,
            pool: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Returns the number of active connections in the pool (useful for testing)
    pub async fn pool_size(&self) -> usize {
        let pool = self.pool.lock().await;
        pool.len()
    }
}

#[async_trait]
impl ConnectionManager for QuicClient {
    type Connection = Connection;
    type Stream = BidirectionalStream;

    async fn get_connection(&self, target: &str) -> Result<Arc<Mutex<Self::Connection>>> {
        let mut pool = self.pool.lock().await;

        // 1. Check if we have an active connection
        if let Some(conn) = pool.get_mut(target) {
            let is_alive = {
                let conn_lock = conn.lock().await;
                conn_lock.remote_addr().is_ok()
            };
            if is_alive {
                return Ok(conn.clone());
            } else {
                pool.remove(target); // Dead connection
            }
        }

        // 2. We don't have a valid connection, establish a new one
        tracing::info!("Establishing new QUIC connection to {}", target);
        
        let target_addr: std::net::SocketAddr = target.parse()
            .map_err(|e| VeinError::internal(format!("Invalid target address: {e}")))?;

        let connect = s2n_quic::client::Connect::new(target_addr).with_server_name("localhost");
        let mut conn = self.client.connect(connect).await
            .map_err(|e| VeinError::internal(format!("QUIC connect failed: {e}")))?;

        conn.keep_alive(true)
            .map_err(|e| VeinError::internal(format!("QUIC keep-alive: {e}")))?;

        let shared_conn = Arc::new(Mutex::new(conn));

        // 3. Cache the connection for future requests
        pool.insert(target.to_string(), shared_conn.clone());
        
        Ok(shared_conn)
    }

    async fn open_stream(&self, target: &str) -> Result<Self::Stream> {
        let shared_conn = self.get_connection(target).await?;
        let mut conn = shared_conn.lock().await;
        
        conn.open_bidirectional_stream().await
            .map_err(|e| VeinError::internal(format!("QUIC open stream: {e}")))
    }
}


