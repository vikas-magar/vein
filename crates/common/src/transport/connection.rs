use async_trait::async_trait;
use error::Result;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Abstract trait for managing network connections and streams.
/// This allows the engine to mock the network layer for testing.
#[async_trait]
pub trait ConnectionManager: Send + Sync + 'static {
    /// The shared connection type returned by this manager.
    type Connection: Send + Sync + 'static;

    /// The type of bidirectional stream returned by this manager.
    type Stream: Send + Sync;

    /// Gets an active connection to the target (e.g., "127.0.0.1:4000").
    /// If a connection exists in the pool, it is reused. Otherwise, a new one is established.
    async fn get_connection(&self, target: &str) -> Result<Arc<Mutex<Self::Connection>>>;

    /// Convenience method to get a connection and immediately open a bidirectional stream.
    async fn open_stream(&self, target: &str) -> Result<Self::Stream>;
}
