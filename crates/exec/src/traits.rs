use common::Result;
use async_trait::async_trait;
use bytes::Bytes;
use crate::daemon::pipeline::VeinBatch;

/// Abstract Network Transport for shuffling data between distributed Executors.
/// Allows swapping QUIC with TCP, shared memory, or test channels.
#[async_trait]
pub trait DataTransport: Send + Sync {
    /// Stream Arrow IPC bytes to a remote executor node.
    async fn send_stream(&self, node_id: u32, stream_id: String, data: Bytes) -> Result<()>;
    
    /// Receive a stream of Arrow IPC bytes from a remote executor node.
    async fn receive_stream(&self, stream_id: String) -> Result<Bytes>;
}

/// Abstraction for Factorized Vectorized Execution.
/// Operators implementing this trait can natively process Kuzu-style factorized chunks
/// without flatting the data, drastically reducing memory usage for many-to-many joins.
pub trait FactorizedOperator: Send + Sync {
    /// Process a factorized chunk directly.
    fn process_factorized(&self, batch: &VeinBatch) -> Result<VeinBatch>;
}

/// Abstraction for Worst-Case Optimal Joins (WCOJ).
/// Implements the leapfrog triejoin algorithm for multi-way cyclic graph joins.
pub trait WcojOperator: Send + Sync {
    /// Initialize the trie iterators for the relations involved in the cyclic join.
    fn init_iterators(&mut self) -> Result<()>;
    
    /// Seek the iterators to the next matching bound.
    fn leapfrog_seek(&mut self) -> Result<Option<VeinBatch>>;
}
