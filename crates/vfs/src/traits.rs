use async_trait::async_trait;
use bytes::Bytes;
use common::Result;

/// Highly abstract Write-Ahead Log (WAL) to decouple durability from the storage medium.
/// Allows swapping a local NVMe WAL with a memory-backed WAL (for tests) or a remote S3 WAL.
#[async_trait]
pub trait WriteAheadLog: Send + Sync {
    /// Append a generic payload to the log. Returns a Monotonic Log Sequence Number (LSN).
    async fn append_entry(&self, payload: Bytes) -> Result<u64>;

    /// Fsync the log to guarantee OS-level durability up to the given LSN.
    async fn sync(&self, lsn: u64) -> Result<()>;
}

/// Abstract Consensus Protocol.
/// Allows us to swap our Leaderless SMR protocol with standard Raft or Paxos seamlessly.
#[async_trait]
pub trait ConsensusProtocol: Send + Sync {
    /// Propose a state machine transition to the cluster.
    async fn propose(&self, payload: Bytes) -> Result<()>;

    /// Mathematically resolve concurrent write collisions (e.g., using Vector Clocks).
    fn resolve_conflicts(&self) -> Result<()>;
}

/// Abstract Block Storage interface.
/// Allows decoupling the storage engine from Memory-Mapped files, enabling Direct I/O or S3 drop-ins.
#[async_trait]
pub trait BlockStorage: Send + Sync {
    /// Write an extent (chunk of data) to a cluster-unique block.
    async fn write_extent(&self, block_id: String, data: Bytes) -> Result<u64>;

    /// Sync the physical block to disk, bypassing the OS Page Cache.
    async fn sync_block(&self, block_id: String) -> Result<()>;
}
