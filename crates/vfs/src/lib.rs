//! VeinFS: The Infinite Virtual Disk & Unified Data Plane

pub mod block;
pub mod config;
pub mod consensus;
pub mod ebpf;
pub mod error;
pub mod ingest;
pub mod metadata;
pub mod recovery;
pub mod rpc;
pub mod topology;

pub use block::{BlockKey, PlaneType, SegmentKey};
pub use config::VfsConfig;
pub use error::{Result, VfsError};
/// Abstract traits defining the system architecture.
pub mod traits;
