//! VeinFS: The Infinite Virtual Disk & Unified Data Plane

pub mod block;
pub mod config;
pub mod error;
pub mod consensus;
pub mod ingest;
pub mod metadata;
pub mod rpc;
pub mod topology;
pub mod recovery;

pub use config::VfsConfig;
pub use error::{Result, VfsError};
