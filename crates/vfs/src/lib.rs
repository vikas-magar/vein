//! VeinFS: The Infinite Virtual Disk & Unified Data Plane

pub mod block;
pub mod config;
pub mod error;
pub mod ingest;
pub mod metadata;
pub mod rpc;
pub mod topology;

pub use config::VfsConfig;
pub use error::{Result, VfsError};
