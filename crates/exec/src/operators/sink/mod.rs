//! Terminal pipeline sink operators.

/// Discard sink that drops all data
pub mod discard;
/// Collect sink that captures output for the caller
pub mod client;
/// Storage sink that persists data to .exo or Parquet
pub mod storage;

pub use client::CollectSink;
pub use discard::DiscardSink;
pub use storage::StorageSink;
