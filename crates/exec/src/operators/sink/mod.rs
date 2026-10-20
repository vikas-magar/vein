//! Terminal pipeline sink operators.

/// Discard sink that drops all data
pub mod discard;
/// Collect sink that captures output for the caller
pub mod client;
/// Incremental storage sink backed by an injected transactional writer.
pub mod storage;

pub use client::CollectSink;
pub use discard::DiscardSink;
pub use storage::{WriteReceipt, WriteSink, WriteStorage, WriteTransaction};
