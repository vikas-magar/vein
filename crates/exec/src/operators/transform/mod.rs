//! Pipeline transform operators.
//!
//! Streaming boundary transforms for row windows and global ordering.

/// Limit transform for row truncation and offset slicing across batches.
pub mod limit;
/// Blocking, global row ordering over the chunks received by one task.
pub mod sort;

pub use limit::LimitTransform;
pub use sort::SortTransform;
