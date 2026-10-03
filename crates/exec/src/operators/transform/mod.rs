//! Pipeline transform operators.
//!
//! Includes streaming boundary transforms ([`LimitTransform`]) as well as the
//! unified in-memory Vector VM engine ([`VectorVmExecutor`]).

/// Limit transform for row truncation and offset slicing across batches.
pub mod limit;
/// Blocking, global row ordering over the chunks received by one task.
pub mod sort;

pub use limit::LimitTransform;
pub use sort::SortTransform;

/// Re-export the Vector VM execution engine as the primary in-memory compute transform.
pub use crate::vm::VectorVmExecutor;
