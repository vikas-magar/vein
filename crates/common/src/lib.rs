#![warn(missing_docs)]

//! Vein Common utilities and abstractions.

/// Newtype identifiers for all system entities.
pub mod identifiers;

/// Cooperative cancellation for async tasks.
pub mod cancellation;

/// Host-aware async task dispatch shared by native and browser runtimes.
pub mod runtime;

/// Shared structured logging initialization.
pub mod logging;
/// Execution program and task types shared with the local runtime.
pub mod protocol;

/// Partition set for shuffle stage routing.
pub mod partition;

/// Apache Arrow data handling.
pub mod arrow;

/// Format-neutral, streaming statistics over borrowed Arrow batches.
pub mod statistics;

/// Monotonic elapsed-time measurement with native and browser implementations.
pub mod time;

/// Re-export unified error type from the `error` crate.
pub use error::{Result, VeinError};
