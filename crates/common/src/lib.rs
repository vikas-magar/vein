#![warn(missing_docs)]

//! Vein Common utilities and abstractions.

/// Newtype identifiers for all system entities.
pub mod identifiers;

/// Cooperative cancellation for async tasks.
pub mod cancellation;

/// System initialization and node primitives.
pub mod system;

/// Shared structured logging initialization.
pub mod logging;
/// Execution program and task types shared with the local runtime.
pub mod protocol;

/// Partition set for shuffle stage routing.
pub mod partition;

/// Apache Arrow data handling.
pub mod arrow;

/// Bounded-query result stream primitives shared by execution boundaries.
pub mod result_stream;
pub use result_stream::{
    bounded_result_channel, ResultBatch, ResultChannelSender, ResultEvent, ResultStream,
    SendableResultStream,
};

/// Re-export unified error type from the `error` crate.
pub use error::{Result, VeinError};
