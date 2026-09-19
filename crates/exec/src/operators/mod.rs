//! Physical pipeline stage operators (Sources, Transforms, Sinks).
//!
//! Organizes execution stages by streaming role:
//! - [`source`]: Generates data chunks ([`ScanSource`](source::ScanSource), [`MemorySource`](source::MemorySource)).
//! - [`transform`]: Processes data chunks ([`LimitTransform`](transform::LimitTransform), [`VectorVmExecutor`](crate::vm::VectorVmExecutor)).
//! - [`sink`]: Terminates data streams ([`CollectSink`](sink::CollectSink), [`DiscardSink`](sink::DiscardSink)).

/// Join operators and source-independent join key extraction.
pub mod join;
/// Sink operators that consume data batches
pub mod sink;
/// Source operators that generate data batches
pub mod source;
/// Transform operators that mutate data batches
pub mod transform;

pub use join::{
    ArrowJoinKeyExtractor, HashJoinTable, JoinKey, JoinKeyBatch, JoinKeyExtractor, JoinKeyPart,
    JoinKeyRow, JoinMatch, JoinPartitioner, JoinRowRef,
};
pub use sink::{CollectSink, DiscardSink};
pub use source::{MemorySource, ScanSource};
pub use transform::LimitTransform;
