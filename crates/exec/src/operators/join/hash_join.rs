//! The join operator: a build sink and a probe node.
//!
//! A join is a pipeline breaker, so it is two nodes rather than one. [`BuildSink`]
//! absorbs the build side; the finished [`HashTable`] is handed to [`HashJoin`],
//! which probes. The runtime sequences them by draining the build input before
//! it drives the probe input.

mod build;
mod collector;
mod output;
mod probe;

pub use build::BuildSink;
pub(crate) use build::BuildResult;
pub use collector::JoinFilterCollector;
pub use probe::HashJoin;

#[cfg(test)]
#[path = "hash_join/tests.rs"]
mod tests;
