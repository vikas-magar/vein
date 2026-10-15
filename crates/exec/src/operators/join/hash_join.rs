//! The join operator: a build sink and a probe node.
//!
//! A join is a pipeline breaker, so it is two nodes rather than one. [`BuildSink`]
//! absorbs the build side; the finished [`HashTable`] is handed to [`HashJoin`],
//! which probes. The runtime sequences them — drive the build side to
//! completion, then probe — which is precisely why a join cannot be a VM
//! instruction: the VM sees one chunk at a time and has no second input.

mod build;
mod collector;
mod probe;

pub use build::BuildSink;
pub(crate) use build::BuildResult;
pub use collector::JoinFilterCollector;
pub use probe::HashJoin;

#[cfg(test)]
#[path = "hash_join/tests.rs"]
mod tests;
