//! Reusable primitives for merging sorted spill runs.

mod frontier;
pub(super) use frontier::{HeapSize, KWayMerge};

#[cfg(test)]
#[path = "spill/tests.rs"]
mod tests;
