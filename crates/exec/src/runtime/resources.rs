//! Query-scoped memory and spill resources shared by execution stages.

use crate::memory::QueryMemoryTracker;

/// Resources shared by every task and adaptive stage in one query.
///
/// Keeping this handle alive also keeps retained checkpoint arrays accounted
/// against the same memory and spill budgets as continuation operators.
#[derive(Clone)]
pub struct QueryExecutionResources {
    pub(super) memory: QueryMemoryTracker,
}

impl QueryExecutionResources {
    /// Current query memory reservation, including retained checkpoint arrays.
    pub fn memory_used_bytes(&self) -> usize {
        self.memory.used()
    }

    /// Peak query memory reservation observed across all stages.
    pub fn memory_peak_bytes(&self) -> usize {
        self.memory.peak()
    }
}
