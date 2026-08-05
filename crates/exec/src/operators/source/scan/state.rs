use std::sync::atomic::{AtomicUsize, Ordering};

/// Shared state across all local workers executing a specific Macro-Morsel.
pub struct SharedScanState {
    /// Total number of micro-morsels available locally.
    pub total_morsels: usize,
    /// The next micro-morsel index to be claimed (Lock-Free).
    pub current_morsel_idx: AtomicUsize,
}

impl SharedScanState {
    /// Creates a new SharedScanState with the given total number of morsels.
    pub fn new(total_morsels: usize) -> Self {
        Self {
            total_morsels,
            current_morsel_idx: AtomicUsize::new(0),
        }
    }

    /// Hardware atomic increment. No Mutex, no waiting, no OS involvement.
    #[inline(always)]
    pub fn next_morsel(&self) -> Option<usize> {
        let claimed = self.current_morsel_idx.fetch_add(1, Ordering::Relaxed);
        if claimed < self.total_morsels {
            Some(claimed)
        } else {
            None
        }
    }
}
