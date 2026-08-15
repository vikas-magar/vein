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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};
    use std::thread;

    #[test]
    fn test_shared_scan_state_concurrent() {
        let total_morsels = 1000;
        let state = Arc::new(SharedScanState::new(total_morsels));
        let claimed_morsels = Arc::new(Mutex::new(HashSet::new()));
        let mut handles = vec![];

        for _ in 0..10 {
            let state_clone = Arc::clone(&state);
            let claimed_clone = Arc::clone(&claimed_morsels);
            handles.push(thread::spawn(move || {
                let mut local_claimed = Vec::new();
                while let Some(morsel) = state_clone.next_morsel() {
                    local_claimed.push(morsel);
                }
                let mut lock = claimed_clone.lock().unwrap();
                for m in local_claimed {
                    assert!(lock.insert(m), "morsel claimed twice");
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let lock = claimed_morsels.lock().unwrap();
        assert_eq!(lock.len(), total_morsels);
    }
}
