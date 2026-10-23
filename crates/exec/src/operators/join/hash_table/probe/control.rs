//! Bounded control polling shared by candidate production and build emission.

/// Upper bound on hash-probe work between cancellation/downstream checks.
/// This is an execution quantum, independent of table names and cardinality estimates.
pub(super) const MAX_WORK_BETWEEN_POLLS: usize = 1024;

pub(crate) struct WorkBudget {
    remaining: usize,
    quantum: usize,
}

impl WorkBudget {
    pub(crate) fn for_batch(rows: usize) -> Self {
        Self { remaining: 0, quantum: rows.clamp(1, MAX_WORK_BETWEEN_POLLS) }
    }

    /// Checks immediately, then once per bounded quantum of row/chain work.
    #[inline]
    pub(crate) fn should_poll(&mut self) -> bool {
        if self.remaining == 0 {
            self.remaining = self.quantum - 1;
            true
        } else {
            self.remaining -= 1;
            false
        }
    }
}
