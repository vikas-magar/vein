//! Memory accounting primitives for bounded join execution.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use common::Result;
use error::VeinError;

/// Default per-join build-state budget.
pub const DEFAULT_JOIN_MEMORY_LIMIT: usize = 512 * 1024 * 1024;

/// Shared memory budget for one join execution.
#[derive(Clone, Debug)]
pub struct JoinMemoryBudget {
    state: Arc<BudgetState>,
}

#[derive(Debug)]
struct BudgetState {
    limit: usize,
    used: AtomicUsize,
}

impl JoinMemoryBudget {
    /// Creates a budget with the default executor-local limit.
    pub fn new() -> Self {
        Self::with_limit(DEFAULT_JOIN_MEMORY_LIMIT)
    }

    /// Creates a budget with an explicit byte limit.
    pub fn with_limit(limit: usize) -> Self {
        Self {
            state: Arc::new(BudgetState {
                limit,
                used: AtomicUsize::new(0),
            }),
        }
    }

    /// Configured byte limit.
    pub fn limit(&self) -> usize {
        self.state.limit
    }

    /// Currently reserved bytes.
    pub fn used(&self) -> usize {
        self.state.used.load(Ordering::Acquire)
    }

    /// Reserves bytes or returns a typed resource-exhaustion error.
    pub fn try_reserve(&self, bytes: usize) -> Result<()> {
        let mut current = self.state.used.load(Ordering::Acquire);
        loop {
            let next = current.checked_add(bytes).ok_or_else(|| {
                VeinError::resource_exhausted("join build state", bytes, self.limit())
            })?;
            if next > self.limit() {
                return Err(VeinError::resource_exhausted(
                    "join build state",
                    bytes,
                    self.limit(),
                ));
            }
            match self.state.used.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => current = observed,
            }
        }
    }

    /// Reserves bytes and returns an ownership guard that releases them on drop.
    pub fn reserve_owned(&self, bytes: usize) -> Result<JoinMemoryReservation> {
        self.try_reserve(bytes)?;
        Ok(JoinMemoryReservation {
            budget: self.clone(),
            bytes,
        })
    }

    /// Returns previously reserved bytes to the budget.
    pub fn release(&self, bytes: usize) {
        self.state.used.fetch_sub(bytes, Ordering::AcqRel);
    }
}

/// RAII reservation for memory retained by join build state.
#[derive(Debug)]
pub struct JoinMemoryReservation {
    budget: JoinMemoryBudget,
    bytes: usize,
}

impl Drop for JoinMemoryReservation {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

impl Default for JoinMemoryBudget {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::JoinMemoryBudget;

    #[test]
    fn owned_reservations_release_automatically() {
        let budget = JoinMemoryBudget::with_limit(10);
        let reservation = budget.reserve_owned(7).expect("reservation fits");
        assert_eq!(budget.used(), 7);
        drop(reservation);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn enforces_limit_and_releases_reservations() {
        let budget = JoinMemoryBudget::with_limit(10);
        budget.try_reserve(7).expect("reservation fits");
        assert_eq!(budget.used(), 7);
        assert!(budget.try_reserve(4).is_err());
        budget.release(7);
        assert_eq!(budget.used(), 0);
        budget.try_reserve(10).expect("released budget is reusable");
    }
}
