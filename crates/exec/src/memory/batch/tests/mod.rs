use super::*;
use crate::memory::QueryMemoryTracker;
use common::cancellation::CancellationToken;

#[test]
fn credit_is_acquired_before_use_and_only_committed_bytes_survive() {
    let tracker = QueryMemoryTracker::unlimited(CancellationToken::new());
    let mut reservation = tracker.reserve(7, "existing state").unwrap();
    {
        let mut batch = BatchReservation::new(&mut reservation, "batch state");
        batch.charge(10, 5).unwrap();
        assert_eq!(tracker.used(), 57);
        batch.charge(10, 4).unwrap();
        assert_eq!(tracker.used(), 57, "second charge reuses already reserved credit");
    }
    assert_eq!(tracker.used(), 27);
    assert_eq!(reservation.bytes(), 27);
    drop(reservation);
    assert_eq!(tracker.used(), 0);
}

#[test]
fn a_large_upper_bound_cannot_reject_an_exact_charge_that_fits() {
    let tracker = QueryMemoryTracker::new(32, CancellationToken::new());
    let mut reservation = tracker.reserve(0, "existing state").unwrap();
    {
        let mut batch = BatchReservation::new(&mut reservation, "batch state");
        batch.charge(10, 100).unwrap();
        batch.charge(10, 99).unwrap();
        batch.charge(10, 98).unwrap();
        assert_eq!(tracker.used(), 30);
        assert!(matches!(batch.charge(10, 97), Err(VeinError::ResourceExhausted { .. })));
    }
    assert_eq!(reservation.bytes(), 30);
    drop(reservation);
    assert_eq!(tracker.used(), 0);
}

#[test]
fn unused_credit_is_released_during_error_unwinding() {
    let tracker = QueryMemoryTracker::unlimited(CancellationToken::new());
    let mut reservation = tracker.reserve(0, "existing state").unwrap();
    let mut attempt = || -> Result<()> {
        let mut batch = BatchReservation::new(&mut reservation, "batch state");
        batch.charge(10, 8)?;
        Err(VeinError::internal("later operation failed"))
    };
    assert!(attempt().is_err());
    assert_eq!(tracker.used(), 10);
}

#[test]
fn cancellation_and_invalid_bounds_are_not_retried_as_memory_shortages() {
    let cancellation = CancellationToken::new();
    let tracker = QueryMemoryTracker::new(32, cancellation.clone());
    let mut reservation = tracker.reserve(0, "existing state").unwrap();
    let mut batch = BatchReservation::new(&mut reservation, "batch state");
    assert!(batch.charge(1, 0).is_err());
    cancellation.cancel();
    assert!(matches!(batch.charge(10, 100), Err(VeinError::Cancelled)));
    drop(batch);
    assert_eq!(tracker.used(), 0);
}
