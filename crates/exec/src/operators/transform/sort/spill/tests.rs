use std::mem::size_of;

use super::{Entry, KWayMerge};
use crate::memory::QueryMemoryTracker;
use error::VeinError;

#[test]
fn generic_frontier_merges_keys_and_stably_breaks_ties() {
    let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
    let mut merge = KWayMerge::new(&tracker).expect("create generic merge frontier");
    merge.push(3, (0, 0), "run0-row0").unwrap();
    merge.push(1, (1, 0), "run1-row0").unwrap();
    merge.push(1, (0, 4), "run0-row4").unwrap();
    merge.push(1, (0, 2), "run0-row2").unwrap();
    merge.push(2, (1, 1), "run1-row1").unwrap();

    assert_eq!(
        std::iter::from_fn(|| merge.pop()).collect::<Vec<_>>(),
        vec!["run0-row2", "run0-row4", "run1-row0", "run1-row1", "run0-row0"]
    );
}

#[test]
fn generic_frontier_accepts_non_arrow_payloads() {
    let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
    let mut merge = KWayMerge::new(&tracker).expect("create generic merge frontier");
    merge.push("b", (0, 0), 20_u64).unwrap();
    merge.push("a", (1, 0), 10_u64).unwrap();

    assert_eq!(merge.pop(), Some(10));
    assert_eq!(merge.pop(), Some(20));
    assert_eq!(merge.pop(), None);
    assert_eq!(tracker.used(), 0, "popping releases frontier reservations");
}

#[test]
fn generic_frontier_respects_budget_and_releases_on_drop() {
    let tracker = QueryMemoryTracker::new(1, common::cancellation::CancellationToken::new());
    let mut merge = KWayMerge::<u64, u64>::new(&tracker).expect("create empty frontier");

    assert!(matches!(
            merge.push(1, (0, 0), 2),
            Err(VeinError::ResourceExhausted { .. })
        ));
    assert_eq!(tracker.used(), 0);

    drop(merge);
    assert_eq!(tracker.used(), 0);
}

#[test]
fn frontier_accounts_inline_entry_once_and_owned_key_capacity_separately() {
    let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
    let mut merge = KWayMerge::<String, usize>::new(&tracker).expect("create frontier");
    let key = String::from("owned sort key");
    let expected = size_of::<Entry<String, usize>>() + key.capacity();

    merge.push(key, (0, 0), 0).expect("retain owned key");

    assert_eq!(tracker.used(), expected);
    assert_eq!(merge.pop(), Some(0));
    assert_eq!(tracker.used(), 0, "popping releases entry and owned key bytes");
}
