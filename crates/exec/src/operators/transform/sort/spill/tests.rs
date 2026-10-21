use std::mem::size_of;

use super::frontier::Entry;
use super::KWayMerge;
use crate::memory::QueryMemoryTracker;
use error::VeinError;

#[test]
fn generic_frontier_merges_keys_and_stably_breaks_ties() {
    let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
    let mut merge = KWayMerge::new(&tracker).expect("create generic merge frontier");
    let mut compare = |left: &i32, right: &i32| Ok(left.cmp(right));
    merge.push(3, (0, 0), "run0-row0", &mut compare).unwrap();
    merge.push(1, (1, 0), "run1-row0", &mut compare).unwrap();
    merge.push(1, (0, 4), "run0-row4", &mut compare).unwrap();
    merge.push(1, (0, 2), "run0-row2", &mut compare).unwrap();
    merge.push(2, (1, 1), "run1-row1", &mut compare).unwrap();

    assert_eq!(
        std::iter::from_fn(|| merge.pop(&mut compare).expect("pop merge frontier"))
            .collect::<Vec<_>>(),
        vec!["run0-row2", "run0-row4", "run1-row0", "run1-row1", "run0-row0"]
    );
}

#[test]
fn generic_frontier_accepts_non_arrow_payloads() {
    let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
    let mut merge = KWayMerge::new(&tracker).expect("create generic merge frontier");
    let mut compare = |left: &&str, right: &&str| Ok(left.cmp(right));
    merge.push("b", (0, 0), 20_u64, &mut compare).unwrap();
    merge.push("a", (1, 0), 10_u64, &mut compare).unwrap();

    assert_eq!(merge.pop(&mut compare).unwrap(), Some(10));
    assert_eq!(merge.pop(&mut compare).unwrap(), Some(20));
    assert_eq!(merge.pop(&mut compare).unwrap(), None);
    assert_eq!(tracker.used(), 0, "popping releases frontier reservations");
}

#[test]
fn generic_frontier_respects_budget_and_releases_on_drop() {
    let tracker = QueryMemoryTracker::new(1, common::cancellation::CancellationToken::new());
    let mut merge = KWayMerge::<u64, u64>::new(&tracker).expect("create empty frontier");
    let mut compare = |left: &u64, right: &u64| Ok(left.cmp(right));

    assert!(matches!(
            merge.push(1, (0, 0), 2, &mut compare),
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
    let mut compare = |left: &String, right: &String| Ok(left.cmp(right));

    merge
        .push(key, (0, 0), 0, &mut compare)
        .expect("retain owned key");

    assert_eq!(tracker.used(), expected);
    assert_eq!(merge.pop(&mut compare).unwrap(), Some(0));
    assert_eq!(tracker.used(), 0, "popping releases entry and owned key bytes");
}

#[test]
fn comparator_error_rolls_back_frontier_push_and_its_reservation() {
    let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
    let mut merge = KWayMerge::new(&tracker).expect("create frontier");
    let mut compare = |left: &u64, right: &u64| Ok(left.cmp(right));
    merge
        .push(3, (0, 0), 3, &mut compare)
        .expect("insert initial entry");
    let retained_bytes = tracker.used();

    let mut fail = |_: &u64, _: &u64| Err(VeinError::internal("comparison failed"));
    assert!(merge.push(1, (0, 1), 1, &mut fail).is_err());
    assert_eq!(tracker.used(), retained_bytes);
    assert_eq!(merge.pop(&mut compare).unwrap(), Some(3));
    assert_eq!(tracker.used(), 0);
}
