use super::*;
use crate::operators::join::JoinKeys;
use crate::pipeline::test_support::dense_view;
use arrow::array::{ArrayRef, Int64Array};
use std::sync::Arc;

fn view(values: Vec<Option<i64>>) -> DataChunkView {
    dense_view(vec![Arc::new(Int64Array::from(values)) as ArrayRef])
}

#[test]
fn bounded_cursor_resumes_duplicate_chain_without_losing_order() {
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    table.insert(view(vec![Some(7); 33])).unwrap();
    let probe = view(vec![Some(7), None, Some(7)]);
    let mut cursor = HashProbe::new(&table, &probe, false).unwrap();
    let mut batch = Vec::new();
    cursor.reserve_candidates(&mut batch).unwrap();
    let mut rows = Vec::new();
    while cursor.next_batch(&mut batch, 1, || false).unwrap() {
        assert_eq!(batch.len(), 1);
        rows.extend_from_slice(&batch);
    }
    assert_eq!(rows.len(), 66);
    assert!(rows[..33].iter().all(|row| row.0 == 0));
    assert!(rows[33..].iter().all(|row| row.0 == 2));
    assert_eq!(rows.iter().map(|row| row.1.1).collect::<Vec<_>>(),
               (0..33).rev().chain((0..33).rev()).collect::<Vec<_>>());
}

#[test]
fn existence_cursor_stops_after_first_accepted_residual_candidate() {
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    table.insert(view(vec![Some(7); 1000])).unwrap();
    let probe = view(vec![Some(7), Some(7), None]);
    let mut cursor = HashProbe::new(&table, &probe, true).unwrap();
    let mut batch = Vec::new();
    let limit = cursor.reserve_candidates(&mut batch).unwrap();
    for round in 0..3 {
        assert!(cursor.next_batch(&mut batch, limit, || false).unwrap());
        assert_eq!(batch.len(), if round == 0 { 2 } else { 1 });
        for &(row, _) in &batch {
            if row == 0 || round == 2 { cursor.accept(row as usize); }
        }
    }
    assert!(!cursor.next_batch(&mut batch, limit, || false).unwrap());
    assert_eq!(cursor.examined, 4, "accepted rows do not visit the remaining duplicate chain");
    assert!(cursor.is_null(2).unwrap());
}

#[test]
fn cursor_honors_downstream_stop_inside_nonmatching_collision_chain() {
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    table.insert(view(vec![Some(7); 1000])).unwrap();
    table.buckets.fill(999);
    let probe = view(vec![Some(9)]);
    let mut cursor = HashProbe::new(&table, &probe, false).unwrap();
    let mut batch = Vec::new();
    let mut checks = 0;
    assert!(!cursor.next_batch(&mut batch, 1, || {
        checks += 1;
        checks > 10
    }).unwrap());
    assert!(cursor.examined < 10);
    assert!(batch.is_empty());
}

#[test]
fn cursor_reservations_release_on_error_and_completion() {
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    table.insert(view(vec![Some(7)])).unwrap();
    let used = table.tracker.used();
    let probe = view(vec![Some(7); 20]);
    {
        let mut cursor = HashProbe::new(&table, &probe, true).unwrap();
        let mut batch = Vec::new();
        cursor.reserve_candidates(&mut batch).unwrap();
        assert!(table.tracker.used() > used);
        assert!(cursor.next_batch(&mut batch, 1, || false).unwrap());
    }
    assert_eq!(table.tracker.used(), used);
}
#[test]
fn probe_workspace_refuses_exhausted_budget_without_retaining_a_reservation() {
    let input = view(vec![Some(7)]);
    let mut reference = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    reference.insert(input.clone()).unwrap();
    let baseline = reference.tracker.used();
    // Hash-table insertion reserves temporary capacity while it grows, so the
    // fixture budget must allow construction before the probe budget is tested.
    let tracker = crate::memory::QueryMemoryTracker::new(
        baseline + 4_096, common::cancellation::CancellationToken::new());
    let mut table = HashTable::with_memory_tracker(
        JoinKeys::new(vec![0]), JoinKeys::new(vec![0]), tracker.clone()).unwrap();
    table.insert(input).unwrap();
    assert_eq!(tracker.used(), baseline);
    let probe = view(vec![Some(7)]);
    let remaining = tracker.limit() - tracker.used();
    let held = tracker.reserve(remaining, "test budget exhaustion").unwrap();
    let exhausted = tracker.used();
    let error = HashProbe::new(&table, &probe, false).err().expect("probe requires workspace");
    assert!(matches!(error, VeinError::ResourceExhausted { .. }));
    assert_eq!(tracker.used(), exhausted, "failed probe must release its reservations");
    drop(held);
    assert_eq!(tracker.used(), baseline);
}

#[test]
fn probe_checks_query_cancellation_and_releases_its_workspace() {
    let cancel = common::cancellation::CancellationToken::new();
    let tracker = crate::memory::QueryMemoryTracker::unlimited(cancel.clone());
    let mut table = HashTable::with_memory_tracker(
        JoinKeys::new(vec![0]), JoinKeys::new(vec![0]), tracker.clone()).unwrap();
    table.insert(view(vec![Some(7); 10])).unwrap();
    let baseline = tracker.used();
    let probe = view(vec![Some(7)]);
    {
        let mut cursor = HashProbe::new(&table, &probe, false).unwrap();
        let mut batch = Vec::new();
        let limit = cursor.reserve_candidates(&mut batch).unwrap();
        cancel.cancel();
        assert!(cursor.next_batch(&mut batch, limit, || false).is_err());
    }
    assert_eq!(tracker.used(), baseline);
}
