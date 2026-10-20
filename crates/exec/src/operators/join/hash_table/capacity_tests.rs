use super::*;
use common::cancellation::CancellationToken;

#[test]
fn empty_hash_table_reserves_only_its_minimum_bucket_state() {
    let tracker = QueryMemoryTracker::new(1_024, CancellationToken::new());
    let table = HashTable::with_memory_tracker(
        JoinKeys::new(vec![0]),
        JoinKeys::new(vec![0]),
        tracker.clone(),
    )
        .expect("minimum hash table state fits a small query budget");

    assert_eq!(table.buckets.len(), MINIMUM_BUCKETS);
    assert!(
        tracker.used() < 1_024,
        "empty hash table should not reserve capacity for a large build side"
    );

    drop(table);
    assert_eq!(tracker.used(), 0, "dropping the table releases its state");
}
