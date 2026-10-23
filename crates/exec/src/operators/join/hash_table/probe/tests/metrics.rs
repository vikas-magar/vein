use super::*;

#[test]
fn probe_counters_account_for_hashing_candidates_and_null_rows() {
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    table.insert(view(vec![Some(7), Some(7), Some(9)])).unwrap();
    let input = view(vec![Some(7), None, Some(9)]);
    let mut cursor = HashProbe::new(&table, &input, false).unwrap();
    let mut batch = Vec::new();
    cursor.reserve_candidates(&mut batch).unwrap();
    let mut matches = 0;
    while cursor.next_batch(&mut batch, 1, || false).unwrap() {
        matches += batch.len();
    }
    assert_eq!(matches, 3);
    assert_eq!(cursor.metrics.hash_rows, 3);
    assert_eq!(cursor.metrics.bucket_lookups, 2);
    assert_eq!(cursor.metrics.key_comparisons, 3);
    assert_eq!(cursor.metrics.key_rejections, 0);
    assert_eq!(cursor.metrics.key_comparisons + cursor.metrics.hash_rejections, cursor.examined);
    assert!(cursor.metrics.control_polls > 0);
}

#[test]
fn worker_probe_metrics_combine_with_saturating_arithmetic() {
    let mut metrics = ProbeMetrics {
        hash_rows: u64::MAX, bucket_lookups: 4, key_comparisons: 2,
        ..Default::default()
    };
    metrics.accumulate(ProbeMetrics {
        hash_rows: 1, bucket_lookups: 3, key_rejections: 1,
        hash_rejections: 2, control_polls: 7, ..Default::default()
    });
    assert_eq!(metrics.hash_rows, u64::MAX);
    assert_eq!(metrics.bucket_lookups, 7);
    assert_eq!(metrics.key_comparisons, 2);
    assert_eq!(metrics.key_rejections, 1);
    assert_eq!(metrics.hash_rejections, 2);
    assert_eq!(metrics.control_polls, 7);
}
