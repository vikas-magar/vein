use super::super::metrics::{count_true_rows, ScanPredicateMetrics};
use arrow::array::BooleanArray;
use std::time::Duration;

#[test]
fn scan_predicate_metrics_count_only_true_rows_and_accumulate_batches() {
    let values = BooleanArray::from(vec![Some(true), Some(false), None, Some(true)]);
    assert_eq!(count_true_rows(&values), 2);

    let metrics = ScanPredicateMetrics::default();
    metrics.record_batch(4, 2, Duration::from_micros(7), 128);
    metrics.record_batch(3, 1, Duration::from_micros(5), 64);
    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.batches, 2);
    assert_eq!(snapshot.rows_in, 7);
    assert_eq!(snapshot.rows_kept, 3);
    assert_eq!(snapshot.eval_wall_us, 12);
    assert_eq!(snapshot.input_array_bytes, 192);
}
