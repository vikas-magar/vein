//! Signed integer and decimal group-key coverage.

use super::*;
use std::hash::{BuildHasher, Hasher};

fn fixed_key_plan(key_type: VeinDataType) -> AggregatePlan {
    AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: false,
        aggs: vec![AggOpCode::Sum],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![key_type, VeinDataType::Int64],
    }
}

#[test]
fn int32_group_keys_merge_across_batches_and_preserve_nulls() {
    let mut sink = AggregateSink::new(fixed_key_plan(VeinDataType::Int32)).unwrap();
    sink.accumulate(crate::pipeline::test_support::dense_view(vec![
        Arc::new(Int32Array::from(vec![Some(-2), None, Some(4), Some(-2)])),
        Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
    ]))
        .unwrap();
    sink.accumulate(crate::pipeline::test_support::dense_view(vec![
        Arc::new(Int32Array::from(vec![Some(4), None, Some(9)])),
        Arc::new(Int64Array::from(vec![5, 6, 7])),
    ]))
        .unwrap();

    let output = sink.finish().unwrap();
    let keys = crate::pipeline::test_support::materialized_column::<Int32Array>(&output[0], 0);
    let sums = crate::pipeline::test_support::materialized_column::<Int64Array>(&output[0], 1);
    let actual = (0..keys.len())
        .map(|row| (keys.is_valid(row).then(|| keys.value(row)), sums.value(row)))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(actual, std::collections::BTreeMap::from([(None, 8), (Some(-2), 5), (Some(4), 8), (Some(9), 7)]));
}

#[test]
fn decimal_group_keys_preserve_scale_nulls_and_spill_merge() {
    let tracker = QueryMemoryTracker::new(32 * 1024, common::cancellation::CancellationToken::new());
    let mut sink = AggregateSink::with_memory_tracker(
        fixed_key_plan(VeinDataType::Decimal128(12, 2)),
        tracker,
    )
        .unwrap();
    let rows = 4096_i64;
    let batch_size = 32_i64;
    for start in (0..rows).step_by(batch_size as usize) {
        let end = (start + batch_size).min(rows);
        let key_values = (start..end)
            .map(|row| (row % 2049 != 0).then_some((row % 2049) as i128 * 100))
            .collect::<Vec<_>>();
        let keys = Decimal128Array::from(key_values)
            .with_precision_and_scale(12, 2)
            .unwrap();
        sink.accumulate(crate::pipeline::test_support::dense_view(vec![
            Arc::new(keys),
            Arc::new(Int64Array::from_iter_values(start + 1..=end)),
        ]))
            .unwrap();
    }
    assert!(!sink.spill_runs.is_empty(), "decimal groups should exercise spill");

    let mut observed = std::collections::HashMap::new();
    sink.finish_with(std::num::NonZeroUsize::new(32).unwrap(), |batch| {
        assert_eq!(batch.column_data_type(0).unwrap(), &arrow::datatypes::DataType::Decimal128(12, 2));
        let keys = crate::pipeline::test_support::materialized_column::<Decimal128Array>(&batch, 0);
        let sums = crate::pipeline::test_support::materialized_column::<Int64Array>(&batch, 1);
        for row in 0..batch.num_rows() {
            observed.insert(keys.is_valid(row).then(|| keys.value(row)), sums.value(row));
        }
        Ok(())
    }).unwrap();
    assert_eq!(observed.len(), 2049);
    for (key, sum) in observed {
        let expected = (0..rows)
            .filter(|row| match key {
                None => row % 2049 == 0,
                Some(key) => row % 2049 == (key / 100) as i64,
            })
            .map(|row| row + 1)
            .sum::<i64>();
        assert_eq!(sum, expected, "wrong aggregate for decimal key {key:?}");
    }
}

#[test]
fn decimal256_group_keys_keep_exact_wide_values_through_spill_merge() {
    let high = i256::from(10_i8).checked_pow(40).unwrap();
    let tracker = QueryMemoryTracker::new(64 * 1024, common::cancellation::CancellationToken::new());
    let mut sink = AggregateSink::with_memory_tracker(
        fixed_key_plan(VeinDataType::Decimal256(76, 0)),
        tracker,
    )
        .unwrap();
    let rows = 4096_i64;
    let batch_size = 32_i64;
    for start in (0..rows).step_by(batch_size as usize) {
        let end = (start + batch_size).min(rows);
        let values = (start..end)
            .map(|row| (row % 2049 != 0).then(|| high + i256::from(row % 2049)))
            .collect::<Vec<_>>();
        let keys = Decimal256Array::from(values)
            .with_precision_and_scale(76, 0)
            .unwrap();
        sink.accumulate(crate::pipeline::test_support::dense_view(vec![
            Arc::new(keys),
            Arc::new(Int64Array::from_iter_values(start + 1..=end)),
        ]))
            .unwrap();
    }
    assert!(!sink.spill_runs.is_empty(), "wide decimal groups should spill");
    let mut observed = std::collections::HashMap::new();
    sink.finish_with(std::num::NonZeroUsize::new(32).unwrap(), |batch| {
        assert_eq!(batch.column_data_type(0).unwrap(), &arrow::datatypes::DataType::Decimal256(76, 0));
        let keys = crate::pipeline::test_support::materialized_column::<Decimal256Array>(&batch, 0);
        let sums = crate::pipeline::test_support::materialized_column::<Int64Array>(&batch, 1);
        for row in 0..batch.num_rows() {
            observed.insert(keys.is_valid(row).then(|| keys.value(row)), sums.value(row));
        }
        Ok(())
    }).unwrap();
    assert_eq!(observed.len(), 2049);
    let group_ids = (1..2049_i64)
        .map(|group_id| (high + i256::from(group_id), group_id))
        .collect::<std::collections::HashMap<_, _>>();
    for (key, sum) in observed {
        let expected = (0..rows)
            .filter(|row| match key {
                None => row % 2049 == 0,
                Some(key) => row % 2049 == group_ids[&key],
            })
            .map(|row| row + 1)
            .sum::<i64>();
        assert_eq!(sum, expected, "wrong aggregate for Decimal256 key {key:?}");
    }
}

#[test]
fn new_group_key_layouts_hash_the_same_in_batch_and_spill_paths() {
    for array in [
        Arc::new(Int32Array::from(vec![Some(-7), None, Some(42)])) as ArrayRef,
        Arc::new(Decimal128Array::from(vec![Some(-700_i128), None, Some(4200)])
            .with_precision_and_scale(12, 2).unwrap()) as ArrayRef,
        Arc::new(Decimal256Array::from(vec![Some(i256::from(-7_i8)), None, Some(i256::from(42_i8))])
            .with_precision_and_scale(76, 0).unwrap()) as ArrayRef,
    ] {
        assert_key_hashes_match(&array);
    }
}

fn assert_key_hashes_match(array: &ArrayRef) {
    let key = GroupKeyColumn::new(array).unwrap();
    let selection = SelectionVector::new_dense(array.len());
    let builder = DefaultHashBuilder::default();
    let mut batch_hashers = (0..array.len()).map(|_| builder.build_hasher()).collect::<Vec<_>>();
    key.hash_batch(0, &selection, &[0], 1, &mut batch_hashers);
    for (row, batch_hasher) in batch_hashers.iter().enumerate() {
        let mut spill_hasher = builder.build_hasher();
        key.hash_row(0, row, &mut spill_hasher);
        assert_eq!(spill_hasher.finish(), batch_hasher.finish(), "hash mismatch at row {row}");
    }
}
