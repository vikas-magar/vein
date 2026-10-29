use super::*;
use crate::test_support::DenseInput;
use arrow::array::Decimal256Array;
use arrow::datatypes::i256;
use types::VeinDataType;

fn wide_accumulator(input: VeinDataType) -> Box<dyn AggregateAccumulator> {
    FunctionRegistry::with_builtins().create_aggregate_accumulator(
        "sum", &[input], &VeinDataType::int128()).unwrap()
}

fn values(state: &dyn AggregateAccumulator, groups: Range<usize>) -> Vec<Option<i128>> {
    let array = state.finalize(groups).unwrap();
    array.as_any().downcast_ref::<Decimal256Array>().unwrap().iter()
        .map(|value| value.map(|value| types::wide_integer::decode(value).unwrap())).collect()
}

use std::ops::Range;

#[test]
fn sums_beyond_int64_are_exact_across_worker_combination_and_spill() {
    let input = DenseInput::column(Arc::new(Int64Array::from(vec![Some(i64::MAX), None, Some(i64::MAX)])));
    let mut left = wide_accumulator(VeinDataType::Int64);
    left.ensure_groups(2).unwrap();
    left.update_batch(&[FunctionInput::new(&input, 3)], &[0, 1, 0]).unwrap();
    let mut worker = wide_accumulator(VeinDataType::Int64);
    worker.ensure_groups(1).unwrap();
    worker.update_batch(&[FunctionInput::new(&input, 3)], &[0, 0, 0]).unwrap();
    left.combine_group(0, worker.as_ref(), 0).unwrap();
    assert_eq!(values(left.as_ref(), 0..2), vec![Some(i128::from(i64::MAX) * 4), None]);
    let mut bytes = Vec::new();
    left.encode_group(0, &mut bytes).unwrap();
    let mut restored = wide_accumulator(VeinDataType::Int64);
    restored.ensure_groups(1).unwrap();
    let mut remaining = bytes.as_slice();
    restored.merge_encoded_group(0, &mut remaining).unwrap();
    assert!(remaining.is_empty());
    assert_eq!(values(restored.as_ref(), 0..1), vec![Some(i128::from(i64::MAX) * 4)]);
}

#[test]
fn native_inputs_preserve_extrema_and_reject_overflow() {
    for boundary in [i128::MIN, i128::MAX] {
        let array = Decimal256Array::from(vec![i256::from(boundary)]).with_precision_and_scale(39, 0).unwrap();
        let input = DenseInput::column(Arc::new(array));
        let mut state = wide_accumulator(VeinDataType::int128());
        state.ensure_groups(1).unwrap();
        state.update_batch(&[FunctionInput::new(&input, 1)], &[0]).unwrap();
        assert_eq!(values(state.as_ref(), 0..1), vec![Some(boundary)]);
        let increment = DenseInput::column(Arc::new(Decimal256Array::from(vec![
            i256::from(if boundary < 0 { -1_i128 } else { 1 })
        ]).with_precision_and_scale(39, 0).unwrap()));
        assert!(state.update_batch(&[FunctionInput::new(&increment, 1)], &[0]).is_err());
        assert_eq!(values(state.as_ref(), 0..1), vec![Some(boundary)]);
    }
}

#[test]
fn combine_rejects_native_overflow_without_changing_the_destination() {
    let mut destination = wide_accumulator(VeinDataType::int128());
    let mut worker = wide_accumulator(VeinDataType::int128());
    for (state, value) in [(&mut destination, i128::MAX), (&mut worker, 1)] {
        state.ensure_groups(1).unwrap();
        let input = DenseInput::column(Arc::new(Decimal256Array::from(vec![i256::from(value)])
            .with_precision_and_scale(39, 0).unwrap()));
        state.update_batch(&[FunctionInput::new(&input, 1)], &[0]).unwrap();
    }
    assert!(destination.combine_group(0, worker.as_ref(), 0).is_err());
    assert_eq!(values(destination.as_ref(), 0..1), vec![Some(i128::MAX)]);
}

#[test]
fn ordinary_decimal_storage_does_not_inherit_native_integer_bounds() {
    let registry = FunctionRegistry::with_builtins();
    let input_type = VeinDataType::Decimal256(39, 0);
    let result_type = VeinDataType::Decimal256(76, 0);
    let mut state = registry.create_aggregate_accumulator("sum", &[input_type], &result_type).unwrap();
    state.ensure_groups(1).unwrap();
    let large = i256::from(i128::MAX).checked_add(i256::ONE).unwrap();
    let input = DenseInput::column(Arc::new(Decimal256Array::from(vec![large])
        .with_precision_and_scale(39, 0).unwrap()));
    state.update_batch(&[FunctionInput::new(&input, 1)], &[0]).unwrap();
    let output = state.finalize(0..1).unwrap();
    assert_eq!(output.as_any().downcast_ref::<Decimal256Array>().unwrap().value(0), large);
}
