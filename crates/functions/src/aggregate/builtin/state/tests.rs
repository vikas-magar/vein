use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::DataType;
use error::Result;

use crate::{
    AggregateAccumulator, FunctionInput, FunctionInputRow, FunctionInputSegment,
    FunctionInputSource, FunctionRegistry,
};

mod numeric;
mod wide_integer;

struct ArrayInput(ArrayRef);

impl FunctionInputSource for ArrayInput {
    fn data_type(&self) -> &DataType { self.0.data_type() }
    fn is_scalar(&self) -> bool { false }

    fn segment(&self, start: usize, max_len: usize, row_count: usize) -> Result<FunctionInputSegment> {
        let end = start.checked_add(max_len).ok_or_else(|| error::VeinError::internal("test input range overflow"))?;
        if max_len == 0 || end > row_count || row_count != self.0.len() {
            return Err(error::VeinError::internal("test input range is invalid"));
        }
        FunctionInputSegment::new(
            Some(Arc::clone(&self.0).slice(start, max_len)),
            false,
            max_len,
            self.0.data_type().clone(),
        )
    }

    fn source_row(&self, row: usize, row_count: usize) -> Result<Option<FunctionInputRow>> {
        if row_count != self.0.len() || row >= row_count {
            return Err(error::VeinError::internal("test input row is invalid"));
        }
        Ok(Some(FunctionInputRow::new(Arc::clone(&self.0), row)))
    }
}

fn accumulator(name: &str, input_type: DataType) -> (Box<dyn AggregateAccumulator>, DataType) {
    let registry = FunctionRegistry::with_builtins();
    let input_types = [types::VeinDataType::try_from(&input_type).unwrap()];
    let result_type = registry.aggregate_function(name)
        .expect("registered aggregate")
        .return_type(&input_types)
        .expect("infer aggregate result type");
    let state = registry.create_aggregate_accumulator(name, &input_types, &result_type)
        .expect("create registered accumulator");
    (state, result_type.to_arrow())
}

#[test]
fn count_ignores_nulls_and_updates_independent_groups() {
    let input = ArrayInput(Arc::new(Int64Array::from(vec![Some(10), None, Some(30), Some(40)])));
    let groups = [0, 0, 1, 1];
    let mut state = accumulator("count", DataType::Int64).0;
    state.ensure_groups(2).expect("allocate groups");
    state.update_batch(&[FunctionInput::new(&input, groups.len())], &groups)
        .expect("update COUNT");

    let output = state.finalize(0..2).expect("finalize COUNT");
    let counts = output.as_any().downcast_ref::<Int64Array>().expect("Int64 result");
    assert_eq!(counts.values(), &[1, 2]);
}

#[test]
fn combine_and_spill_restore_preserve_sum_state() {
    let left_input = ArrayInput(Arc::new(Int64Array::from(vec![Some(10), Some(20)])));
    let right_input = ArrayInput(Arc::new(Int64Array::from(vec![Some(30)])));
    let mut left = accumulator("sum", DataType::Int64).0;
    let mut right = accumulator("sum", DataType::Int64).0;
    left.ensure_groups(1).expect("allocate left group");
    right.ensure_groups(1).expect("allocate right group");
    left.update_batch(&[FunctionInput::new(&left_input, 2)], &[0, 0]).expect("update left");
    right.update_batch(&[FunctionInput::new(&right_input, 1)], &[0]).expect("update right");
    left.combine_group(0, right.as_ref(), 0).expect("combine partial states");

    let mut bytes = Vec::new();
    left.encode_group(0, &mut bytes).expect("encode spill state");
    let mut restored = accumulator("sum", DataType::Int64).0;
    restored.ensure_groups(1).expect("allocate restored group");
    let mut remaining = bytes.as_slice();
    restored.merge_encoded_group(0, &mut remaining).expect("restore spill state");
    assert!(remaining.is_empty());

    let output = restored.finalize(0..1).expect("finalize restored SUM");
    let sum = output.as_any().downcast_ref::<arrow::array::Decimal256Array>().expect("Int128 storage");
    assert_eq!(types::wide_integer::decode(sum.value(0)).unwrap(), 60);
}

#[test]
fn avg_skips_nulls_and_emits_inferred_float_type() {
    let input = ArrayInput(Arc::new(Int64Array::from(vec![Some(10), None, Some(20)])));
    let mut state = accumulator("avg", DataType::Int64).0;
    state.ensure_groups(1).expect("allocate group");
    state.update_batch(&[FunctionInput::new(&input, 3)], &[0, 0, 0]).expect("update AVG");

    let output = state.finalize(0..1).expect("finalize AVG");
    let avg = output.as_any().downcast_ref::<Float64Array>().expect("Float64 result");
    assert_eq!(avg.value(0), 15.0);
}

#[test]
fn single_value_result_shares_the_input_value_buffer() {
    let input_array: ArrayRef = Arc::new(Int64Array::from(vec![42]));
    let input_values = input_array.as_any().downcast_ref::<Int64Array>().unwrap().values().as_ptr();
    let input = ArrayInput(Arc::clone(&input_array));
    let mut state = accumulator("single_value", DataType::Int64).0;
    state.ensure_groups(1).expect("allocate scalar group");
    state.update_batch(&[FunctionInput::new(&input, 1)], &[0]).expect("update scalar");

    let output = state.finalize(0..1).expect("finalize scalar");
    let value = output.as_any().downcast_ref::<Int64Array>().expect("Int64 result");
    assert_eq!(value.value(0), 42);
    assert_eq!(value.values().as_ptr(), input_values, "final value reuses the source buffer");
}
