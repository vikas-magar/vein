use super::*;

use arrow::array::{Decimal128Array, Decimal256Array, Float32Array};
use arrow::compute::cast;
use arrow::datatypes::i256;

fn sum_state(input_type: DataType, result_type: DataType) -> Box<dyn AggregateAccumulator> {
    FunctionRegistry::with_builtins()
        .create_aggregate_accumulator("sum", &[input_type], &result_type)
        .expect("create registered SUM accumulator")
}

fn finish_float_sum(input: ArrayRef, input_type: DataType) -> ArrayRef {
    let input = ArrayInput(input);
    let mut state = sum_state(input_type, DataType::Float64);
    state.ensure_groups(1).expect("allocate group");
    state
        .update_batch(&[FunctionInput::new(&input, 2)], &[0, 0])
        .expect("update SUM");
    state.finalize(0..1).expect("finalize SUM")
}

#[test]
fn float16_and_float32_sum_emit_widened_float64_results() {
    let source: ArrayRef = Arc::new(Float32Array::from(vec![1.5, 2.5]));
    let float16 = cast(&source, &DataType::Float16).expect("cast Float16 input");
    let cases = [
        (float16, DataType::Float16),
        (source, DataType::Float32),
    ];

    for (input, input_type) in cases {
        let output = finish_float_sum(input, input_type);
        let sum = output
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("widened Float64 result");
        assert_eq!(sum.value(0), 4.0);
    }
}

#[test]
fn decimal128_sum_enforces_38_digit_precision() {
    let max = 10_i128.checked_pow(38).unwrap() - 1;
    let values = Decimal128Array::from(vec![max, 1])
        .with_precision_and_scale(38, 0)
        .expect("valid Decimal128 input");
    let input = ArrayInput(Arc::new(values));
    let mut state = sum_state(DataType::Decimal128(38, 0), DataType::Decimal128(38, 0));
    state.ensure_groups(1).expect("allocate group");

    let error = state
        .update_batch(&[FunctionInput::new(&input, 2)], &[0, 0])
        .expect_err("38 digit overflow must fail");
    assert!(error.to_string().contains("overflow while computing SUM"));
}

#[test]
fn decimal256_sum_enforces_76_digit_precision() {
    let max = i256::from(10_i8)
        .checked_pow(76)
        .unwrap()
        .checked_sub(i256::ONE)
        .unwrap();
    let values = Decimal256Array::from(vec![max, i256::ONE])
        .with_precision_and_scale(76, 0)
        .expect("valid Decimal256 input");
    let input = ArrayInput(Arc::new(values));
    let mut state = sum_state(DataType::Decimal256(76, 0), DataType::Decimal256(76, 0));
    state.ensure_groups(1).expect("allocate group");

    let error = state
        .update_batch(&[FunctionInput::new(&input, 2)], &[0, 0])
        .expect_err("76 digit overflow must fail");
    assert!(error.to_string().contains("overflow while computing SUM"));
}
