use super::*;
use crate::pipeline::{DataChunkView, ViewColumnRef, ViewInputRows};

fn mapped(array: ArrayRef) -> ExpressionValue {
    mapped_rows(array, &[Some(2), Some(0), None, Some(1), Some(2)])
}

fn mapped_rows(array: ArrayRef, rows: &[Option<usize>]) -> ExpressionValue {
    let source = crate::pipeline::test_support::dense_view(vec![array]);
    let view = DataChunkView::combine(&[ViewInputRows { view: &source, rows }],
                                      &[ViewColumnRef { input: 0, column: 0 }]).unwrap();
    ExpressionValue::view_column(Arc::new(view), 0).unwrap()
}

fn values(value: &ExpressionValue) -> Vec<Option<u64>> {
    (0..5).map(|row| value.source_row(row, 5).unwrap().and_then(|(array, index, _)| {
        let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
        array.is_valid(index).then(|| array.value(index).to_bits())
    })).collect()
}

#[test]
fn numeric_reference_casts_match_arrow_bit_for_bit_without_gathering_inputs() {
    let mut inputs: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![Some(i64::MIN), None, Some(i64::MAX)])),
        Arc::new(UInt64Array::from(vec![Some(0), None, Some(u64::MAX)])),
        Arc::new(Float32Array::from(vec![Some(-0.0), None, Some(f32::INFINITY)])),
        Arc::new(Float64Array::from(vec![Some(f64::NAN), None, Some(-0.0)])),
    ];
    for scale in [-10, -1, 0, 2, 18, 38] {
        inputs.push(Arc::new(Decimal128Array::from(vec![
            Some(-99_999_999_999_999_999_999_999_i128), None,
            Some(99_999_999_999_999_999_999_999_i128),
        ]).with_precision_and_scale(38, scale).unwrap()));
    }
    for array in inputs {
        let input = mapped(Arc::clone(&array));
        let expected = super::super::eval_mapped_unary_value(&input, 5,
                                                             |len| Ok(new_null_array(&DataType::Float64, len)),
                                                             |array| super::super::eval_cast(array, &VeinDataType::Float64)).unwrap();
        let result = evaluate(&input, &VeinDataType::Float64, 5).unwrap();
        assert!(matches!(result, ExpressionValue::Array(_)));
        assert_eq!(values(&result), values(&expected));
        assert!(Arc::ptr_eq(input.source_row(0, 5).unwrap().unwrap().0, &array));
    }
}

#[test]
fn checked_casts_keep_errors_and_do_not_evaluate_unselected_values() {
    let input = mapped(Arc::new(Int64Array::from(vec![Some(1), None, Some(i64::MAX)])));
    assert!(evaluate(&input, &VeinDataType::Int8, 5).is_err());
    let selected = mapped_rows(Arc::new(Int64Array::from(vec![i64::MAX, 1, 2])), &[Some(1), Some(2)]);
    assert!(evaluate(&selected, &VeinDataType::Int8, 2).is_ok(), "unselected overflow must not be evaluated");
    let empty = ExpressionValue::Array(Arc::new(Int64Array::from(Vec::<i64>::new())));
    assert_eq!(evaluate(&empty, &VeinDataType::Float64, 0).unwrap().physical_len(), 0);
    let scalar = ExpressionValue::Scalar(Arc::new(Int64Array::from(vec![1])));
    assert!(evaluate(&scalar, &VeinDataType::Float64, 5).unwrap().is_scalar());
}
