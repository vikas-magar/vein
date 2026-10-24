use super::*;
use crate::pipeline::{DataChunkView, ViewColumnRef, ViewInputRows};
use arrow::datatypes::i256;

fn mapped(array: ArrayRef, rows: &[Option<usize>]) -> ExpressionValue {
    let source = crate::pipeline::test_support::dense_view(vec![array]);
    let view = DataChunkView::combine(&[ViewInputRows { view: &source, rows }],
                                      &[ViewColumnRef { input: 0, column: 0 }]).unwrap();
    ExpressionValue::view_column(Arc::new(view), 0).unwrap()
}

fn booleans(value: &ExpressionValue, rows: usize) -> Vec<Option<bool>> {
    (0..rows).map(|row| value.source_row(row, rows).unwrap().and_then(|(array, index, _)| {
        let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        array.is_valid(index).then(|| array.value(index))
    })).collect()
}

#[test]
fn fragmented_comparisons_match_arrow_for_all_operators_and_keep_source_buffers() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![Some(i64::MIN), None, Some(3), Some(i64::MAX)])),
        Arc::new(UInt64Array::from(vec![Some(0), None, Some(3), Some(u64::MAX)])),
        Arc::new(Decimal128Array::from(vec![Some(-100), None, Some(2), Some(100)])
            .with_precision_and_scale(12, 2).unwrap()),
        Arc::new(Decimal256Array::from(vec![Some(i256::from(i128::MIN)), None,
                                            Some(i256::from(2)), Some(i256::from(i128::MAX))])
            .with_precision_and_scale(39, 0).unwrap()),
        Arc::new(StringArray::from(vec![Some(""), None, Some("é"), Some("zz")])),
        Arc::new(LargeStringArray::from(vec![Some(""), None, Some("é"), Some("zz")])),
        Arc::new(StringViewArray::from(vec![Some(""), None, Some("é"), Some("zz")])),
        Arc::new(BooleanArray::from(vec![Some(false), None, Some(true), Some(false)])),
        Arc::new(Date32Array::from(vec![Some(-1), None, Some(0), Some(1)])),
        Arc::new(Float64Array::from(vec![Some(f64::NAN), Some(-0.0), Some(0.0), Some(f64::NEG_INFINITY)])),
        Arc::new(Float32Array::from(vec![Some(f32::NAN), Some(-0.0), Some(0.0), Some(f32::INFINITY)])),
    ];
    let operations = [BinaryExprOp::Equal, BinaryExprOp::NotEqual, BinaryExprOp::Less,
        BinaryExprOp::LessEqual, BinaryExprOp::Greater, BinaryExprOp::GreaterEqual];
    for array in arrays {
        let left = mapped(Arc::clone(&array), &[Some(3), Some(0), Some(2), Some(1), None, Some(3)]);
        let right = mapped(Arc::clone(&array), &[Some(0), Some(3), Some(1), Some(2), Some(3), None]);
        for op in operations {
            let expected = super::super::eval_mapped_binary_value(&left, &right, 6, match op {
                BinaryExprOp::Equal => super::super::eval_cmp_eq,
                BinaryExprOp::NotEqual => super::super::eval_cmp_neq,
                BinaryExprOp::Less => super::super::eval_cmp_lt,
                BinaryExprOp::LessEqual => super::super::eval_cmp_lte,
                BinaryExprOp::Greater => super::super::eval_cmp_gt,
                _ => super::super::eval_cmp_gte,
            }).unwrap();
            let actual = evaluate(op, &left, &right, 6).unwrap();
            assert!(matches!(actual, ExpressionValue::Array(_)), "one bitmap batch");
            assert_eq!(booleans(&actual, 6), booleans(&expected, 6));
        }
        assert!(Arc::ptr_eq(left.source_row(0, 6).unwrap().unwrap().0, &array));
    }
}

#[test]
fn scalar_nulls_empty_inputs_and_float_nan_keep_arrow_semantics() {
    let left = mapped(Arc::new(Int64Array::from(vec![1, 2, 3])),
                      &[Some(2), Some(0), Some(2), None]);
    for scalar in [Some(2), None] {
        let right = ExpressionValue::Scalar(Arc::new(Int64Array::from(vec![scalar])));
        let result = evaluate(BinaryExprOp::Greater, &left, &right, 4).unwrap();
        assert_eq!(booleans(&result, 4),
                   if scalar.is_some() { vec![Some(true), Some(false), Some(true), None] } else { vec![None; 4] });
    }
    let floats = mapped(Arc::new(Float64Array::from(vec![f64::NAN, -0.0, 0.0])),
                        &[Some(2), Some(0), Some(1)]);
    let result = evaluate(BinaryExprOp::Equal, &floats, &floats, 3).unwrap();
    let expected = super::super::eval_mapped_binary_value(&floats, &floats, 3,
                                                          super::super::eval_cmp_eq).unwrap();
    assert_eq!(booleans(&result, 3), booleans(&expected, 3));
    let empty = ExpressionValue::Array(Arc::new(Int64Array::from(Vec::<i64>::new())));
    assert_eq!(evaluate(BinaryExprOp::Equal, &empty, &empty, 0).unwrap().physical_len(), 0);
}

#[test]
fn fragmented_in_and_not_in_preserve_null_candidates_duplicates_and_source_identity() {
    let source: ArrayRef = Arc::new(StringArray::from(vec![Some("13"), None, Some("31"), Some("99")]));
    let input = mapped(Arc::clone(&source), &[Some(3), Some(0), Some(2), Some(1), None, Some(0)]);
    let candidates = [
        ExpressionValue::Scalar(Arc::new(StringArray::from(vec![Some("13")]))),
        ExpressionValue::Scalar(Arc::new(StringArray::from(vec![Some("13")]))),
        ExpressionValue::Scalar(Arc::new(StringArray::from(vec![None::<&str>]))),
    ];
    for negated in [false, true] {
        let output = super::super::eval_in_list_value(&input, &candidates, 6, negated).unwrap();
        assert!(matches!(output, ExpressionValue::Array(_)), "one batch, not per-row output arrays");
        assert_eq!(booleans(&output, 6), vec![None, Some(!negated), None, None, None, Some(!negated)]);
    }
    assert!(Arc::ptr_eq(input.source_row(0, 6).unwrap().unwrap().0, &source));
}
