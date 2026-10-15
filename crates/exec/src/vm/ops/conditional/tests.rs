use super::*;
use arrow::array::{Int64Array, StringArray};

fn array(value: impl Array + 'static) -> RegValue {
    RegValue::Array(Arc::new(value))
}

fn scalar(value: impl Array + 'static) -> RegValue {
    RegValue::Scalar(Arc::new(value))
}

#[test]
fn in_list_obeys_sql_null_and_negation_rules() {
    let values = array(Int64Array::from(vec![Some(1), Some(2), None, Some(4)]));
    let candidates = vec![
        scalar(Int64Array::from(vec![2])),
        scalar(Int64Array::from(vec![3])),
        scalar(Int64Array::from(vec![None])),
    ];

    let result = eval_in_list(&values, &candidates, 4, false).unwrap();
    let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert_eq!(
        result,
        &BooleanArray::from(vec![None, Some(true), None, None])
    );

    let result = eval_in_list(&values, &candidates, 4, true).unwrap();
    let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert_eq!(
        result,
        &BooleanArray::from(vec![None, Some(false), None, None])
    );
}

#[test]
fn empty_in_candidate_set_returns_identity_values() {
    let values = array(Int64Array::from(vec![Some(1), None]));
    let matched = eval_in_list(&values, &[], 2, false).unwrap();
    assert_eq!(
        matched.as_any().downcast_ref::<BooleanArray>().unwrap(),
        &BooleanArray::from(vec![false, false])
    );
    let unmatched = eval_in_list(&values, &[], 2, true).unwrap();
    assert_eq!(
        unmatched.as_any().downcast_ref::<BooleanArray>().unwrap(),
        &BooleanArray::from(vec![true, true])
    );
}

#[test]
fn case_uses_first_true_arm_and_treats_null_when_as_false() {
    let conditions = vec![
        array(BooleanArray::from(vec![Some(true), Some(false), None])),
        array(BooleanArray::from(vec![Some(true), Some(true), Some(true)])),
    ];
    let results = vec![
        array(StringArray::from(vec![
            Some("first"),
            Some("first"),
            Some("first"),
        ])),
        array(StringArray::from(vec![
            Some("second"),
            Some("second"),
            Some("second"),
        ])),
    ];
    let otherwise = scalar(StringArray::from(vec![Some("else")]));

    let result =
        eval_case(&conditions, &results, Some(&otherwise), &DataType::Utf8, 3).unwrap();
    let result = result.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        result,
        &StringArray::from(vec![Some("first"), Some("second"), Some("second")])
    );
}

#[test]
fn case_without_else_returns_null_of_bound_result_type() {
    let conditions = vec![array(BooleanArray::from(vec![false, false]))];
    let results = vec![array(Int64Array::from(vec![10, 20]))];
    let result = eval_case(&conditions, &results, None, &DataType::Int64, 2).unwrap();
    assert_eq!(
        result.as_any().downcast_ref::<Int64Array>().unwrap(),
        &Int64Array::from(vec![None, None])
    );
}

#[test]
fn case_treats_untyped_null_when_as_not_true() {
    let conditions = vec![scalar(arrow::array::NullArray::new(1))];
    let results = vec![scalar(Int64Array::from(vec![7]))];
    let otherwise = scalar(Int64Array::from(vec![9]));
    let result = eval_case(
        &conditions,
        &results,
        Some(&otherwise),
        &DataType::Int64,
        2,
    )
        .unwrap();
    assert_eq!(
        result.as_any().downcast_ref::<Int64Array>().unwrap(),
        &Int64Array::from(vec![9, 9])
    );
}
