use super::*;
use arrow::datatypes::i256;

#[test]
fn normalized_results_share_buffers_and_keep_full_width_null_values() {
    let array = Decimal256Array::from(vec![Some(i256::from(i128::MAX)), None, Some(i256::from(i128::MIN))])
        .with_precision_and_scale(40, 0).unwrap();
    let output = normalize(&array).unwrap();
    let output = output.as_any().downcast_ref::<Decimal256Array>().unwrap();
    assert_eq!(output.values().as_ptr(), array.values().as_ptr());
    assert!(output.is_null(1));
    assert_eq!(output.precision(), 39);
    assert_eq!(output.value(2), i256::from(i128::MIN));
}

#[test]
fn normalization_and_casts_reject_signed_range_overflow() {
    let overflow = i256::from(i128::MAX).checked_add(i256::ONE).unwrap();
    let array = Decimal256Array::from(vec![overflow]).with_precision_and_scale(40, 0).unwrap();
    assert!(normalize(&array).is_err());
    assert!(crate::expression::kernels::eval_cast(&array, &types::VeinDataType::int128()).is_err());
    let fractional = Decimal256Array::from(vec![i256::ONE]).with_precision_and_scale(40, 1).unwrap();
    assert!(normalize(&fractional).is_err());
}
