use super::*;
use arrow::array::{Array, ArrayRef, Decimal128Array, Decimal256Array};
use std::sync::Arc;

#[test]
fn multiplication_preserves_each_scale_and_matches_arrow_output() {
    for (lp, ls, rp, rs) in [(15, 2, 10, 0), (20, 4, 22, 2), (8, 0, 9, 3), (8, -1, 8, 2)] {
        let left = VeinDataType::Decimal128(lp, ls);
        let right = VeinDataType::Decimal128(rp, rs);
        let (l, r) = decimal_multiplication_operands(&left, &right).unwrap();
        assert_eq!((&l, &r), (&left, &right));
        let a: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(12), None])
                .with_precision_and_scale(lp, ls)
                .unwrap(),
        );
        let b: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(3), Some(4)])
                .with_precision_and_scale(rp, rs)
                .unwrap(),
        );
        let actual = numeric::mul(&a, &b).unwrap();
        assert_eq!(
            decimal_arithmetic_result_type(DecimalArithmeticOp::Multiply, &l, &r)
                .unwrap()
                .unwrap()
                .to_arrow(),
            *actual.data_type()
        );
        let output = actual.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(output.value(0), 36);
        assert!(output.is_null(1));
    }
}

#[test]
fn integers_have_zero_scale_and_mixed_widths_use_decimal256() {
    let decimal = VeinDataType::Decimal128(15, 2);
    for integer in [
        VeinDataType::Int8,
        VeinDataType::Int16,
        VeinDataType::Int32,
        VeinDataType::Int64,
        VeinDataType::UInt64,
    ] {
        let (a, b) = decimal_multiplication_operands(&decimal, &integer).unwrap();
        assert_eq!(a, decimal);
        assert!(matches!(b, VeinDataType::Decimal128(_, 0)));
        assert_eq!(
            decimal_multiplication_operands(&integer, &decimal),
            Some((b, a))
        );
    }
    let a = VeinDataType::Decimal128(15, 2);
    let b = VeinDataType::Decimal256(40, 3);
    let operands = decimal_multiplication_operands(&a, &b).unwrap();
    assert_eq!(operands, (VeinDataType::Decimal256(15, 2), b));
    let a: ArrayRef = Arc::new(
        Decimal256Array::from(vec![arrow::datatypes::i256::from(12)])
            .with_precision_and_scale(15, 2)
            .unwrap(),
    );
    let b: ArrayRef = Arc::new(
        Decimal256Array::from(vec![arrow::datatypes::i256::from(3)])
            .with_precision_and_scale(40, 3)
            .unwrap(),
    );
    assert_eq!(
        *numeric::mul(&a, &b).unwrap().data_type(),
        decimal_arithmetic_result_type(DecimalArithmeticOp::Multiply, &operands.0, &operands.1)
            .unwrap()
            .unwrap()
            .to_arrow()
    );
}

#[test]
fn multiplication_leaves_floats_and_non_decimals_to_common_coercion() {
    for other in [
        VeinDataType::Float32,
        VeinDataType::Float64,
        VeinDataType::Utf8,
    ] {
        assert!(
            decimal_multiplication_operands(&VeinDataType::Decimal128(15, 2), &other).is_none()
        );
    }
    assert!(decimal_multiplication_operands(&VeinDataType::Int64, &VeinDataType::Int64).is_none());
}
