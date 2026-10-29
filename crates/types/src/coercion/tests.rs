use super::*;
use arrow::array::new_null_array;
use arrow::compute::kernels::numeric;
use arrow::datatypes::TimeUnit;

mod multiplication;

#[test]
fn unifies_numeric_types_symmetrically() {
    assert_eq!(
        unify_types(&VeinDataType::Int32, &VeinDataType::Int64).unwrap(),
        VeinDataType::Int64
    );
    assert_eq!(
        unify_types(&VeinDataType::Int64, &VeinDataType::Int32).unwrap(),
        VeinDataType::Int64
    );
}

#[test]
fn unifies_integer_and_decimal_without_losing_range_or_scale() {
    let decimal = VeinDataType::Decimal128(15, 2);
    let expected = VeinDataType::Decimal128(21, 2);

    assert_eq!(
        unify_types(&VeinDataType::Int64, &decimal).unwrap(),
        expected
    );
    assert_eq!(
        unify_types(&decimal, &VeinDataType::Int64).unwrap(),
        expected
    );
    assert_eq!(
        unify_types(&VeinDataType::UInt64, &decimal).unwrap(),
        VeinDataType::Decimal128(22, 2)
    );
}

#[test]
fn mixed_decimal_and_float_unify_to_float64_symmetrically() {
    for decimal in [
        VeinDataType::Decimal128(1, 1),
        VeinDataType::Decimal256(40, 12),
    ] {
        for float in [VeinDataType::Float16, VeinDataType::Float32, VeinDataType::Float64] {
            assert_eq!(unify_types(&decimal, &float).unwrap(), VeinDataType::Float64);
            assert_eq!(unify_types(&float, &decimal).unwrap(), VeinDataType::Float64);
        }
    }
}

#[test]
fn decimal_unification_preserves_widest_scale_and_uses_decimal256_when_needed() {
    assert_eq!(
        unify_types(
            &VeinDataType::Decimal128(12, 3),
            &VeinDataType::Decimal128(15, 5)
        )
            .unwrap(),
        VeinDataType::Decimal128(15, 5)
    );
    assert_eq!(
        unify_types(&VeinDataType::Int64, &VeinDataType::Decimal128(38, 20)).unwrap(),
        VeinDataType::Decimal256(39, 20)
    );
}

#[test]
fn decimal_arithmetic_inference_matches_arrow_kernels() {
    let operators = [
        DecimalArithmeticOp::Add,
        DecimalArithmeticOp::Subtract,
        DecimalArithmeticOp::Multiply,
        DecimalArithmeticOp::Divide,
        DecimalArithmeticOp::Remainder,
    ];
    let operands = [
        (VeinDataType::Decimal128(17, 4), VeinDataType::Decimal128(13, 2)),
        (VeinDataType::Decimal256(42, 8), VeinDataType::Decimal256(42, 8)),
    ];

    for (left_type, right_type) in operands {
        for op in operators {
            let left = new_null_array(&left_type.to_arrow(), 1);
            let right = new_null_array(&right_type.to_arrow(), 1);
            let actual = match op {
                DecimalArithmeticOp::Add => numeric::add(&left, &right),
                DecimalArithmeticOp::Subtract => numeric::sub(&left, &right),
                DecimalArithmeticOp::Multiply => numeric::mul(&left, &right),
                DecimalArithmeticOp::Divide => numeric::div(&left, &right),
                DecimalArithmeticOp::Remainder => numeric::rem(&left, &right),
            }
                .expect("Arrow decimal kernel should infer a result type");
            let expected = decimal_arithmetic_result_type(op, &left_type, &right_type)
                .unwrap()
                .expect("decimal operands should produce a decimal result");
            assert_eq!(actual.data_type(), &expected.to_arrow(), "{op:?}");
        }
    }
    assert_eq!(
        decimal_arithmetic_result_type(
            DecimalArithmeticOp::Divide,
            &VeinDataType::Decimal256(42, 8),
            &VeinDataType::Decimal256(42, 8),
        )
            .unwrap(),
        Some(VeinDataType::Decimal256(54, 12))
    );
    assert!(decimal_arithmetic_result_type(
        DecimalArithmeticOp::Multiply,
        &VeinDataType::Decimal128(38, 20),
        &VeinDataType::Decimal128(38, 20),
    )
        .is_err());
}

#[test]
fn null_unifies_with_other_types() {
    assert_eq!(
        unify_types(&VeinDataType::Utf8, &VeinDataType::Null).unwrap(),
        VeinDataType::Utf8
    );
}

#[test]
fn date_promotes_to_timestamp_and_preserves_timezone() {
    assert_eq!(
        unify_types(
            &VeinDataType::Date32,
            &VeinDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        )
            .unwrap(),
        VeinDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
}

#[test]
fn timestamp_unification_selects_finest_unit_and_requires_matching_timezones() {
    let seconds = VeinDataType::Timestamp(TimeUnit::Second, None);
    let nanos = VeinDataType::Timestamp(TimeUnit::Nanosecond, None);
    let expected = VeinDataType::Timestamp(TimeUnit::Nanosecond, None);

    assert_eq!(unify_types(&seconds, &nanos).unwrap(), expected);
    assert_eq!(unify_types(&nanos, &seconds).unwrap(), expected);
    assert!(unify_types(
        &seconds,
        &VeinDataType::Timestamp(TimeUnit::Second, Some("UTC".into()))
    )
        .is_err());
}

#[test]
fn incompatible_types_return_semantic_error() {
    let error = unify_types(&VeinDataType::Boolean, &VeinDataType::Int64).unwrap_err();
    assert!(matches!(error, VeinError::Semantic { .. }));
}
