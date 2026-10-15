use super::*;
use arrow::datatypes::TimeUnit;

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
fn decimal_multiplication_matches_arrow_precision_and_scale() {
    assert_eq!(
        decimal_multiply_result_type(
            &VeinDataType::Decimal128(17, 4),
            &VeinDataType::Decimal128(17, 4),
        )
            .unwrap(),
        Some(VeinDataType::Decimal128(35, 8))
    );
    assert_eq!(
        decimal_multiply_result_type(
            &VeinDataType::Decimal256(40, 4),
            &VeinDataType::Decimal256(40, 4),
        )
            .unwrap(),
        Some(VeinDataType::Decimal256(76, 8))
    );
    assert!(decimal_multiply_result_type(
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
