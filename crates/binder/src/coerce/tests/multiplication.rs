use super::*;

#[test]
fn integer_times_decimal_preserves_fractional_scale_in_both_directions() {
    let decimal = lit(VeinDataType::Decimal128(15, 2));
    let integer = LogicalExpr::Literal(LogicalLiteral::Int64(1));
    let (l, r, t) =
        coerce_arithmetic(decimal.clone(), integer.clone(), BinaryOp::Multiply).unwrap();
    assert_eq!(l, decimal);
    assert_eq!(r.data_type(), VeinDataType::Decimal128(19, 0));
    assert!(matches!(t, VeinDataType::Decimal128(_, 2)));
    let (l2, r2, t2) = coerce_arithmetic(integer, decimal.clone(), BinaryOp::Multiply).unwrap();
    assert_eq!(l2, r);
    assert_eq!(r2, decimal);
    assert_eq!(t2, t);
}

#[test]
fn chained_decimal_multiplication_adds_original_scales() {
    let (_, _, product) = coerce_arithmetic(
        lit(VeinDataType::Decimal128(15, 2)),
        lit(VeinDataType::Decimal128(15, 2)),
        BinaryOp::Multiply,
    )
    .unwrap();
    assert!(matches!(product, VeinDataType::Decimal128(_, 4)));
    let (l, r, total) = coerce_arithmetic(
        lit(product.clone()),
        lit(VeinDataType::Decimal128(15, 2)),
        BinaryOp::Multiply,
    )
    .unwrap();
    assert_eq!(l.data_type(), product);
    assert_eq!(r.data_type(), VeinDataType::Decimal128(15, 2));
    assert!(matches!(total, VeinDataType::Decimal128(_, 6)));
}

#[test]
fn multiplication_keeps_float_promotion_and_addition_keeps_common_scale() {
    let (_, _, float) = coerce_arithmetic(
        lit(VeinDataType::Decimal128(15, 2)),
        lit(VeinDataType::Float64),
        BinaryOp::Multiply,
    )
    .unwrap();
    assert_eq!(float, VeinDataType::Float64);
    let (l, r, _) = coerce_arithmetic(
        lit(VeinDataType::Decimal128(15, 2)),
        lit(VeinDataType::Int64),
        BinaryOp::Plus,
    )
    .unwrap();
    assert_eq!(l.data_type(), r.data_type());
}
