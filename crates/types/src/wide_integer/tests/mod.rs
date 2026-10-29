use super::*;
use arrow::array::{Array, Decimal256Array};
use arrow::datatypes::{Field, i256};
use std::collections::HashMap;

#[test]
fn full_native_range_fits_the_declared_interchange_layout() {
    let values = [i128::MIN, -1, 0, 1, i128::MAX];
    let array = Decimal256Array::from(values.map(i256::from).to_vec())
        .with_precision_and_scale(39, 0).unwrap();
    array.to_data().validate_full().unwrap();
    for (value, expected) in array.values().iter().zip(values) {
        assert_eq!(decode(*value).unwrap(), expected);
    }
    assert!(decode(i256::from(i128::MAX).checked_add(i256::ONE).unwrap()).is_err());
    assert!(decode(i256::from(i128::MIN).checked_sub(i256::ONE).unwrap()).is_err());
}

#[test]
fn schema_metadata_distinguishes_int128_from_ordinary_decimal_storage() {
    let logical = VeinDataType::int128();
    let field = logical.to_arrow_field("total", true);
    assert_eq!(field.data_type(), &INT128_STORAGE);
    assert_eq!(VeinDataType::from_arrow_field(&field).unwrap(), logical);
    assert_eq!(VeinDataType::arrow_field_type_name(&field).unwrap(), "Int128");
    let ordinary = Field::new("decimal", INT128_STORAGE, true);
    assert_eq!(VeinDataType::from_arrow_field(&ordinary).unwrap(), VeinDataType::Decimal256(39, 0));
    let invalid = Field::new("invalid", DataType::Decimal128(38, 0), false)
        .with_metadata(HashMap::from([("ARROW:extension:name".into(), INT128_EXTENSION.into())]));
    assert!(VeinDataType::from_arrow_field(&invalid).is_err());
}

#[test]
fn signed_sums_and_integer_coercions_use_the_native_range() {
    for input in [VeinDataType::Int8, VeinDataType::Int16, VeinDataType::Int32, VeinDataType::Int64] {
        assert_eq!(input.sum_result_type(), Some(VeinDataType::int128()));
        assert_eq!(crate::unify_types(&input, &VeinDataType::int128()).unwrap(), VeinDataType::int128());
    }
    assert_eq!(crate::unify_types(&VeinDataType::int128(), &VeinDataType::Float64).unwrap(), VeinDataType::Float64);
    assert_eq!(VeinDataType::int128().sum_result_type(), Some(VeinDataType::int128()));
}

#[test]
fn nested_schema_fields_preserve_native_identity_and_nullability() {
    let native = VeinDataType::int128();
    let cases = [
        VeinDataType::List(Arc::new(native.clone())),
        VeinDataType::LargeList(Arc::new(native.clone())),
        VeinDataType::FixedSizeList(Arc::new(native.clone()), 2),
        VeinDataType::Struct(vec![crate::ResolvedColumn::new(
            crate::ColumnId(0), "total", None, native,
        ).with_nullable(false)].into()),
    ];
    for logical in cases {
        assert_eq!(VeinDataType::try_from(&logical.to_arrow()).unwrap(), logical);
    }
}
