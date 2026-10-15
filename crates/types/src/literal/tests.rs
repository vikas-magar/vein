use std::sync::Arc;

use super::*;

#[test]
fn time_interval_and_decimal_infer_their_types() {
    assert_eq!(
        literal_type(&LogicalLiteral::Time(1_000)),
        VeinDataType::Time64(TimeUnit::Microsecond)
    );
    assert_eq!(
        literal_type(&LogicalLiteral::Interval {
            months: 1,
            days: 2,
            nanos: 3
        }),
        VeinDataType::Interval(crate::IntervalUnit::MonthDayNano)
    );
    assert_eq!(
        literal_type(&LogicalLiteral::Decimal(123, 76, 20)),
        VeinDataType::Decimal128(76, 20)
    );
}

#[test]
fn compatible_literals_compare_without_lossy_decimal_or_float_handling() {
    assert_eq!(LogicalLiteral::Decimal(125, 5, 2).try_cmp(&LogicalLiteral::Decimal(1250, 6, 3)), Some(Ordering::Equal));
    assert_eq!(LogicalLiteral::Int64(4).try_cmp(&LogicalLiteral::Int64(5)), Some(Ordering::Less));
    assert_eq!(
        LogicalLiteral::UInt64(u64::MAX).try_cmp(&LogicalLiteral::UInt64(u64::MAX - 1)),
        Some(Ordering::Greater)
    );
    assert_eq!(LogicalLiteral::Float64(f64::NAN).try_cmp(&LogicalLiteral::Float64(1.0)), None);
    assert_eq!(LogicalLiteral::Date(1).try_cmp(&LogicalLiteral::Int64(1)), None);
    assert_eq!(literal_type(&LogicalLiteral::UInt64(u64::MAX)), VeinDataType::UInt64);
}

#[test]
fn list_infers_element_type_and_skips_nulls() {
    assert_eq!(
        literal_type(&LogicalLiteral::List(vec![
            LogicalLiteral::Int64(1),
            LogicalLiteral::Int64(2)
        ])),
        VeinDataType::List(Arc::new(VeinDataType::Int64))
    );
    assert_eq!(
        literal_type(&LogicalLiteral::List(vec![
            LogicalLiteral::Null(VeinDataType::Int64),
            LogicalLiteral::String("x".into())
        ])),
        VeinDataType::List(Arc::new(VeinDataType::Utf8))
    );
}

#[test]
fn empty_list_falls_back_to_null() {
    assert_eq!(
        literal_type(&LogicalLiteral::List(vec![])),
        VeinDataType::List(Arc::new(VeinDataType::Null))
    );
}

#[test]
fn struct_infers_named_fields() {
    let ty = literal_type(&LogicalLiteral::Struct(vec![
        ("id".to_string(), LogicalLiteral::Int64(1)),
        ("name".to_string(), LogicalLiteral::String("vein".into())),
    ]));
    let VeinDataType::Struct(fields) = ty else {
        panic!("expected struct");
    };
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0].name, "id");
    assert_eq!(fields[0].data_type, VeinDataType::Int64);
    assert_eq!(fields[1].name, "name");
    assert_eq!(fields[1].data_type, VeinDataType::Utf8);
}

#[test]
fn map_infers_key_and_value_types() {
    assert_eq!(
        literal_type(&LogicalLiteral::Map(vec![(
            LogicalLiteral::String("k".into()),
            LogicalLiteral::Float64(1.5)
        )])),
        VeinDataType::Map(
            Arc::new(VeinDataType::Utf8),
            Arc::new(VeinDataType::Float64)
        )
    );
}

#[test]
fn vector_infers_embedding_dimension() {
    assert_eq!(
        literal_type(&LogicalLiteral::Vector(vec![1.0, 2.0, 3.0, 4.0])),
        VeinDataType::Vector(4)
    );
}

