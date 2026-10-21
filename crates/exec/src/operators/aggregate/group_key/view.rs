//! Scalar access to source-backed grouping keys.

use arrow::array::{
    Array, Date32Array, Decimal128Array, Decimal256Array, DictionaryArray, Int8Array,
    Int16Array, Int32Array, Int64Array, StringArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::{DataType, Int32Type};

use super::*;

pub(super) fn value(view: &DataChunkView, row: usize) -> KeyValueRef<'_> {
    let resolved = view
        .resolve(0, row)
        .expect("aggregate group-key view rows are validated");
    let Some((array, row)) = resolved else {
        return KeyValueRef::Null;
    };
    if array.is_null(row) {
        return KeyValueRef::Null;
    }

    let array = array.as_ref();
    if let Some(values) = array.as_any().downcast_ref::<Date32Array>() {
        return KeyValueRef::Date32(values.value(row));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int8Array>() {
        return KeyValueRef::Int64(i64::from(values.value(row)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int16Array>() {
        return KeyValueRef::Int64(i64::from(values.value(row)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
        return KeyValueRef::Int64(i64::from(values.value(row)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        return KeyValueRef::Int64(values.value(row));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt8Array>() {
        return KeyValueRef::UInt64(u64::from(values.value(row)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt16Array>() {
        return KeyValueRef::UInt64(u64::from(values.value(row)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt32Array>() {
        return KeyValueRef::UInt64(u64::from(values.value(row)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
        return KeyValueRef::UInt64(values.value(row));
    }
    if let Some(values) = array.as_any().downcast_ref::<Decimal128Array>() {
        return KeyValueRef::Decimal128(values.value(row));
    }
    if let Some(values) = array.as_any().downcast_ref::<Decimal256Array>() {
        return KeyValueRef::Decimal256(values.value(row));
    }
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return KeyValueRef::Utf8(values.value(row));
    }
    if let Some(values) = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
        let index = values.keys().value(row) as usize;
        let strings = values
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("validated dictionary group key values are Utf8");
        return if strings.is_null(index) {
            KeyValueRef::Null
        } else {
            KeyValueRef::Utf8(strings.value(index))
        };
    }
    unreachable!("unsupported aggregate view key type {:?}", array.data_type())
}

pub(super) fn supports(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Utf8
    ) || matches!(data_type, DataType::Dictionary(key, value)
        if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8)
}
