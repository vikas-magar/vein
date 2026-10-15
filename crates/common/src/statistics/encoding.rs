//! Borrowed Arrow scalar encoders used by streaming statistics.

use super::*;

/// Encodes a non-null Arrow scalar without materializing or copying its column.
///
/// A storage adapter may implement this trait for a custom Arrow extension
/// array. Returning None from supports makes NDV explicitly unknown for that
/// column while row and null counts remain available.
pub trait ArrowValueEncoder: Send + Sync {
    /// Whether every non-null value of this Arrow type can be encoded.
    fn supports(&self, data_type: &DataType) -> bool;
    /// Stable hash of one non-null scalar.
    fn hash_value(&self, array: &dyn Array, index: usize) -> Option<u64>;
    /// Whether this scalar is NaN. Non-floating types return false.
    fn is_nan(&self, array: &dyn Array, index: usize) -> bool;
    /// Whether this encoder can produce exact optimizer literals for a type.
    fn supports_bounds(&self, _data_type: &DataType) -> bool { false }
    /// Produce a bounded-size, owned scalar only when a bound changes.
    fn bound_value(
        &self,
        _array: &dyn Array,
        _index: usize,
        _max_string_bytes: usize,
    ) -> Option<LogicalLiteral> { None }
    /// Compare one borrowed value with an existing retained bound without
    /// materializing a per-row scalar. The result is `value.cmp(bound)`.
    fn compare_bound(
        &self,
        _array: &dyn Array,
        _index: usize,
        _bound: &LogicalLiteral,
    ) -> Option<std::cmp::Ordering> { None }
    /// Whether a borrowed scalar equals a retained scalar without copying the
    /// input value. Encoders can override this for unordered scalar values.
    fn equals_bound(
        &self,
        array: &dyn Array,
        index: usize,
        bound: &LogicalLiteral,
    ) -> Option<bool> {
        self.compare_bound(array, index, bound).map(|order| order.is_eq())
    }
    /// Return an approximate finite numeric value for moments.
    fn numeric_value(&self, _array: &dyn Array, _index: usize) -> Option<f64> { None }
    /// Whether numeric moments apply to this logical Arrow type.
    fn supports_numeric(&self, _data_type: &DataType) -> bool { false }
    /// Whether this encoder can measure byte lengths for this type.
    fn supports_byte_lengths(&self, _data_type: &DataType) -> bool { false }
    /// Whether byte-length summaries also have a meaningful ASCII property.
    fn supports_non_ascii_tracking(&self, _data_type: &DataType) -> bool { false }
    /// Return the borrowed value's byte length.
    fn byte_length(&self, _array: &dyn Array, _index: usize) -> Option<u64> { None }
    /// Return whether a borrowed UTF-8 value contains non-ASCII bytes.
    fn contains_non_ascii(&self, _array: &dyn Array, _index: usize) -> Option<bool> { None }
    /// Whether two encoder instances apply identical scalar semantics.
    /// Stateful encoders with different options should override this check.
    fn merge_compatible(&self, _other: &Self) -> bool { true }
}

/// Stable scalar encoder for Arrow primitive, temporal, decimal, and byte types.
#[derive(Debug, Default, Clone, Copy)]
pub struct ArrowScalarEncoder;

impl ArrowValueEncoder for ArrowScalarEncoder {
    fn supports(&self, data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
                | DataType::FixedSizeBinary(_)
                | DataType::Date32
                | DataType::Date64
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Timestamp(_, _)
                | DataType::Duration(_)
                | DataType::Decimal128(_, _)
        )
    }

    fn hash_value(&self, array: &dyn Array, index: usize) -> Option<u64> {
        let tag = type_tag(array.data_type())?;
        macro_rules! primitive {
            ($array:ty, $map:expr) => {
                array
                    .as_any()
                    .downcast_ref::<$array>()
                    .map(|values| $map(values.value(index)))
            };
        }
        match array.data_type() {
            DataType::Boolean => primitive!(BooleanArray, |value: bool| {
                stable_hash(tag, &[u8::from(value)])
            }),
            DataType::Int8 => primitive!(Int8Array, |value: i8| stable_hash(tag, &value.to_le_bytes())),
            DataType::Int16 => primitive!(Int16Array, |value: i16| stable_hash(tag, &value.to_le_bytes())),
            DataType::Int32 => primitive!(Int32Array, |value: i32| stable_hash(tag, &value.to_le_bytes())),
            DataType::Int64 => primitive!(Int64Array, |value: i64| stable_hash(tag, &value.to_le_bytes())),
            DataType::UInt8 => primitive!(UInt8Array, |value: u8| stable_hash(tag, &[value])),
            DataType::UInt16 => primitive!(UInt16Array, |value: u16| stable_hash(tag, &value.to_le_bytes())),
            DataType::UInt32 => primitive!(UInt32Array, |value: u32| stable_hash(tag, &value.to_le_bytes())),
            DataType::UInt64 => primitive!(UInt64Array, |value: u64| stable_hash(tag, &value.to_le_bytes())),
            DataType::Float32 => primitive!(Float32Array, |value: f32| {
                stable_hash(tag, &canonical_float(f64::from(value)).to_le_bytes())
            }),
            DataType::Float64 => primitive!(Float64Array, |value: f64| {
                stable_hash(tag, &canonical_float(value).to_le_bytes())
            }),
            DataType::Utf8 => array.as_any().downcast_ref::<StringArray>()
                .map(|values| stable_hash(tag, values.value(index).as_bytes())),
            DataType::LargeUtf8 => array.as_any().downcast_ref::<LargeStringArray>()
                .map(|values| stable_hash(tag, values.value(index).as_bytes())),
            DataType::Utf8View => array.as_any().downcast_ref::<StringViewArray>()
                .map(|values| stable_hash(tag, values.value(index).as_bytes())),
            DataType::Binary => array.as_any().downcast_ref::<BinaryArray>()
                .map(|values| stable_hash(tag, values.value(index))),
            DataType::LargeBinary => array.as_any().downcast_ref::<LargeBinaryArray>()
                .map(|values| stable_hash(tag, values.value(index))),
            DataType::BinaryView => array.as_any().downcast_ref::<BinaryViewArray>()
                .map(|values| stable_hash(tag, values.value(index))),
            DataType::FixedSizeBinary(_) => array.as_any().downcast_ref::<arrow::array::FixedSizeBinaryArray>()
                .map(|values| stable_hash(tag, values.value(index))),
            DataType::Date32 => primitive!(Date32Array, |value: i32| stable_hash(tag, &value.to_le_bytes())),
            DataType::Date64 => primitive!(Date64Array, |value: i64| stable_hash(tag, &value.to_le_bytes())),
            DataType::Time32(TimeUnit::Second) => primitive!(Time32SecondArray, |value: i32| stable_hash(tag, &value.to_le_bytes())),
            DataType::Time32(TimeUnit::Millisecond) => primitive!(Time32MillisecondArray, |value: i32| stable_hash(tag, &value.to_le_bytes())),
            DataType::Time64(TimeUnit::Microsecond) => primitive!(Time64MicrosecondArray, |value: i64| stable_hash(tag, &value.to_le_bytes())),
            DataType::Time64(TimeUnit::Nanosecond) => primitive!(Time64NanosecondArray, |value: i64| stable_hash(tag, &value.to_le_bytes())),
            DataType::Timestamp(TimeUnit::Second, _) => primitive!(TimestampSecondArray, |value: i64| stable_hash(tag, &value.to_le_bytes())),
            DataType::Timestamp(TimeUnit::Millisecond, _) => primitive!(TimestampMillisecondArray, |value: i64| stable_hash(tag, &value.to_le_bytes())),
            DataType::Timestamp(TimeUnit::Microsecond, _) => primitive!(TimestampMicrosecondArray, |value: i64| stable_hash(tag, &value.to_le_bytes())),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => primitive!(TimestampNanosecondArray, |value: i64| stable_hash(tag, &value.to_le_bytes())),
            DataType::Duration(unit) => hash_duration(array, tag, index, unit),
            DataType::Decimal128(_, _) => primitive!(Decimal128Array, |value: i128| stable_hash(tag, &value.to_le_bytes())),
            _ => None,
        }
    }

    fn is_nan(&self, array: &dyn Array, index: usize) -> bool {
        match array.data_type() {
            DataType::Float32 => array.as_any().downcast_ref::<Float32Array>()
                .is_some_and(|values| values.value(index).is_nan()),
            DataType::Float64 => array.as_any().downcast_ref::<Float64Array>()
                .is_some_and(|values| values.value(index).is_nan()),
            _ => false,
        }
    }

    fn supports_bounds(&self, data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Boolean
                | DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
                | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
                | DataType::Float32 | DataType::Float64
                | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                | DataType::Date32 | DataType::Date64
                | DataType::Time32(_) | DataType::Time64(_) | DataType::Timestamp(_, _)
                | DataType::Decimal128(_, _)
        )
    }

    fn bound_value(
        &self,
        array: &dyn Array,
        index: usize,
        max_string_bytes: usize,
    ) -> Option<LogicalLiteral> {
        macro_rules! map_value {
            ($array:ty, $map:expr) => {
                array.as_any().downcast_ref::<$array>().map(|values| $map(values.value(index)))
            };
        }
        match array.data_type() {
            DataType::Boolean => map_value!(BooleanArray, LogicalLiteral::Boolean),
            DataType::Int8 => map_value!(Int8Array, |value: i8| LogicalLiteral::Int64(i64::from(value))),
            DataType::Int16 => map_value!(Int16Array, |value: i16| LogicalLiteral::Int64(i64::from(value))),
            DataType::Int32 => map_value!(Int32Array, |value: i32| LogicalLiteral::Int64(i64::from(value))),
            DataType::Int64 => map_value!(Int64Array, LogicalLiteral::Int64),
            DataType::UInt8 => map_value!(UInt8Array, |value: u8| LogicalLiteral::Int64(i64::from(value))),
            DataType::UInt16 => map_value!(UInt16Array, |value: u16| LogicalLiteral::Int64(i64::from(value))),
            DataType::UInt32 => map_value!(UInt32Array, |value: u32| LogicalLiteral::Int64(i64::from(value))),
            DataType::UInt64 => map_value!(UInt64Array, LogicalLiteral::UInt64),
            DataType::Float32 => map_value!(Float32Array, |value: f32| LogicalLiteral::Float64(f64::from(value))),
            DataType::Float64 => map_value!(Float64Array, LogicalLiteral::Float64),
            DataType::Utf8 => array.as_any().downcast_ref::<StringArray>().and_then(|values| {
                let value = values.value(index);
                (value.len() <= max_string_bytes).then(|| LogicalLiteral::String(value.to_owned()))
            }),
            DataType::LargeUtf8 => array.as_any().downcast_ref::<LargeStringArray>().and_then(|values| {
                let value = values.value(index);
                (value.len() <= max_string_bytes).then(|| LogicalLiteral::String(value.to_owned()))
            }),
            DataType::Utf8View => array.as_any().downcast_ref::<StringViewArray>().and_then(|values| {
                let value = values.value(index);
                (value.len() <= max_string_bytes).then(|| LogicalLiteral::String(value.to_owned()))
            }),
            DataType::Date32 => map_value!(Date32Array, LogicalLiteral::Date),
            DataType::Date64 => array.as_any().downcast_ref::<Date64Array>()
                .and_then(|values| values.value(index).checked_mul(1_000))
                .map(LogicalLiteral::Timestamp),
            DataType::Time32(TimeUnit::Second) => array.as_any()
                .downcast_ref::<Time32SecondArray>()
                .and_then(|values| i64::from(values.value(index)).checked_mul(1_000_000))
                .map(LogicalLiteral::Time),
            DataType::Time32(TimeUnit::Millisecond) => array.as_any()
                .downcast_ref::<Time32MillisecondArray>()
                .and_then(|values| i64::from(values.value(index)).checked_mul(1_000))
                .map(LogicalLiteral::Time),
            DataType::Time64(TimeUnit::Microsecond) => map_value!(Time64MicrosecondArray, LogicalLiteral::Time),
            DataType::Time64(TimeUnit::Nanosecond) => array.as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .map(|values| values.value(index))
                .filter(|value| value % 1_000 == 0)
                .map(|value| LogicalLiteral::Time(value / 1_000)),
            DataType::Timestamp(unit, _) => timestamp_micros(array, index, unit)
                .map(LogicalLiteral::Timestamp),
            DataType::Decimal128(precision, scale) => array.as_any()
                .downcast_ref::<Decimal128Array>()
                .and_then(|values| u8::try_from(*precision).ok().map(|precision| {
                    LogicalLiteral::Decimal(values.value(index), precision, *scale)
                })),
            _ => None,
        }
    }

    fn compare_bound(
        &self,
        array: &dyn Array,
        index: usize,
        bound: &LogicalLiteral,
    ) -> Option<std::cmp::Ordering> {
        match (array.data_type(), bound) {
            (DataType::Utf8, LogicalLiteral::String(bound)) => Some(
                array.as_any().downcast_ref::<StringArray>()?.value(index).cmp(bound.as_str()),
            ),
            (DataType::LargeUtf8, LogicalLiteral::String(bound)) => Some(
                array.as_any().downcast_ref::<LargeStringArray>()?.value(index).cmp(bound.as_str()),
            ),
            (DataType::Utf8View, LogicalLiteral::String(bound)) => Some(
                array.as_any().downcast_ref::<StringViewArray>()?.value(index).cmp(bound.as_str()),
            ),
            _ => self.bound_value(array, index, 0)?.try_cmp(bound),
        }
    }

    fn numeric_value(&self, array: &dyn Array, index: usize) -> Option<f64> {
        macro_rules! numeric {
            ($array:ty, $map:expr) => {
                array.as_any().downcast_ref::<$array>().map(|values| $map(values.value(index)))
            };
        }
        match array.data_type() {
            DataType::Int8 => numeric!(Int8Array, |value: i8| f64::from(value)),
            DataType::Int16 => numeric!(Int16Array, |value: i16| f64::from(value)),
            DataType::Int32 => numeric!(Int32Array, |value: i32| f64::from(value)),
            DataType::Int64 => numeric!(Int64Array, |value: i64| value as f64),
            DataType::UInt8 => numeric!(UInt8Array, |value: u8| f64::from(value)),
            DataType::UInt16 => numeric!(UInt16Array, |value: u16| f64::from(value)),
            DataType::UInt32 => numeric!(UInt32Array, |value: u32| f64::from(value)),
            DataType::UInt64 => numeric!(UInt64Array, |value: u64| value as f64),
            DataType::Float32 => numeric!(Float32Array, |value: f32| f64::from(value)),
            DataType::Float64 => numeric!(Float64Array, |value: f64| value),
            DataType::Decimal128(_, scale) => numeric!(Decimal128Array, |value: i128| {
                value as f64 / 10_f64.powi(i32::from(*scale))
            }),
            _ => None,
        }
    }

    fn supports_numeric(&self, data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
                | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
                | DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _)
        )
    }

    fn supports_byte_lengths(&self, data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                | DataType::Binary | DataType::LargeBinary | DataType::BinaryView
                | DataType::FixedSizeBinary(_)
        )
    }

    fn supports_non_ascii_tracking(&self, data_type: &DataType) -> bool {
        matches!(data_type, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
    }

    fn byte_length(&self, array: &dyn Array, index: usize) -> Option<u64> {
        let length = match array.data_type() {
            DataType::Utf8 => array.as_any().downcast_ref::<StringArray>()?.value(index).len(),
            DataType::LargeUtf8 => array.as_any().downcast_ref::<LargeStringArray>()?.value(index).len(),
            DataType::Utf8View => array.as_any().downcast_ref::<StringViewArray>()?.value(index).len(),
            DataType::Binary => array.as_any().downcast_ref::<BinaryArray>()?.value(index).len(),
            DataType::LargeBinary => array.as_any().downcast_ref::<LargeBinaryArray>()?.value(index).len(),
            DataType::BinaryView => array.as_any().downcast_ref::<BinaryViewArray>()?.value(index).len(),
            DataType::FixedSizeBinary(_) => array.as_any()
                .downcast_ref::<arrow::array::FixedSizeBinaryArray>()?.value(index).len(),
            _ => return None,
        };
        u64::try_from(length).ok()
    }

    fn contains_non_ascii(&self, array: &dyn Array, index: usize) -> Option<bool> {
        match array.data_type() {
            DataType::Utf8 => Some(!array.as_any().downcast_ref::<StringArray>()?.value(index).is_ascii()),
            DataType::LargeUtf8 => Some(!array.as_any().downcast_ref::<LargeStringArray>()?.value(index).is_ascii()),
            DataType::Utf8View => Some(!array.as_any().downcast_ref::<StringViewArray>()?.value(index).is_ascii()),
            _ => None,
        }
    }
}

fn timestamp_micros(array: &dyn Array, index: usize, unit: &TimeUnit) -> Option<i64> {
    let value = match unit {
        TimeUnit::Second => array.as_any().downcast_ref::<TimestampSecondArray>()?.value(index)
            .checked_mul(1_000_000)?,
        TimeUnit::Millisecond => array.as_any().downcast_ref::<TimestampMillisecondArray>()?.value(index)
            .checked_mul(1_000)?,
        TimeUnit::Microsecond => array.as_any().downcast_ref::<TimestampMicrosecondArray>()?.value(index),
        TimeUnit::Nanosecond => {
            let value = array.as_any().downcast_ref::<TimestampNanosecondArray>()?.value(index);
            if value % 1_000 != 0 { return None; }
            value / 1_000
        }
    };
    Some(value)
}

fn hash_duration(array: &dyn Array, tag: u8, index: usize, unit: &TimeUnit) -> Option<u64> {
    match unit {
        TimeUnit::Second => array.as_any().downcast_ref::<arrow::array::DurationSecondArray>()
            .map(|values| stable_hash(tag, &values.value(index).to_le_bytes())),
        TimeUnit::Millisecond => array.as_any().downcast_ref::<arrow::array::DurationMillisecondArray>()
            .map(|values| stable_hash(tag, &values.value(index).to_le_bytes())),
        TimeUnit::Microsecond => array.as_any().downcast_ref::<arrow::array::DurationMicrosecondArray>()
            .map(|values| stable_hash(tag, &values.value(index).to_le_bytes())),
        TimeUnit::Nanosecond => array.as_any().downcast_ref::<arrow::array::DurationNanosecondArray>()
            .map(|values| stable_hash(tag, &values.value(index).to_le_bytes())),
    }
}

pub(super) fn type_tag(data_type: &DataType) -> Option<u8> {
    Some(match data_type {
        DataType::Boolean => 1,
        DataType::Int8 => 2,
        DataType::Int16 => 3,
        DataType::Int32 => 4,
        DataType::Int64 => 5,
        DataType::UInt8 => 6,
        DataType::UInt16 => 7,
        DataType::UInt32 => 8,
        DataType::UInt64 => 9,
        DataType::Float32 => 10,
        DataType::Float64 => 11,
        DataType::Utf8 => 12,
        DataType::LargeUtf8 => 13,
        DataType::Utf8View => 14,
        DataType::Binary => 15,
        DataType::LargeBinary => 16,
        DataType::BinaryView => 17,
        DataType::FixedSizeBinary(_) => 18,
        DataType::Date32 => 19,
        DataType::Date64 => 20,
        DataType::Time32(TimeUnit::Second) => 21,
        DataType::Time32(TimeUnit::Millisecond) => 22,
        DataType::Time64(TimeUnit::Microsecond) => 23,
        DataType::Time64(TimeUnit::Nanosecond) => 24,
        DataType::Timestamp(TimeUnit::Second, _) => 25,
        DataType::Timestamp(TimeUnit::Millisecond, _) => 26,
        DataType::Timestamp(TimeUnit::Microsecond, _) => 27,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => 28,
        DataType::Duration(TimeUnit::Second) => 29,
        DataType::Duration(TimeUnit::Millisecond) => 30,
        DataType::Duration(TimeUnit::Microsecond) => 31,
        DataType::Duration(TimeUnit::Nanosecond) => 32,
        DataType::Decimal128(_, _) => 33,
        _ => return None,
    })
}

pub(super) fn canonical_float(value: f64) -> u64 {
    if value == 0.0 {
        0
    } else if value.is_nan() {
        f64::NAN.to_bits()
    } else {
        value.to_bits()
    }
}

pub(super) fn stable_hash(tag: u8, bytes: &[u8]) -> u64 {
    // FNV-1a followed by an avalanche finalizer. The exact algorithm is part of
    // the persisted HyperLogLog format, so changing it requires a version bump.
    let mut hash = 0xcbf2_9ce4_8422_2325_u64 ^ u64::from(tag);
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash ^= hash >> 30;
    hash = hash.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash ^= hash >> 27;
    hash = hash.wrapping_mul(0x94d0_49bb_1331_11eb);
    hash ^ (hash >> 31)
}
