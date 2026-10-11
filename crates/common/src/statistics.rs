//! Format-neutral, streaming statistics over borrowed Arrow batches.
//!
//! This module owns algorithms, not file readers. Any source that produces
//! Arrow arrays can feed batches to the collector. Values remain in their input
//! buffers; collection reads borrowed scalars and retains only bounded synopsis
//! state.

use arrow::array::{
    Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
    Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeBinaryArray, LargeStringArray, StringArray, StringViewArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use catalog::{
    ByteLengthStatistics, ColumnStatistics, NumericStatistics, SerializedSketch, TableStatistics,
};
use error::VeinError;
use types::LogicalLiteral;

use crate::Result;

/// Fingerprint a logical schema independently of the physical file format.
///
/// Names, order, nullability, and logical types are part of the identity. A
/// changed schema safely invalidates synopses collected for the prior layout.
pub fn schema_fingerprint(schema: &types::VeinSchema) -> String {
    let mut identity = String::from("vein-logical-schema-v1;");
    for field in &schema.fields {
        identity.push_str(&field.name.len().to_string());
        identity.push(':');
        identity.push_str(&field.name);
        identity.push('|');
        identity.push_str(&format!("{:?}", field.data_type));
        identity.push('|');
        identity.push(if field.nullable { '1' } else { '0' });
        identity.push(';');
    }
    format!("{:016x}", stable_hash(0xfe, identity.as_bytes()))
}

/// Configuration shared by the statistics collectors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatisticsConfig {
    /// HyperLogLog precision. More registers reduce error and increase memory.
    pub distinct_precision: u8,
    /// Maximum string bound retained as an optimizer literal. Longer values
    /// keep length statistics but make the column bounds explicitly unknown.
    pub max_bound_value_bytes: usize,
}

impl Default for StatisticsConfig {
    fn default() -> Self {
        Self {
            distinct_precision: 12,
            max_bound_value_bytes: 4096,
        }
    }
}

/// Estimate and merge contract for bounded distinct-value synopses.
///
/// Implementations can replace HyperLogLog without changing the batch
/// collector or catalog-facing statistics model.
pub trait MergeableDistinctSketch: Clone + Send + Sync + 'static {
    /// Construct a sketch from the validated collector configuration.
    fn with_config(config: &StatisticsConfig) -> Result<Self>;
    /// Add a stable, type-tagged value hash.
    fn update_hash(&mut self, hash: u64);
    /// Merge another compatible sketch.
    fn merge(&mut self, other: &Self) -> Result<()>;
    /// Estimated number of distinct values.
    fn estimate(&self) -> Option<u64>;
    /// Relative standard error for the estimate, if known.
    fn relative_error(&self) -> Option<f64>;
    /// Serialize the bounded state into the format-neutral catalog envelope.
    fn to_serialized(&self) -> SerializedSketch;
    /// Restore state from a catalog envelope.
    fn from_serialized(state: &SerializedSketch) -> Result<Self>;
}

/// HyperLogLog distinct-value sketch with a fixed, persisted hash function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HyperLogLog {
    precision: u8,
    registers: Vec<u8>,
}

impl HyperLogLog {
    const ALGORITHM: &'static str = "hyperloglog";
    const VERSION: u16 = 1;

    fn new(precision: u8) -> Result<Self> {
        if !(4..=16).contains(&precision) {
            return Err(VeinError::internal(format!(
                "distinct sketch precision must be between 4 and 16, got {precision}"
            )));
        }
        Ok(Self {
            precision,
            registers: vec![0; 1_usize << precision],
        })
    }

    fn validate_state(state: &SerializedSketch) -> Result<(u8, &[u8])> {
        if state.algorithm != Self::ALGORITHM || state.version != Self::VERSION {
            return Err(VeinError::internal(format!(
                "unsupported distinct sketch {} version {}",
                state.algorithm, state.version
            )));
        }
        let [precision] = state.parameters.as_slice() else {
            return Err(VeinError::internal("invalid HyperLogLog parameters"));
        };
        if !(4..=16).contains(precision) || state.payload.len() != (1_usize << precision) {
            return Err(VeinError::internal("invalid HyperLogLog register state"));
        }
        if state.payload.iter().any(|register| *register > 64) {
            return Err(VeinError::internal("HyperLogLog register exceeds hash width"));
        }
        Ok((*precision, &state.payload))
    }
}

impl MergeableDistinctSketch for HyperLogLog {
    fn with_config(config: &StatisticsConfig) -> Result<Self> {
        Self::new(config.distinct_precision)
    }

    fn update_hash(&mut self, hash: u64) {
        let index_bits = u32::from(self.precision);
        let index = (hash as usize) & (self.registers.len() - 1);
        let remaining = hash >> index_bits;
        let rank = remaining
            .leading_zeros()
            .saturating_sub(index_bits)
            .saturating_add(1)
            .min(64 - index_bits + 1) as u8;
        self.registers[index] = self.registers[index].max(rank);
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        if self.precision != other.precision || self.registers.len() != other.registers.len() {
            return Err(VeinError::internal(
                "cannot merge HyperLogLog sketches with different precision",
            ));
        }
        for (left, right) in self.registers.iter_mut().zip(&other.registers) {
            *left = (*left).max(*right);
        }
        Ok(())
    }

    fn estimate(&self) -> Option<u64> {
        let m = self.registers.len() as f64;
        if m == 0.0 {
            return Some(0);
        }
        let alpha = match self.registers.len() {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / m),
        };
        let denominator = self
            .registers
            .iter()
            .map(|register| 2.0_f64.powi(-i32::from(*register)))
            .sum::<f64>();
        if denominator <= 0.0 || !denominator.is_finite() {
            return None;
        }
        let raw = alpha * m * m / denominator;
        let zero_registers = self.registers.iter().filter(|value| **value == 0).count();
        let estimate = if raw <= 2.5 * m && zero_registers > 0 {
            m * (m / zero_registers as f64).ln()
        } else {
            raw
        };
        (estimate.is_finite() && estimate >= 0.0)
            .then(|| estimate.round().min(u64::MAX as f64) as u64)
    }

    fn relative_error(&self) -> Option<f64> {
        Some(1.04 / (self.registers.len() as f64).sqrt())
    }

    fn to_serialized(&self) -> SerializedSketch {
        SerializedSketch {
            algorithm: Self::ALGORITHM.to_owned(),
            version: Self::VERSION,
            parameters: vec![self.precision],
            payload: self.registers.clone(),
        }
    }

    fn from_serialized(state: &SerializedSketch) -> Result<Self> {
        let (precision, registers) = Self::validate_state(state)?;
        Ok(Self {
            precision,
            registers: registers.to_vec(),
        })
    }
}

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

fn type_tag(data_type: &DataType) -> Option<u8> {
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

fn canonical_float(value: f64) -> u64 {
    if value == 0.0 {
        0
    } else if value.is_nan() {
        f64::NAN.to_bits()
    } else {
        value.to_bits()
    }
}

fn stable_hash(tag: u8, bytes: &[u8]) -> u64 {
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

#[derive(Clone)]
struct NumericAccumulator {
    finite_count: u64,
    non_finite_count: u64,
    sum: Option<f64>,
    moments: Option<(f64, f64)>,
}

impl Default for NumericAccumulator {
    fn default() -> Self {
        Self { finite_count: 0, non_finite_count: 0, sum: Some(0.0), moments: Some((0.0, 0.0)) }
    }
}

impl NumericAccumulator {
    fn update(&mut self, value: f64) -> Result<()> {
        if !value.is_finite() {
            self.non_finite_count = self.non_finite_count.checked_add(1)
                .ok_or_else(|| VeinError::internal("numeric non-finite count overflow"))?;
            return Ok(());
        }
        let count = self.finite_count.checked_add(1)
            .ok_or_else(|| VeinError::internal("numeric finite count overflow"))?;
        self.sum = self.sum.and_then(|sum| {
            let next = sum + value;
            next.is_finite().then_some(next)
        });
        if let Some((mean, m2)) = self.moments {
            let delta = value - mean;
            let next_mean = mean + delta / count as f64;
            let next_m2 = m2 + delta * (value - next_mean);
            self.moments = (next_mean.is_finite() && next_m2.is_finite() && next_m2 >= 0.0)
                .then_some((next_mean, next_m2));
        }
        self.finite_count = count;
        Ok(())
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        let finite_count = self.finite_count.checked_add(other.finite_count)
            .ok_or_else(|| VeinError::internal("numeric finite count overflow"))?;
        let non_finite_count = self.non_finite_count.checked_add(other.non_finite_count)
            .ok_or_else(|| VeinError::internal("numeric non-finite count overflow"))?;
        self.sum = self.sum.zip(other.sum).and_then(|(left, right)| {
            let sum = left + right;
            sum.is_finite().then_some(sum)
        });
        self.moments = match (self.moments, other.moments) {
            (Some(_), Some(right)) if self.finite_count == 0 => Some(right),
            (Some(left), Some(_)) if other.finite_count == 0 => Some(left),
            (Some((left_mean, left_m2)), Some((right_mean, right_m2))) => {
                let total = finite_count as f64;
                let delta = right_mean - left_mean;
                let mean = left_mean + delta * (other.finite_count as f64 / total);
                let m2 = left_m2 + right_m2
                    + delta * delta * (self.finite_count as f64 * other.finite_count as f64 / total);
                (mean.is_finite() && m2.is_finite() && m2 >= 0.0).then_some((mean, m2))
            }
            _ => None,
        };
        self.finite_count = finite_count;
        self.non_finite_count = non_finite_count;
        Ok(())
    }

    fn restore(summary: &NumericStatistics) -> Self {
        Self {
            finite_count: summary.finite_count,
            non_finite_count: summary.non_finite_count,
            sum: summary.sum,
            moments: if summary.finite_count == 0 {
                Some((0.0, 0.0))
            } else {
                summary.mean.zip(summary.m2)
            },
        }
    }

    fn finish(&self) -> NumericStatistics {
        NumericStatistics {
            finite_count: self.finite_count,
            non_finite_count: self.non_finite_count,
            sum: self.sum,
            mean: (self.finite_count > 0).then(|| self.moments.map(|(mean, _)| mean)).flatten(),
            m2: (self.finite_count > 0).then(|| self.moments.map(|(_, m2)| m2)).flatten(),
        }
    }
}

#[derive(Clone)]
struct ByteLengthAccumulator {
    value_count: u64,
    total_bytes: Option<u64>,
    min_bytes: Option<u64>,
    max_bytes: Option<u64>,
    contains_non_ascii: Option<bool>,
}

impl Default for ByteLengthAccumulator {
    fn default() -> Self {
        Self { value_count: 0, total_bytes: Some(0), min_bytes: None, max_bytes: None, contains_non_ascii: None }
    }
}

impl ByteLengthAccumulator {
    fn for_type(tracks_non_ascii: bool) -> Self {
        Self {
            contains_non_ascii: tracks_non_ascii.then_some(false),
            ..Self::default()
        }
    }
}

impl ByteLengthAccumulator {
    fn update(&mut self, length: u64, contains_non_ascii: Option<bool>) -> Result<()> {
        self.value_count = self.value_count.checked_add(1)
            .ok_or_else(|| VeinError::internal("variable-length value count overflow"))?;
        self.total_bytes = self.total_bytes.and_then(|total| total.checked_add(length));
        self.min_bytes = Some(self.min_bytes.map_or(length, |current| current.min(length)));
        self.max_bytes = Some(self.max_bytes.map_or(length, |current| current.max(length)));
        self.contains_non_ascii = match (self.contains_non_ascii, contains_non_ascii) {
            (Some(previous), Some(current)) => Some(previous || current),
            _ => None,
        };
        Ok(())
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        self.value_count = self.value_count.checked_add(other.value_count)
            .ok_or_else(|| VeinError::internal("variable-length value count overflow"))?;
        self.total_bytes = self.total_bytes.zip(other.total_bytes).and_then(|(left, right)| left.checked_add(right));
        self.min_bytes = match (self.min_bytes, other.min_bytes) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        };
        self.max_bytes = match (self.max_bytes, other.max_bytes) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (left, right) => left.or(right),
        };
        self.contains_non_ascii = match (self.contains_non_ascii, other.contains_non_ascii) {
            (Some(left), Some(right)) => Some(left || right),
            _ => None,
        };
        Ok(())
    }

    fn restore(summary: &ByteLengthStatistics) -> Self {
        Self {
            value_count: summary.value_count,
            total_bytes: summary.total_bytes,
            min_bytes: summary.min_bytes,
            max_bytes: summary.max_bytes,
            contains_non_ascii: summary.contains_non_ascii,
        }
    }

    fn finish(&self) -> ByteLengthStatistics {
        ByteLengthStatistics {
            value_count: self.value_count,
            total_bytes: self.total_bytes,
            min_bytes: self.min_bytes,
            max_bytes: self.max_bytes,
            contains_non_ascii: self.contains_non_ascii,
        }
    }
}

struct ColumnCollector<S> {
    name: String,
    data_type: DataType,
    sketch: Option<S>,
    distinct_complete: bool,
    null_count: u64,
    nan_count: u64,
    min_max_complete: bool,
    min_value: Option<LogicalLiteral>,
    max_value: Option<LogicalLiteral>,
    numeric: Option<NumericAccumulator>,
    byte_length: Option<ByteLengthAccumulator>,
}

/// Streaming statistics for a logical table.
///
/// The collector processes batches incrementally. Its state is proportional to
/// column count and sketch precision, independent of table row count.
pub struct StatisticsCollector<S = HyperLogLog, E = ArrowScalarEncoder> {
    schema: SchemaRef,
    rows: u64,
    columns: Vec<ColumnCollector<S>>,
    config: StatisticsConfig,
    encoder: E,
}

impl<S, E> StatisticsCollector<S, E>
where
    S: MergeableDistinctSketch,
    E: ArrowValueEncoder,
{
    /// Create a collector for an Arrow schema and a pluggable scalar encoder.
    pub fn with_encoder(schema: SchemaRef, config: StatisticsConfig, encoder: E) -> Result<Self> {
        if !(4..=16).contains(&config.distinct_precision) {
            return Err(VeinError::internal(format!(
                "distinct sketch precision must be between 4 and 16, got {}",
                config.distinct_precision
            )));
        }
        let mut columns = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let supported = encoder.supports(field.data_type());
            let bounds_supported = encoder.supports_bounds(field.data_type());
            let numeric = encoder.supports_numeric(field.data_type());
            let byte_length = encoder.supports_byte_lengths(field.data_type());
            columns.push(ColumnCollector {
                name: field.name().clone(),
                data_type: field.data_type().clone(),
                sketch: supported.then(|| S::with_config(&config)).transpose()?,
                distinct_complete: supported,
                null_count: 0,
                nan_count: 0,
                min_max_complete: bounds_supported,
                min_value: None,
                max_value: None,
                numeric: numeric.then(NumericAccumulator::default),
                byte_length: byte_length.then(|| ByteLengthAccumulator::for_type(
                    encoder.supports_non_ascii_tracking(field.data_type()),
                )),
            });
        }
        Ok(Self {
            schema,
            rows: 0,
            columns,
            config,
            encoder,
        })
    }

    /// Add one batch by borrowing its existing Arrow arrays.
    pub fn update(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.schema().as_ref() != self.schema.as_ref() {
            return Err(VeinError::internal(
                "statistics batch schema differs from the collector schema",
            ));
        }
        let next_rows = self
            .rows
            .checked_add(batch.num_rows() as u64)
            .ok_or_else(|| VeinError::internal("statistics row count overflow"))?;

        for (column, array) in self.columns.iter_mut().zip(batch.columns()) {
            if column.data_type != *array.data_type() {
                return Err(VeinError::internal(format!(
                    "statistics type changed for column '{}'",
                    column.name
                )));
            }
            let nulls = u64::try_from(array.null_count())
                .map_err(|_| VeinError::internal("Arrow null count does not fit u64"))?;
            column.null_count = column
                .null_count
                .checked_add(nulls)
                .ok_or_else(|| VeinError::internal("statistics null count overflow"))?;

            for row in 0..array.len() {
                if array.is_null(row) {
                    continue;
                }

                let is_nan = self.encoder.is_nan(array.as_ref(), row);
                if is_nan {
                    column.nan_count = column.nan_count.checked_add(1)
                        .ok_or_else(|| VeinError::internal("statistics NaN count overflow"))?;
                }

                if let Some(sketch) = &mut column.sketch {
                    if column.distinct_complete {
                        if let Some(hash) = self.encoder.hash_value(array.as_ref(), row) {
                            sketch.update_hash(hash);
                        } else {
                            column.distinct_complete = false;
                        }
                    }
                }

                let numeric_value = self.encoder.numeric_value(array.as_ref(), row);
                if let Some(numeric) = &mut column.numeric {
                    if let Some(value) = numeric_value {
                        numeric.update(value)?;
                    } else {
                        column.numeric = None;
                    }
                }

                let byte_length_value = self.encoder.byte_length(array.as_ref(), row);
                if let Some(byte_length) = &mut column.byte_length {
                    if let Some(length) = byte_length_value {
                        byte_length.update(length, self.encoder.contains_non_ascii(array.as_ref(), row))?;
                    } else {
                        column.byte_length = None;
                    }
                }

                if column.min_max_complete && !is_nan {
                    let comparison = match (&column.min_value, &column.max_value) {
                        (None, None) => None,
                        (Some(min), Some(max)) => Some((
                            self.encoder.compare_bound(array.as_ref(), row, min),
                            self.encoder.compare_bound(array.as_ref(), row, max),
                        )),
                        _ => Some((None, None)),
                    };
                    match comparison {
                        None => {
                            let bound = self.encoder.bound_value(
                                array.as_ref(), row, self.config.max_bound_value_bytes,
                            );
                            if let Some(bound) = bound {
                                column.min_value = Some(bound.clone());
                                column.max_value = Some(bound);
                            } else {
                                column.min_max_complete = false;
                            }
                        }
                        Some((Some(min_order), Some(max_order))) => {
                            if min_order.is_lt() {
                                column.min_value = self.encoder.bound_value(
                                    array.as_ref(), row, self.config.max_bound_value_bytes,
                                );
                            }
                            if max_order.is_gt() {
                                column.max_value = self.encoder.bound_value(
                                    array.as_ref(), row, self.config.max_bound_value_bytes,
                                );
                            }
                            if column.min_value.is_none() || column.max_value.is_none() {
                                column.min_max_complete = false;
                            }
                        }
                        Some(_) => column.min_max_complete = false,
                    }
                    if !column.min_max_complete {
                        column.min_value = None;
                        column.max_value = None;
                    }
                }
            }
        }
        self.rows = next_rows;
        Ok(())
    }

    /// Restore a collector from a complete prior table summary for append.
    ///
    /// Unsupported, missing, or malformed synopsis state returns an error so
    /// callers can preserve unknown statistics instead of publishing a partial
    /// table summary as complete.
    pub fn from_statistics(
        schema: SchemaRef,
        config: StatisticsConfig,
        encoder: E,
        statistics: &TableStatistics,
    ) -> Result<Self> {
        let mut collector = Self::with_encoder(schema, config, encoder)?;
        let rows = statistics
            .num_rows
            .ok_or_else(|| VeinError::internal("cannot append to statistics without row count"))?;
        for column in &mut collector.columns {
            let summary = statistics.column_statistics.get(&column.name).ok_or_else(|| {
                VeinError::internal(format!(
                    "cannot append: statistics are missing column '{}'",
                    column.name
                ))
            })?;
            if !summary.is_consistent_with_rows(rows) {
                return Err(VeinError::internal(format!(
                    "cannot append: statistics are inconsistent for column '{}'",
                    column.name
                )));
            }
            if summary.min_max_complete.is_none() {
                return Err(VeinError::internal(format!(
                    "cannot append: min/max completeness is unknown for column '{}'",
                    column.name
                )));
            }
            let nulls = summary.null_count.ok_or_else(|| {
                VeinError::internal(format!(
                    "cannot append: null count is unknown for column '{}'",
                    column.name
                ))
            })?;
            column.null_count = nulls;
            column.nan_count = summary.nan_count.ok_or_else(|| VeinError::internal(format!(
                "cannot append: NaN count is unknown for column '{}'", column.name
            )))?;
            if column.min_max_complete {
                match summary.min_max_complete {
                    Some(true) => match (&summary.min_value, &summary.max_value) {
                        (None, None) => {}
                        (Some(min), Some(max)) => {
                            column.min_value = Some(min.clone());
                            column.max_value = Some(max.clone());
                        }
                        _ => {
                            return Err(VeinError::internal(format!(
                                "cannot append: min/max bounds are inconsistent for column '{}'",
                                column.name
                            )));
                        }
                    },
                    Some(false) => {
                        column.min_max_complete = false;
                        column.min_value = None;
                        column.max_value = None;
                    }
                    None => return Err(VeinError::internal(format!(
                        "cannot append: min/max completeness is unknown for column '{}'",
                        column.name
                    ))),
                }
            }
            if let Some(numeric) = &mut column.numeric {
                let summary = summary.numeric.as_ref().ok_or_else(|| VeinError::internal(format!(
                    "cannot append: numeric statistics are missing for column '{}'", column.name
                )))?;
                *numeric = NumericAccumulator::restore(summary);
            }
            if let Some(byte_length) = &mut column.byte_length {
                let summary = summary.byte_length.as_ref().ok_or_else(|| VeinError::internal(format!(
                    "cannot append: byte-length statistics are missing for column '{}'", column.name
                )))?;
                *byte_length = ByteLengthAccumulator::restore(summary);
            }
            if column.sketch.is_some() {
                let state = summary.distinct_sketch.as_ref().ok_or_else(|| {
                    VeinError::internal(format!(
                        "cannot append: distinct sketch is missing for column '{}'",
                        column.name
                    ))
                })?;
                column.sketch = Some(S::from_serialized(state)?);
                if !column.distinct_complete {
                    return Err(VeinError::internal(format!(
                        "cannot append: distinct synopsis is incomplete for column '{}'",
                        column.name
                    )));
                }
            }
        }
        collector.rows = rows;
        Ok(collector)
    }

    /// Merge two partial collectors built from the same schema/configuration.
    pub fn merge(&mut self, other: &Self) -> Result<()> {
        if self.schema.as_ref() != other.schema.as_ref()
            || self.config != other.config
            || !self.encoder.merge_compatible(&other.encoder)
            || self.columns.len() != other.columns.len()
        {
            return Err(VeinError::internal(
                "cannot merge statistics collected with different schemas or configurations",
            ));
        }
        let rows = self
            .rows
            .checked_add(other.rows)
            .ok_or_else(|| VeinError::internal("statistics row count overflow"))?;
        for (left, right) in self.columns.iter_mut().zip(&other.columns) {
            let left_ordered_values = self.rows.saturating_sub(left.null_count).saturating_sub(left.nan_count);
            let right_ordered_values = other.rows.saturating_sub(right.null_count).saturating_sub(right.nan_count);
            left.null_count = left
                .null_count
                .checked_add(right.null_count)
                .ok_or_else(|| VeinError::internal("statistics null count overflow"))?;
            left.nan_count = left
                .nan_count
                .checked_add(right.nan_count)
                .ok_or_else(|| VeinError::internal("statistics NaN count overflow"))?;
            left.min_max_complete &= right.min_max_complete;
            if left.min_max_complete {
                match (&left.min_value, &left.max_value, &right.min_value, &right.max_value) {
                    (None, None, None, None) if left_ordered_values == 0 && right_ordered_values == 0 => {}
                    (None, None, Some(right_min), Some(right_max)) if left_ordered_values == 0 => {
                        left.min_value = Some(right_min.clone());
                        left.max_value = Some(right_max.clone());
                    }
                    (Some(_), Some(_), None, None) if right_ordered_values == 0 => {}
                    (Some(left_min), Some(left_max), Some(right_min), Some(right_max)) => {
                        let min_order = left_min.try_cmp(right_min);
                        let max_order = left_max.try_cmp(right_max);
                        if let (Some(min_order), Some(max_order)) = (min_order, max_order) {
                            if min_order.is_gt() { left.min_value = Some(right_min.clone()); }
                            if max_order.is_lt() { left.max_value = Some(right_max.clone()); }
                        } else {
                            left.min_max_complete = false;
                            left.min_value = None;
                            left.max_value = None;
                        }
                    }
                    _ => {
                        left.min_max_complete = false;
                        left.min_value = None;
                        left.max_value = None;
                    }
                }
            } else {
                left.min_value = None;
                left.max_value = None;
            }
            match (&mut left.numeric, &right.numeric) {
                (Some(left_numeric), Some(right_numeric)) => left_numeric.merge(right_numeric)?,
                (None, None) => {}
                _ => left.numeric = None,
            }
            match (&mut left.byte_length, &right.byte_length) {
                (Some(left_lengths), Some(right_lengths)) => left_lengths.merge(right_lengths)?,
                (None, None) => {}
                _ => left.byte_length = None,
            }
            left.distinct_complete &= right.distinct_complete;
            match (&mut left.sketch, &right.sketch) {
                (Some(left_sketch), Some(right_sketch)) if left.distinct_complete => {
                    left_sketch.merge(right_sketch)?
                }
                (Some(_), Some(_)) => {}
                (None, None) => {}
                _ => {
                    return Err(VeinError::internal(
                        "cannot merge statistics with different synopsis support",
                    ));
                }
            }
        }
        self.rows = rows;
        Ok(())
    }

    /// Finalize the table summary. Footer adapters may merge their independent
    /// storage facts and bounds into this value.
    pub fn finish(self) -> TableStatistics {
        let mut statistics = TableStatistics {
            num_rows: Some(self.rows),
            ..TableStatistics::default()
        };
        for column in self.columns {
            let sketch = column.sketch.filter(|_| column.distinct_complete);
            let estimate = sketch.as_ref().and_then(MergeableDistinctSketch::estimate)
                .map(|value| value.min(self.rows.saturating_sub(column.null_count)));
            let summary = ColumnStatistics {
                null_count: Some(column.null_count),
                nan_count: Some(column.nan_count),
                distinct_count: estimate,
                distinct_count_relative_error: sketch.as_ref().and_then(MergeableDistinctSketch::relative_error),
                distinct_sketch: sketch.as_ref().map(MergeableDistinctSketch::to_serialized),
                min_value: column.min_value,
                max_value: column.max_value,
                min_max_complete: Some(column.min_max_complete),
                numeric: column.numeric.map(|numeric| numeric.finish()),
                byte_length: column.byte_length.map(|byte_length| byte_length.finish()),
                ..ColumnStatistics::default()
            };
            statistics.column_statistics.insert(column.name, summary);
        }
        statistics
    }
}

impl StatisticsCollector<HyperLogLog, ArrowScalarEncoder> {
    /// Create a collector with Vein's stable Arrow scalar encoder.
    pub fn new(schema: SchemaRef, config: StatisticsConfig) -> Result<Self> {
        Self::with_encoder(schema, config, ArrowScalarEncoder)
    }

    /// Restore a standard collector from a prior summary.
    pub fn restore(
        schema: SchemaRef,
        config: StatisticsConfig,
        statistics: &TableStatistics,
    ) -> Result<Self> {
        Self::from_statistics(schema, config, ArrowScalarEncoder, statistics)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, true),
            Field::new("label", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(1), None])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("a"), None])),
            ],
        )
            .expect("test record batch")
    }

    #[test]
    fn collects_mergeable_table_ndv_and_null_counts_from_borrowed_batches() {
        let batch = batch();
        let input_array = Arc::clone(batch.column(0));
        let input_string_array = Arc::clone(batch.column(1));
        let retained_references = Arc::strong_count(&input_array);
        let retained_string_references = Arc::strong_count(&input_string_array);
        let input_values = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().as_ptr();
        let mut collector = StatisticsCollector::new(
            batch.schema(),
            StatisticsConfig { distinct_precision: 10, ..StatisticsConfig::default() },
        )
            .expect("collector");
        collector.update(&batch).expect("collect input batch");
        let statistics = collector.finish();

        assert_eq!(statistics.num_rows, Some(4));
        assert_eq!(statistics.column_statistics["key"].null_count, Some(1));
        assert_eq!(statistics.column_statistics["key"].distinct_count, Some(2));
        assert_eq!(statistics.column_statistics["key"].distinct_count_relative_error, Some(1.04 / 32.0));
        assert_eq!(statistics.column_statistics["label"].null_count, Some(1));
        assert_eq!(statistics.column_statistics["label"].distinct_count, Some(2));
        assert_eq!(batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().as_ptr(), input_values);
        assert_eq!(Arc::strong_count(&input_array), retained_references);
        assert_eq!(Arc::strong_count(&input_string_array), retained_string_references);
    }

    #[test]
    fn collector_merge_and_restore_preserve_table_wide_ndv() {
        let batch = batch();
        let schema = batch.schema();
        let config = StatisticsConfig { distinct_precision: 10, ..StatisticsConfig::default() };
        let mut left = StatisticsCollector::new(Arc::clone(&schema), config.clone()).unwrap();
        let mut right = StatisticsCollector::new(Arc::clone(&schema), config.clone()).unwrap();
        left.update(&batch.slice(0, 2)).unwrap();
        right.update(&batch.slice(2, 2)).unwrap();
        left.merge(&right).unwrap();

        let merged = left.finish();
        assert_eq!(merged.num_rows, Some(4));
        assert_eq!(merged.column_statistics["key"].distinct_count, Some(2));
        assert_eq!(merged.column_statistics["key"].null_count, Some(1));

        let mut restored = StatisticsCollector::restore(
            Arc::clone(&schema),
            config,
            &merged,
        )
            .expect("restore serialized mergeable state");
        restored.update(&batch.slice(0, 1)).unwrap();
        let appended = restored.finish();
        assert_eq!(appended.num_rows, Some(5));
        assert_eq!(appended.column_statistics["key"].distinct_count, Some(2));
        assert_eq!(appended.column_statistics["key"].min_value, Some(LogicalLiteral::Int64(1)));
        assert_eq!(appended.column_statistics["key"].max_value, Some(LogicalLiteral::Int64(2)));
        assert_eq!(appended.column_statistics["key"].numeric.as_ref().unwrap().finite_count, 4);
        assert_eq!(appended.column_statistics["label"].byte_length.as_ref().unwrap().value_count, 4);
    }

    #[test]
    fn collects_exact_bounds_numeric_moments_and_utf8_lengths() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("measure", DataType::Float64, true),
            Field::new("label", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(vec![
                    Some(3.0), Some(f64::NAN), Some(f64::INFINITY),
                    Some(f64::NEG_INFINITY), None, Some(-2.0), Some(9.0),
                ])),
                Arc::new(StringArray::from(vec![Some("a"), Some("é"), Some("medium"), None, Some(""), None, None])),
            ],
        ).unwrap();
        let mut collector = StatisticsCollector::new(schema, StatisticsConfig::default()).unwrap();
        collector.update(&batch).unwrap();
        let stats = collector.finish();

        let measure = &stats.column_statistics["measure"];
        assert_eq!(measure.null_count, Some(1));
        assert_eq!(measure.nan_count, Some(1));
        assert_eq!(measure.min_value, Some(LogicalLiteral::Float64(f64::NEG_INFINITY)));
        assert_eq!(measure.max_value, Some(LogicalLiteral::Float64(f64::INFINITY)));
        assert_eq!(measure.min_max_complete, Some(true));
        let numeric = measure.numeric.as_ref().unwrap();
        assert_eq!(numeric.finite_count, 3);
        assert_eq!(numeric.non_finite_count, 3);
        assert_eq!(numeric.sum, Some(10.0));
        assert!((numeric.mean.unwrap() - (10.0 / 3.0)).abs() < 1e-12);
        assert!((numeric.m2.unwrap() - (182.0 / 3.0)).abs() < 1e-12);

        let label = &stats.column_statistics["label"];
        assert_eq!(label.min_value, Some(LogicalLiteral::String("".to_owned())));
        assert_eq!(label.max_value, Some(LogicalLiteral::String("é".to_owned())));
        assert_eq!(label.min_max_complete, Some(true));
        assert_eq!(label.byte_length, Some(ByteLengthStatistics {
            value_count: 4,
            total_bytes: Some(9),
            min_bytes: Some(0),
            max_bytes: Some(6),
            contains_non_ascii: Some(true),
        }));
    }

    #[test]
    fn uint64_bounds_preserve_values_above_signed_range() {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::UInt64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt64Array::from(vec![1, i64::MAX as u64 + 1, u64::MAX]))],
        ).unwrap();
        let mut collector = StatisticsCollector::new(schema, StatisticsConfig::default()).unwrap();
        collector.update(&batch).unwrap();
        let summary = collector.finish().column_statistics.remove("key").unwrap();
        assert_eq!(summary.min_value, Some(LogicalLiteral::UInt64(1)));
        assert_eq!(summary.max_value, Some(LogicalLiteral::UInt64(u64::MAX)));
        assert_eq!(summary.min_max_complete, Some(true));
    }

    #[test]
    fn oversized_string_bounds_become_unknown_without_losing_length_stats() {
        let schema = Arc::new(Schema::new(vec![Field::new("label", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["z", "a much longer string"]))],
        ).unwrap();
        let config = StatisticsConfig { max_bound_value_bytes: 3, ..StatisticsConfig::default() };
        let mut collector = StatisticsCollector::new(schema, config).unwrap();
        collector.update(&batch).unwrap();
        let summary = collector.finish().column_statistics.remove("label").unwrap();
        assert_eq!(summary.min_max_complete, Some(false));
        assert_eq!(summary.min_value, None);
        assert_eq!(summary.max_value, None);
        assert_eq!(summary.byte_length.as_ref().unwrap().max_bytes, Some(20));
        assert_eq!(summary.distinct_count, Some(2));
    }

    #[test]
    fn empty_numeric_batches_keep_appendable_moments_and_empty_bounds() {
        let schema = Arc::new(Schema::new(vec![Field::new("measure", DataType::Int64, true)]));
        let empty = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(Vec::<Option<i64>>::new()))],
        ).unwrap();
        let one = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(7)]))],
        ).unwrap();
        let mut collector = StatisticsCollector::new(schema.clone(), StatisticsConfig::default()).unwrap();
        collector.update(&empty).unwrap();
        let empty_stats = collector.finish();
        assert_eq!(empty_stats.column_statistics["measure"].min_max_complete, Some(true));
        assert_eq!(empty_stats.column_statistics["measure"].min_value, None);
        assert_eq!(empty_stats.column_statistics["measure"].numeric.as_ref().unwrap().mean, None);

        let mut restored = StatisticsCollector::restore(schema, StatisticsConfig::default(), &empty_stats).unwrap();
        restored.update(&one).unwrap();
        let stats = restored.finish();
        assert_eq!(stats.column_statistics["measure"].numeric.as_ref().unwrap().mean, Some(7.0));
        assert_eq!(stats.column_statistics["measure"].min_value, Some(LogicalLiteral::Int64(7)));
        assert_eq!(stats.column_statistics["measure"].max_value, Some(LogicalLiteral::Int64(7)));
    }

    #[test]
    fn compatible_sketches_merge_and_round_trip_through_catalog_state() {
        let mut left = HyperLogLog::new(10).expect("sketch");
        let mut right = HyperLogLog::new(10).expect("sketch");
        for value in 0..1_000_u64 {
            left.update_hash(stable_hash(5, &value.to_le_bytes()));
        }
        for value in 1_000..2_000_u64 {
            right.update_hash(stable_hash(5, &value.to_le_bytes()));
        }
        left.merge(&right).expect("merge sketches");
        assert!((1_850..=2_150).contains(&left.estimate().unwrap()));
        let restored = HyperLogLog::from_serialized(&left.to_serialized()).expect("restore");
        assert_eq!(left, restored);
    }

    #[test]
    fn unsupported_arrow_types_keep_counts_but_do_not_publish_a_false_ndv() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "nested",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        )]));
        let batch = RecordBatch::new_empty(schema.clone());
        let mut collector = StatisticsCollector::new(schema, StatisticsConfig::default()).unwrap();
        collector.update(&batch).unwrap();
        let stats = collector.finish();
        assert_eq!(stats.num_rows, Some(0));
        assert_eq!(stats.column_statistics["nested"].null_count, Some(0));
        assert_eq!(stats.column_statistics["nested"].distinct_count, None);
        assert!(stats.column_statistics["nested"].distinct_sketch.is_none());
    }

    #[test]
    fn malformed_and_incompatible_hll_states_are_rejected() {
        let mut state = HyperLogLog::new(10).unwrap().to_serialized();
        state.payload.pop();
        assert!(HyperLogLog::from_serialized(&state).is_err());

        let mut low = HyperLogLog::new(10).unwrap();
        let high = HyperLogLog::new(11).unwrap();
        assert!(low.merge(&high).is_err());
    }
}
