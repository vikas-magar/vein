//! Data type representation and conversions for Vein.
//!
//! Provides [`VeinDataType`] and strongly-typed, zero-allocation bidirectional
//! conversions to and from Apache Arrow layouts.

use std::sync::Arc;
use arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit, UnionFields, UnionMode};
use serde::{Deserialize, Serialize};

use crate::column::ResolvedColumn;

/// Unified data types for the Vein database engine.
///
/// Supports standard SQL primitives, Arrow columnar formats, high-dimensional AI vectors,
/// and semi-structured eBPF network variants.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
)]
pub enum VeinDataType {
    // ── Primitives ────────────────────────────────────────
    /// A Null type.
    Null,
    /// A Boolean type.
    Boolean,
    /// An 8-bit signed integer.
    Int8,
    /// A 16-bit signed integer.
    Int16,
    /// A 32-bit signed integer.
    Int32,
    /// A 64-bit signed integer.
    Int64,
    /// An 8-bit unsigned integer.
    UInt8,
    /// A 16-bit unsigned integer.
    UInt16,
    /// A 32-bit unsigned integer.
    UInt32,
    /// A 64-bit unsigned integer.
    UInt64,
    /// A 16-bit floating point type.
    Float16,
    /// A 32-bit floating point type.
    Float32,
    /// A 64-bit floating point type.
    Float64,

    // ── Strings & Blobs ───────────────────────────────────
    /// A UTF-8 string type.
    Utf8,
    /// A large UTF-8 string type (64-bit offsets).
    LargeUtf8,
    /// Arrow StringView (Zero-copy inline strings).
    StringView,
    /// A variable-length binary blob.
    Binary,
    /// A large variable-length binary blob (64-bit offsets).
    LargeBinary,
    /// Fixed-size binary for hashes and UUIDs.
    FixedSizeBinary(i32),
    /// Arrow BinaryView (Zero-copy inline bytes).
    BinaryView,

    // ── Temporal & Time-Series ────────────────────────────
    /// Days since UNIX epoch.
    Date32,
    /// Milliseconds since UNIX epoch.
    Date64,
    /// Time of day (32-bit).
    Time32,
    /// Time of day (64-bit).
    Time64,
    /// Absolute timestamp with optional timezone.
    Timestamp(Option<String>),
    /// A duration in the given time unit.
    #[serde(with = "time_unit_serde")]
    Duration(TimeUnit),
    /// Time-series interval (Month, Day, Nanosecond).
    Interval,

    // ── Financial / High Precision ────────────────────────
    /// A 128-bit decimal type with precision and scale.
    Decimal128(u8, i8),
    /// A 256-bit decimal type with precision and scale.
    Decimal256(u8, i8),

    // ── Nested & Collections ──────────────────────────────
    /// A variable length list of another type.
    List(Arc<VeinDataType>),
    /// A large (64-bit offset) variable length list.
    LargeList(Arc<VeinDataType>),
    /// Fixed-size list of another type.
    FixedSizeList(Arc<VeinDataType>, i32),
    /// High-dimensional vector for AI embeddings.
    Vector(u32),
    /// A nested struct of named fields.
    Struct(Arc<[ResolvedColumn]>),
    /// A map of key-value pairs.
    Map(
        Arc<VeinDataType>,
        Arc<VeinDataType>,
    ),
    /// A union of mutually exclusive member types.
    Union(Arc<[VeinDataType]>),
    /// A dictionary-encoded (key, value) pair.
    Dictionary(
        Arc<VeinDataType>,
        Arc<VeinDataType>,
    ),
    /// A run-end-encoded (run_end, value) pair.
    RunEndEncoded(
        Arc<VeinDataType>,
        Arc<VeinDataType>,
    ),

    // ── Microservice & Ecosystem Extensions ───────────────
    /// Semi-structured JSON payload targeted for zero-copy eBPF socket extraction.
    Variant,
    /// Custom User-Defined Type (UDT). Name + underlying physical fallback type.
    Extension(String, Arc<VeinDataType>),
}

impl VeinDataType {
    /// Whether this type has a numeric Arrow layout supported by arithmetic.
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            Self::Int8
                | Self::Int16
                | Self::Int32
                | Self::Int64
                | Self::UInt8
                | Self::UInt16
                | Self::UInt32
                | Self::UInt64
                | Self::Float16
                | Self::Float32
                | Self::Float64
                | Self::Decimal128(_, _)
                | Self::Decimal256(_, _)
        )
    }

    /// Result layout selected for `SUM` over this numeric input layout.
    ///
    /// Narrow integers widen to 64 bits, floating point widens to Float64, and
    /// decimals widen precision while preserving their scale.
    pub fn sum_result_type(&self) -> Option<Self> {
        Some(match self {
            Self::Int8 | Self::Int16 | Self::Int32 | Self::Int64 => Self::Int64,
            Self::UInt8 | Self::UInt16 | Self::UInt32 | Self::UInt64 => Self::UInt64,
            Self::Float16 | Self::Float32 | Self::Float64 => Self::Float64,
            Self::Decimal128(_, scale) => Self::Decimal128(38, *scale),
            Self::Decimal256(_, scale) => Self::Decimal256(76, *scale),
            _ => return None,
        })
    }

    /// Converts to Apache Arrow memory layout.
    pub fn to_arrow(&self) -> DataType {
        match self {
            VeinDataType::Null => DataType::Null,
            VeinDataType::Boolean => DataType::Boolean,
            VeinDataType::Int8 => DataType::Int8,
            VeinDataType::Int16 => DataType::Int16,
            VeinDataType::Int32 => DataType::Int32,
            VeinDataType::Int64 => DataType::Int64,
            VeinDataType::UInt8 => DataType::UInt8,
            VeinDataType::UInt16 => DataType::UInt16,
            VeinDataType::UInt32 => DataType::UInt32,
            VeinDataType::UInt64 => DataType::UInt64,
            VeinDataType::Float16 => DataType::Float16,
            VeinDataType::Float32 => DataType::Float32,
            VeinDataType::Float64 => DataType::Float64,

            VeinDataType::Utf8 => DataType::Utf8,
            VeinDataType::LargeUtf8 => DataType::LargeUtf8,
            VeinDataType::StringView => DataType::Utf8View,
            VeinDataType::Binary => DataType::Binary,
            VeinDataType::LargeBinary => DataType::LargeBinary,
            VeinDataType::FixedSizeBinary(size) => DataType::FixedSizeBinary(*size),
            VeinDataType::BinaryView => DataType::BinaryView,

            VeinDataType::Date32 => DataType::Date32,
            VeinDataType::Date64 => DataType::Date64,
            VeinDataType::Time32 => DataType::Time32(TimeUnit::Millisecond),
            VeinDataType::Time64 => DataType::Time64(TimeUnit::Microsecond),
            VeinDataType::Timestamp(tz) => {
                DataType::Timestamp(TimeUnit::Microsecond, tz.as_deref().map(|s| s.into()))
            }
            VeinDataType::Duration(unit) => DataType::Duration(*unit),
            VeinDataType::Interval => DataType::Interval(IntervalUnit::MonthDayNano),

            VeinDataType::Decimal128(p, s) => DataType::Decimal128(*p, *s),
            VeinDataType::Decimal256(p, s) => DataType::Decimal256(*p, *s),

            VeinDataType::List(inner) => {
                DataType::List(Arc::new(Field::new("item", inner.to_arrow(), true)))
            }
            VeinDataType::LargeList(inner) => {
                DataType::LargeList(Arc::new(Field::new("item", inner.to_arrow(), true)))
            }
            VeinDataType::FixedSizeList(inner, size) => {
                DataType::FixedSizeList(Arc::new(Field::new("item", inner.to_arrow(), true)), *size)
            }
            VeinDataType::Vector(dim) => DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                *dim as i32,
            ),
            VeinDataType::Struct(fields) => {
                let arrow_fields = fields
                    .iter()
                    .map(|f| Field::new(&f.name, f.data_type.to_arrow(), true))
                    .collect();
                DataType::Struct(arrow_fields)
            }
            VeinDataType::Map(key, value) => {
                let keys = Field::new("key", key.to_arrow(), false);
                let values = Field::new("value", value.to_arrow(), true);
                let struct_type = DataType::Struct(vec![keys, values].into());
                DataType::Map(Arc::new(Field::new("entries", struct_type, false)), false)
            }
            VeinDataType::Union(members) => {
                let type_ids = (0..members.len() as i8).collect::<Vec<_>>();
                let fields = members
                    .iter()
                    .enumerate()
                    .map(|(i, t)| Field::new(format!("field_{i}"), t.to_arrow(), true))
                    .collect::<Vec<_>>();
                let union_fields =
                    UnionFields::try_new(type_ids, fields).unwrap_or_else(|_| UnionFields::empty());
                DataType::Union(union_fields, UnionMode::Dense)
            }
            VeinDataType::Dictionary(key, value) => {
                DataType::Dictionary(Box::new(key.to_arrow()), Box::new(value.to_arrow()))
            }
            VeinDataType::RunEndEncoded(run_end, value) => DataType::RunEndEncoded(
                Arc::new(Field::new("run_end", run_end.to_arrow(), false)),
                Arc::new(Field::new("values", value.to_arrow(), true)),
            ),

            VeinDataType::Variant => DataType::Utf8View,
            VeinDataType::Extension(_name, fallback) => fallback.to_arrow(),
        }
    }
}

impl From<&DataType> for VeinDataType {
    fn from(dt: &DataType) -> Self {
        match dt {
            DataType::Null => VeinDataType::Null,
            DataType::Boolean => VeinDataType::Boolean,
            DataType::Int8 => VeinDataType::Int8,
            DataType::Int16 => VeinDataType::Int16,
            DataType::Int32 => VeinDataType::Int32,
            DataType::Int64 => VeinDataType::Int64,
            DataType::UInt8 => VeinDataType::UInt8,
            DataType::UInt16 => VeinDataType::UInt16,
            DataType::UInt32 => VeinDataType::UInt32,
            DataType::UInt64 => VeinDataType::UInt64,
            DataType::Float16 => VeinDataType::Float16,
            DataType::Float32 => VeinDataType::Float32,
            DataType::Float64 => VeinDataType::Float64,
            DataType::Utf8 => VeinDataType::Utf8,
            DataType::LargeUtf8 => VeinDataType::LargeUtf8,
            DataType::Utf8View => VeinDataType::StringView,
            DataType::Binary => VeinDataType::Binary,
            DataType::LargeBinary => VeinDataType::LargeBinary,
            DataType::FixedSizeBinary(size) => VeinDataType::FixedSizeBinary(*size),
            DataType::BinaryView => VeinDataType::BinaryView,
            DataType::Date32 => VeinDataType::Date32,
            DataType::Date64 => VeinDataType::Date64,
            DataType::Time32(_) => VeinDataType::Time32,
            DataType::Time64(_) => VeinDataType::Time64,
            DataType::Timestamp(_, tz) => {
                VeinDataType::Timestamp(tz.as_ref().map(|s| s.to_string()))
            }
            DataType::Duration(unit) => VeinDataType::Duration(*unit),
            DataType::Interval(_) => VeinDataType::Interval,
            DataType::Decimal128(p, s) => VeinDataType::Decimal128(*p, *s),
            DataType::Decimal256(p, s) => VeinDataType::Decimal256(*p, *s),
            DataType::List(inner) => VeinDataType::List(Arc::new(Self::from(inner.data_type()))),
            DataType::LargeList(inner) => {
                VeinDataType::LargeList(Arc::new(Self::from(inner.data_type())))
            }
            DataType::FixedSizeList(inner, size) => {
                VeinDataType::FixedSizeList(Arc::new(Self::from(inner.data_type())), *size)
            }
            DataType::Struct(fields) => {
                let vein_fields = fields
                    .iter()
                    .enumerate()
                    .map(|(i, f)| {
                        ResolvedColumn::new(
                            crate::column::ColumnId(i),
                            f.name().clone(),
                            None,
                            Self::from(f.data_type()),
                        )
                    })
                    .collect::<Vec<_>>();
                VeinDataType::Struct(vein_fields.into())
            }
            _ => VeinDataType::Utf8,
        }
    }
}

impl From<&VeinDataType> for DataType {
    fn from(v: &VeinDataType) -> Self {
        v.to_arrow()
    }
}

/// Selects the result type for a supported aggregate from its argument type.
///
/// `fallback` preserves the registry's declared result type for aggregates
/// without arguments and for aggregate rules not specialized here.
pub fn aggregate_return_type(
    name: &str,
    argument_type: Option<&VeinDataType>,
    fallback: VeinDataType,
) -> VeinDataType {
    let Some(argument_type) = argument_type else {
        return fallback;
    };

    if name.eq_ignore_ascii_case("SUM") {
        argument_type.sum_result_type().unwrap_or(fallback)
    } else if (name.eq_ignore_ascii_case("MIN") || name.eq_ignore_ascii_case("MAX"))
        && argument_type.is_numeric()
    {
        argument_type.clone()
    } else {
        fallback
    }
}

impl std::str::FromStr for VeinDataType {
    type Err = error::VeinError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "null" => Ok(VeinDataType::Null),
            "boolean" | "bool" => Ok(VeinDataType::Boolean),
            "int8" | "i8" => Ok(VeinDataType::Int8),
            "int16" | "i16" => Ok(VeinDataType::Int16),
            "int32" | "i32" | "int" | "integer" => Ok(VeinDataType::Int32),
            "int64" | "i64" | "bigint" => Ok(VeinDataType::Int64),
            "uint8" | "u8" => Ok(VeinDataType::UInt8),
            "uint16" | "u16" => Ok(VeinDataType::UInt16),
            "uint32" | "u32" => Ok(VeinDataType::UInt32),
            "uint64" | "u64" => Ok(VeinDataType::UInt64),
            "float16" | "f16" => Ok(VeinDataType::Float16),
            "float32" | "f32" | "float" => Ok(VeinDataType::Float32),
            "float64" | "f64" | "double" => Ok(VeinDataType::Float64),
            "utf8" | "string" | "varchar" | "text" => Ok(VeinDataType::Utf8),
            "largeutf8" => Ok(VeinDataType::LargeUtf8),
            "stringview" | "utf8view" => Ok(VeinDataType::StringView),
            "binary" => Ok(VeinDataType::Binary),
            "largebinary" => Ok(VeinDataType::LargeBinary),
            "binaryview" => Ok(VeinDataType::BinaryView),
            "date32" | "date" => Ok(VeinDataType::Date32),
            "date64" => Ok(VeinDataType::Date64),
            "time32" => Ok(VeinDataType::Time32),
            "time64" | "time" => Ok(VeinDataType::Time64),
            "timestamp" => Ok(VeinDataType::Timestamp(None)),
            "variant" => Ok(VeinDataType::Variant),
            _ => Ok(VeinDataType::Utf8),
        }
    }
}

impl std::fmt::Display for VeinDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

mod time_unit_serde {
    use arrow::datatypes::TimeUnit;
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(unit: &TimeUnit, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = match unit {
            TimeUnit::Second => "Second",
            TimeUnit::Millisecond => "Millisecond",
            TimeUnit::Microsecond => "Microsecond",
            TimeUnit::Nanosecond => "Nanosecond",
        };
        serializer.serialize_str(s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<TimeUnit, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "Second" => Ok(TimeUnit::Second),
            "Millisecond" => Ok(TimeUnit::Millisecond),
            "Microsecond" => Ok(TimeUnit::Microsecond),
            "Nanosecond" => Ok(TimeUnit::Nanosecond),
            other => Err(serde::de::Error::custom(format!("invalid TimeUnit: {other}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serde_roundtrip() {
        let t = VeinDataType::Duration(TimeUnit::Millisecond);
        let serialized = serde_json::to_string(&t).unwrap();
        let deserialized: VeinDataType = serde_json::from_str(&serialized).unwrap();
        assert_eq!(t, deserialized);
    }

    #[test]
    fn test_arrow_roundtrip_primitives() {
        let types = vec![
            VeinDataType::Int8,
            VeinDataType::Int16,
            VeinDataType::Int32,
            VeinDataType::Int64,
            VeinDataType::UInt8,
            VeinDataType::UInt16,
            VeinDataType::UInt32,
            VeinDataType::UInt64,
            VeinDataType::Float16,
            VeinDataType::Float32,
            VeinDataType::Float64,
            VeinDataType::Decimal128(38, 4),
            VeinDataType::Decimal256(76, 4),
            VeinDataType::Utf8,
            VeinDataType::Boolean,
            VeinDataType::Date32,
            VeinDataType::Date64,
        ];

        for t in types {
            let arrow_dt: DataType = (&t).into();
            let back: VeinDataType = (&arrow_dt).into();
            assert_eq!(t, back);
        }
    }

    #[test]
    fn numeric_aggregate_types_are_centralized() {
        let cases = [
            (VeinDataType::Int8, VeinDataType::Int64),
            (VeinDataType::Int16, VeinDataType::Int64),
            (VeinDataType::Int32, VeinDataType::Int64),
            (VeinDataType::Int64, VeinDataType::Int64),
            (VeinDataType::UInt8, VeinDataType::UInt64),
            (VeinDataType::UInt16, VeinDataType::UInt64),
            (VeinDataType::UInt32, VeinDataType::UInt64),
            (VeinDataType::UInt64, VeinDataType::UInt64),
            (VeinDataType::Float16, VeinDataType::Float64),
            (VeinDataType::Float32, VeinDataType::Float64),
            (VeinDataType::Float64, VeinDataType::Float64),
            (VeinDataType::Decimal128(12, 3), VeinDataType::Decimal128(38, 3)),
            (VeinDataType::Decimal256(20, 3), VeinDataType::Decimal256(76, 3)),
        ];

        for (input, expected_sum) in cases {
            assert!(input.is_numeric());
            assert_eq!(input.sum_result_type(), Some(expected_sum));
        }
        assert!(!VeinDataType::Utf8.is_numeric());
        assert_eq!(VeinDataType::Utf8.sum_result_type(), None);
    }

    #[test]
    fn aggregate_return_types_use_argument_layout_or_fallback() {
        let fallback = VeinDataType::Float64;
        assert_eq!(
            aggregate_return_type("sum", Some(&VeinDataType::Int32), fallback.clone()),
            VeinDataType::Int64
        );
        assert_eq!(
            aggregate_return_type("MIN", Some(&VeinDataType::Decimal128(15, 2)), fallback.clone()),
            VeinDataType::Decimal128(15, 2)
        );
        assert_eq!(
            aggregate_return_type("max", Some(&VeinDataType::Int16), fallback.clone()),
            VeinDataType::Int16
        );
        assert_eq!(
            aggregate_return_type("sum", Some(&VeinDataType::Utf8), fallback.clone()),
            fallback
        );
        assert_eq!(
            aggregate_return_type("count", Some(&VeinDataType::Int64), fallback.clone()),
            fallback
        );
        assert_eq!(aggregate_return_type("sum", None, fallback.clone()), fallback);
    }

    #[test]
    fn test_string_parsing() {
        assert_eq!("int32".parse::<VeinDataType>().unwrap(), VeinDataType::Int32);
        assert_eq!("INT64".parse::<VeinDataType>().unwrap(), VeinDataType::Int64);
        assert_eq!("utf8".parse::<VeinDataType>().unwrap(), VeinDataType::Utf8);
        assert_eq!("float64".parse::<VeinDataType>().unwrap(), VeinDataType::Float64);
    }
}
