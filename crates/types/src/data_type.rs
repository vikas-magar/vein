//! Data type representation and conversions for Vein.
//!
//! Provides [`VeinDataType`] and strongly-typed, zero-allocation bidirectional
//! conversions to and from Apache Arrow layouts.

use std::sync::Arc;
use arrow::datatypes::{DataType, Field, UnionFields, UnionMode};
use error::{Result, VeinError};
use serde::{Deserialize, Serialize};

use crate::column::ResolvedColumn;
use crate::{IntervalUnit, TimeUnit};

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
    /// Time of day in the Arrow 32-bit time unit.
    Time32(#[serde(with = "time_unit_serde")] TimeUnit),
    /// Time of day in the Arrow 64-bit time unit.
    Time64(#[serde(with = "time_unit_serde")] TimeUnit),
    /// Absolute timestamp in the Arrow time unit with optional timezone.
    Timestamp(#[serde(with = "time_unit_serde")] TimeUnit, Option<String>),
    /// A duration in the given time unit.
    #[serde(with = "time_unit_serde")]
    Duration(TimeUnit),
    /// An interval using the corresponding Arrow physical layout.
    #[serde(with = "interval_unit_serde")]
    Interval(IntervalUnit),

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

/// Physical key representations supported by the hash join implementation.
///
/// Keeping this capability separate from the logical data type lets planning
/// reject unsupported key layouts before execution, while the executor uses
/// the same mapping when resolving Arrow arrays.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HashJoinKeyType {
    /// Signed 64-bit integer key.
    Int64,
    /// UTF-8 string key.
    Utf8,
}

impl VeinDataType {
    /// Returns the physical hash-join key representation for this logical type.
    pub fn hash_join_key_type(&self) -> Option<HashJoinKeyType> {
        match self {
            Self::Int64 => Some(HashJoinKeyType::Int64),
            Self::Utf8 => Some(HashJoinKeyType::Utf8),
            _ => None,
        }
    }

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
            VeinDataType::Time32(unit) => DataType::Time32(*unit),
            VeinDataType::Time64(unit) => DataType::Time64(*unit),
            VeinDataType::Timestamp(unit, tz) => {
                DataType::Timestamp(*unit, tz.as_deref().map(|s| s.into()))
            }
            VeinDataType::Duration(unit) => DataType::Duration(*unit),
            VeinDataType::Interval(unit) => DataType::Interval(*unit),

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

impl TryFrom<&DataType> for VeinDataType {
    type Error = VeinError;

    fn try_from(dt: &DataType) -> Result<Self> {
        Ok(match dt {
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
            DataType::Time32(unit) => VeinDataType::Time32(*unit),
            DataType::Time64(unit) => VeinDataType::Time64(*unit),
            DataType::Timestamp(unit, tz) => {
                VeinDataType::Timestamp(*unit, tz.as_ref().map(|s| s.to_string()))
            }
            DataType::Duration(unit) => VeinDataType::Duration(*unit),
            DataType::Interval(unit) => VeinDataType::Interval(*unit),
            DataType::Decimal128(p, s) => VeinDataType::Decimal128(*p, *s),
            DataType::Decimal256(p, s) => VeinDataType::Decimal256(*p, *s),
            DataType::List(inner) => {
                VeinDataType::List(Arc::new(Self::try_from(inner.data_type())?))
            }
            DataType::LargeList(inner) => {
                VeinDataType::LargeList(Arc::new(Self::try_from(inner.data_type())?))
            }
            DataType::FixedSizeList(inner, size)
            if inner.data_type() == &DataType::Float32 && *size >= 0 =>
                {
                    VeinDataType::Vector(*size as u32)
                }
            DataType::FixedSizeList(inner, size) => {
                VeinDataType::FixedSizeList(
                    Arc::new(Self::try_from(inner.data_type())?),
                    *size,
                )
            }
            DataType::Struct(fields) => {
                let vein_fields = fields
                    .iter()
                    .enumerate()
                    .map(|(i, f)| {
                        Ok(ResolvedColumn::new(
                            crate::column::ColumnId(i),
                            f.name().clone(),
                            None,
                            Self::try_from(f.data_type())?,
                        )
                            .with_nullable(f.is_nullable()))
                    })
                    .collect::<Result<Vec<_>>>()?;
                VeinDataType::Struct(vein_fields.into())
            }
            _ => {
                return Err(VeinError::planning(
                    format!("Arrow data type {dt:?} is not supported by Vein"),
                    None,
                ));
            }
        })
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
            "time32" => Ok(VeinDataType::Time32(TimeUnit::Millisecond)),
            "time64" | "time" => Ok(VeinDataType::Time64(TimeUnit::Microsecond)),
            "timestamp" => Ok(VeinDataType::Timestamp(TimeUnit::Microsecond, None)),
            "variant" => Ok(VeinDataType::Variant),
            _ => Err(VeinError::semantic(
                format!("unknown data type '{s}'"),
                None,
            )),
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

mod interval_unit_serde {
    use arrow::datatypes::IntervalUnit;
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(unit: &IntervalUnit, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let name = match unit {
            IntervalUnit::YearMonth => "YearMonth",
            IntervalUnit::DayTime => "DayTime",
            IntervalUnit::MonthDayNano => "MonthDayNano",
        };
        serializer.serialize_str(name)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<IntervalUnit, D::Error>
    where
        D: Deserializer<'de>,
    {
        let name = String::deserialize(deserializer)?;
        match name.as_str() {
            "YearMonth" => Ok(IntervalUnit::YearMonth),
            "DayTime" => Ok(IntervalUnit::DayTime),
            "MonthDayNano" => Ok(IntervalUnit::MonthDayNano),
            other => Err(serde::de::Error::custom(format!("invalid IntervalUnit: {other}"))),
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
            let back = VeinDataType::try_from(&arrow_dt).expect("supported Arrow type");
            assert_eq!(t, back);
        }
    }

    #[test]
    fn arrow_roundtrip_preserves_temporal_units_and_timezone() {
        let types = [
            VeinDataType::Time32(TimeUnit::Second),
            VeinDataType::Time32(TimeUnit::Millisecond),
            VeinDataType::Time64(TimeUnit::Microsecond),
            VeinDataType::Time64(TimeUnit::Nanosecond),
            VeinDataType::Timestamp(TimeUnit::Second, None),
            VeinDataType::Timestamp(TimeUnit::Millisecond, None),
            VeinDataType::Timestamp(TimeUnit::Microsecond, None),
            VeinDataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            VeinDataType::Interval(IntervalUnit::YearMonth),
            VeinDataType::Interval(IntervalUnit::DayTime),
            VeinDataType::Interval(IntervalUnit::MonthDayNano),
        ];

        for vein_type in types {
            let arrow_type = vein_type.to_arrow();
            assert_eq!(VeinDataType::try_from(&arrow_type).unwrap(), vein_type);
        }
    }

    #[test]
    fn temporal_units_roundtrip_through_serde() {
        let data_types = [
            VeinDataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            VeinDataType::Interval(IntervalUnit::YearMonth),
            VeinDataType::Interval(IntervalUnit::DayTime),
            VeinDataType::Interval(IntervalUnit::MonthDayNano),
        ];
        for data_type in data_types {
            let encoded = serde_json::to_string(&data_type).unwrap();
            let decoded: VeinDataType = serde_json::from_str(&encoded).unwrap();
            assert_eq!(decoded, data_type);
        }
    }

    #[test]
    fn unsupported_arrow_types_return_an_error_instead_of_becoming_utf8() {
        let dictionary = DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::Utf8),
        );

        let error = VeinDataType::try_from(&dictionary).expect_err("dictionary is unsupported");
        assert!(error.to_string().contains("not supported by Vein"));
    }

    #[test]
    fn unsupported_nested_arrow_types_return_an_error() {
        let nested = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        )));

        assert!(VeinDataType::try_from(&nested).is_err());
    }

    #[test]
    fn fixed_size_float32_list_maps_to_vector() {
        let vector = VeinDataType::Vector(3);
        let arrow_type = vector.to_arrow();
        assert_eq!(
            VeinDataType::try_from(&arrow_type).expect("supported vector Arrow type"),
            vector
        );
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
    fn hash_join_key_capabilities_are_explicit_and_shared() {
        assert_eq!(
            VeinDataType::Int64.hash_join_key_type(),
            Some(HashJoinKeyType::Int64)
        );
        assert_eq!(
            VeinDataType::Utf8.hash_join_key_type(),
            Some(HashJoinKeyType::Utf8)
        );

        for unsupported in [
            VeinDataType::Int32,
            VeinDataType::Decimal128(15, 2),
            VeinDataType::Date32,
            VeinDataType::Boolean,
        ] {
            assert_eq!(unsupported.hash_join_key_type(), None, "{unsupported:?}");
        }
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
        assert!("not_a_type".parse::<VeinDataType>().is_err());
    }
}
