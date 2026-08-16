use crate::schema::column::ResolvedColumn;
use arrow::datatypes::TimeUnit;

/// Rkyv-safe mirror of Arrow types to enable zero-overhead IPC over QUIC.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum VeinDataType {
    // ── Primitives ────────────────────────────────────────
    /// A Null type
    Null,
    /// A Boolean type
    Boolean,
    /// An 8-bit signed integer
    Int8,
    /// A 16-bit signed integer
    Int16,
    /// A 32-bit signed integer
    Int32,
    /// A 64-bit signed integer
    Int64,
    /// An 8-bit unsigned integer
    UInt8,
    /// A 16-bit unsigned integer
    UInt16,
    /// A 32-bit unsigned integer
    UInt32,
    /// A 64-bit unsigned integer
    UInt64,
    /// A 16-bit floating point type (Half precision, ideal for ML)
    Float16,
    /// A 32-bit floating point type
    Float32,
    /// A 64-bit floating point type
    Float64,

    // ── Strings & Blobs ───────────────────────────────────
    /// A UTF-8 string type
    Utf8,
    /// A large UTF-8 string type (64-bit offsets)
    LargeUtf8,
    /// Arrow StringView (Zero-copy inline strings - "German-style")
    StringView,
    /// A variable-length binary blob
    Binary,
    /// A large variable-length binary blob (64-bit offsets)
    LargeBinary,
    /// Fixed size binary for hashes/UUIDs
    FixedSizeBinary(i32),
    /// Arrow BinaryView (Zero-copy inline bytes)
    BinaryView,

    // ── Temporal & Time-Series ────────────────────────────
    /// Days since UNIX epoch
    Date32,
    /// Milliseconds since UNIX epoch (higher-precision date)
    Date64,
    /// Time of day (Seconds/Milliseconds, 32-bit)
    Time32,
    /// Time of day (Microseconds, 64-bit)
    Time64,
    /// Absolute timestamp with optional timezone
    Timestamp(Option<String>),
    /// A duration in the given time unit
    Duration(TimeUnit),
    /// Time-series interval (Month, Day, Nanosecond)
    Interval,

    // ── Financial / High Precision ────────────────────────
    /// A 128-bit decimal type with precision and scale
    Decimal128(u8, i8),
    /// A 256-bit decimal type with precision and scale
    Decimal256(u8, i8),

    // ── Nested & Collections ──────────────────────────────
    /// A variable length list of another type
    List(Box<VeinDataType>),
    /// A large (64-bit offset) variable length list
    LargeList(Box<VeinDataType>),
    /// Fixed size list
    FixedSizeList(Box<VeinDataType>, i32),
    /// High-dimensional vector for AI embeddings
    Vector(u32),
    /// A nested struct of named fields
    Struct(Vec<ResolvedColumn>),
    /// A map of key-value pairs
    Map(Box<VeinDataType>, Box<VeinDataType>),
    /// A union of mutually exclusive member types
    Union(Vec<VeinDataType>),
    /// A dictionary-encoded (key, value) pair
    Dictionary(Box<VeinDataType>, Box<VeinDataType>),
    /// A run-end-encoded (run_end, value) pair
    RunEndEncoded(Box<VeinDataType>, Box<VeinDataType>),

    // ── Microservice & Ecosystem Extensions ───────────────
    /// Semi-structured JSON payload. Vein will target eBPF extraction
    /// for this directly from the QUIC network socket into Arrow StringView.
    Variant,
    /// Custom User-Defined Type (UDT). Name + underlying physical fallback type.
    Extension(String, Box<VeinDataType>),
}

impl VeinDataType {
    /// Converts to Apache Arrow memory layouts exactly when the CPU is ready to process.
    pub fn to_arrow(&self) -> arrow::datatypes::DataType {
        use arrow::datatypes::{DataType, Field, IntervalUnit, UnionFields, UnionMode};
        use std::sync::Arc;

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

            // Microservice Extensions
            // Variants are stored as StringView in memory for blazing fast eBPF JSON extractions
            VeinDataType::Variant => DataType::Utf8View,

            VeinDataType::Extension(_name, fallback) => {
                // In Arrow, extension types are represented by the fallback DataType
                // combined with Schema Field metadata (which is handled during schema generation, not here).
                fallback.to_arrow()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, UnionMode};

    fn field(name: &str, ty: DataType) -> arrow::datatypes::FieldRef {
        std::sync::Arc::new(arrow::datatypes::Field::new(name, ty, true))
    }

    #[test]
    fn temporal_types_map_to_arrow() {
        assert_eq!(VeinDataType::Date32.to_arrow(), DataType::Date32);
        assert_eq!(VeinDataType::Date64.to_arrow(), DataType::Date64);
        assert_eq!(
            VeinDataType::Time32.to_arrow(),
            DataType::Time32(TimeUnit::Millisecond)
        );
        assert_eq!(
            VeinDataType::Time64.to_arrow(),
            DataType::Time64(TimeUnit::Microsecond)
        );
        assert_eq!(
            VeinDataType::Duration(TimeUnit::Nanosecond).to_arrow(),
            DataType::Duration(TimeUnit::Nanosecond)
        );
        assert_eq!(
            VeinDataType::Interval.to_arrow(),
            DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano)
        );
    }

    #[test]
    fn decimal_types_map_to_arrow() {
        assert_eq!(
            VeinDataType::Decimal128(38, 10).to_arrow(),
            DataType::Decimal128(38, 10)
        );
        assert_eq!(
            VeinDataType::Decimal256(76, 20).to_arrow(),
            DataType::Decimal256(76, 20)
        );
    }

    #[test]
    fn list_and_large_list_map_to_arrow() {
        assert_eq!(
            VeinDataType::List(Box::new(VeinDataType::Int64)).to_arrow(),
            DataType::List(field("item", DataType::Int64))
        );
        assert_eq!(
            VeinDataType::LargeList(Box::new(VeinDataType::Utf8)).to_arrow(),
            DataType::LargeList(field("item", DataType::Utf8))
        );
    }

    #[test]
    fn vector_and_map_map_to_arrow() {
        assert_eq!(
            VeinDataType::Vector(3).to_arrow(),
            DataType::FixedSizeList(field("item", DataType::Float32), 3)
        );
        let entries = DataType::Struct(
            vec![
                arrow::datatypes::Field::new("key", DataType::Utf8, false),
                arrow::datatypes::Field::new("value", DataType::Int64, true),
            ]
            .into(),
        );
        assert_eq!(
            VeinDataType::Map(
                Box::new(VeinDataType::Utf8),
                Box::new(VeinDataType::Int64)
            )
            .to_arrow(),
            DataType::Map(std::sync::Arc::new(arrow::datatypes::Field::new(
                "entries",
                entries,
                false
            )), false)
        );
    }

    #[test]
    fn union_maps_to_dense_union_with_fresh_ids() {
        let arrow_ty = VeinDataType::Union(vec![
            VeinDataType::Int64,
            VeinDataType::Utf8,
            VeinDataType::Float64,
        ])
        .to_arrow();
        let DataType::Union(union_fields, UnionMode::Dense) = arrow_ty else {
            panic!("expected dense union, got {arrow_ty:?}");
        };
        assert_eq!(union_fields.len(), 3);
        let collected: Vec<(i8, String)> = union_fields
            .iter()
            .map(|(id, f)| (id, f.data_type().to_string()))
            .collect();
        assert_eq!(
            collected,
            vec![
                (0, "Int64".to_string()),
                (1, "Utf8".to_string()),
                (2, "Float64".to_string())
            ]
        );
    }

    #[test]
    fn dictionary_and_run_end_encoded_map_to_arrow() {
        assert_eq!(
            VeinDataType::Dictionary(
                Box::new(VeinDataType::Int32),
                Box::new(VeinDataType::Utf8)
            )
            .to_arrow(),
            DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::Utf8)
            )
        );
        assert_eq!(
            VeinDataType::RunEndEncoded(
                Box::new(VeinDataType::Int32),
                Box::new(VeinDataType::Int64)
            )
            .to_arrow(),
            DataType::RunEndEncoded(
                std::sync::Arc::new(arrow::datatypes::Field::new(
                    "run_end",
                    DataType::Int32,
                    false
                )),
                field("values", DataType::Int64)
            )
        );
    }
}
