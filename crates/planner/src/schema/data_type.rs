use crate::schema::column::ResolvedColumn;

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
    /// Time of day (Microseconds)
    Time64,
    /// Absolute timestamp with optional timezone
    Timestamp(Option<String>),
    /// Time-series interval (Month, Day, Nanosecond)
    Interval,

    // ── Financial / High Precision ────────────────────────
    /// A 128-bit decimal type with precision and scale
    Decimal128(u8, i8),

    // ── Nested & Collections ──────────────────────────────
    /// A variable length list of another type
    List(Box<VeinDataType>),
    /// Fixed size list
    FixedSizeList(Box<VeinDataType>, i32),
    /// High-dimensional vector for AI embeddings
    Vector(u32),
    /// A nested struct of named fields
    Struct(Vec<ResolvedColumn>),
    /// A map of key-value pairs
    Map(Box<VeinDataType>, Box<VeinDataType>),

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
        use arrow::datatypes::{DataType, Field, TimeUnit, IntervalUnit};
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
            VeinDataType::Time64 => DataType::Time64(TimeUnit::Microsecond),
            VeinDataType::Timestamp(tz) => DataType::Timestamp(TimeUnit::Microsecond, tz.as_deref().map(|s| s.into())),
            VeinDataType::Interval => DataType::Interval(IntervalUnit::MonthDayNano),
            
            VeinDataType::Decimal128(p, s) => DataType::Decimal128(*p, *s),
            
            VeinDataType::List(inner) => DataType::List(Arc::new(Field::new("item", inner.to_arrow(), true))),
            VeinDataType::FixedSizeList(inner, size) => DataType::FixedSizeList(Arc::new(Field::new("item", inner.to_arrow(), true)), *size),
            VeinDataType::Vector(dim) => DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), *dim as i32),
            VeinDataType::Struct(fields) => {
                let arrow_fields = fields.iter()
                    .map(|f| Field::new(&f.name, f.data_type.to_arrow(), true))
                    .collect();
                DataType::Struct(arrow_fields)
            },
            VeinDataType::Map(key, value) => {
                let keys = Field::new("key", key.to_arrow(), false);
                let values = Field::new("value", value.to_arrow(), true);
                let struct_type = DataType::Struct(vec![keys, values].into());
                DataType::Map(Arc::new(Field::new("entries", struct_type, false)), false)
            },

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
