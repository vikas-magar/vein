use crate::schema::VeinDataType;

/// A literal value inside a logical expression
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum LogicalLiteral {
    /// A null value of a specific type
    Null(VeinDataType),
    /// A boolean literal
    Boolean(bool),
    /// A 64-bit integer literal
    Int64(i64),
    /// A 64-bit float literal
    Float64(f64),
    /// A string literal
    String(String),
    /// A date literal, days since the UNIX epoch (`Date32`).
    Date(i32),
    /// A timestamp literal, microseconds since the UNIX epoch (`Timestamp`).
    Timestamp(i64),
    /// A time-of-day literal, microseconds since midnight (`Time64`).
    Time(i64),
    /// A duration literal in months, days, and nanoseconds (`Interval`).
    Interval {
        /// Whole months in the interval.
        months: i32,
        /// Whole days in the interval.
        days: i32,
        /// Sub-second nanoseconds in the interval.
        nanos: i64,
    },
    /// A 128-bit decimal literal with precision and scale (`Decimal128`).
    Decimal(i128, u8, i8),
    /// A list literal of homogeneous values (`List`).
    List(Vec<LogicalLiteral>),
    /// A struct literal of named fields (`Struct`).
    Struct(Vec<(String, LogicalLiteral)>),
    /// A map literal of key/value pairs (`Map`).
    Map(Vec<(LogicalLiteral, LogicalLiteral)>),
    /// A vector literal for AI embeddings (`Vector`).
    Vector(Vec<f32>),
}
