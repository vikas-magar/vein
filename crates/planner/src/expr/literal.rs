use crate::schema::VeinDataType;

/// A literal value inside a logical expression
#[derive(Debug, Clone, PartialEq)]
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
}
