//! Literal values of the engine's data types.

use std::cmp::Ordering;
use std::sync::Arc;
use arrow::datatypes::TimeUnit;

use crate::column::{ColumnId, ResolvedColumn};
use crate::data_type::VeinDataType;

/// A literal value of a known [`VeinDataType`].
///
/// Literals are the constant leaves of logical expressions and the payload type
/// for statistics, such as a column's minimum and maximum. They live beside
/// [`VeinDataType`] rather than in the logical-expression crate so that both the
/// expression layer and the metadata layer can name a constant without either
/// depending on the other.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum LogicalLiteral {
    /// A null value of a specific type
    Null(VeinDataType),
    /// A boolean literal
    Boolean(bool),
    /// A 64-bit integer literal
    Int64(i64),
    /// A 64-bit unsigned integer literal, retained exactly for statistics.
    UInt64(u64),
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

impl LogicalLiteral {
    /// Compares compatible scalar literals without lossy numeric coercion.
    ///
    /// This is intended for metadata and optimizer reasoning. Incompatible
    /// literal kinds, non-finite floats, and decimal rescaling overflow return
    /// `None`; callers must treat an unknown comparison conservatively.
    pub fn try_cmp(&self, other: &Self) -> Option<Ordering> {
        use LogicalLiteral::*;
        match (self, other) {
            (Boolean(left), Boolean(right)) => Some(left.cmp(right)),
            (Int64(left), Int64(right)) => Some(left.cmp(right)),
            (UInt64(left), UInt64(right)) => Some(left.cmp(right)),
            (Float64(left), Float64(right)) => left.partial_cmp(right),
            (String(left), String(right)) => Some(left.cmp(right)),
            (Date(left), Date(right)) => Some(left.cmp(right)),
            (Time(left), Time(right)) | (Timestamp(left), Timestamp(right)) => {
                Some(left.cmp(right))
            }
            (Decimal(left, _, left_scale), Decimal(right, _, right_scale)) => {
                let scale = (*left_scale).max(*right_scale);
                let left_factor = 10_i128.checked_pow((scale as i16 - *left_scale as i16) as u32)?;
                let right_factor = 10_i128.checked_pow((scale as i16 - *right_scale as i16) as u32)?;
                Some(left.checked_mul(left_factor)?.cmp(&right.checked_mul(right_factor)?))
            }
            _ => None,
        }
    }
}

/// Returns the logical data type represented by a literal value.
pub fn literal_type(literal: &LogicalLiteral) -> VeinDataType {
    match literal {
        LogicalLiteral::Null(data_type) => data_type.clone(),
        LogicalLiteral::Boolean(_) => VeinDataType::Boolean,
        LogicalLiteral::Int64(_) => VeinDataType::Int64,
        LogicalLiteral::UInt64(_) => VeinDataType::UInt64,
        LogicalLiteral::Float64(_) => VeinDataType::Float64,
        LogicalLiteral::String(_) => VeinDataType::Utf8,
        LogicalLiteral::Date(_) => VeinDataType::Date32,
        LogicalLiteral::Timestamp(_) => VeinDataType::Timestamp(TimeUnit::Microsecond, None),
        LogicalLiteral::Time(_) => VeinDataType::Time64(TimeUnit::Microsecond),
        LogicalLiteral::Interval { .. } => {
            VeinDataType::Interval(crate::IntervalUnit::MonthDayNano)
        }
        LogicalLiteral::Decimal(_, precision, scale) => {
            VeinDataType::Decimal128(*precision, *scale)
        }
        LogicalLiteral::List(items) => VeinDataType::List(Arc::new(common_element_type(items))),
        LogicalLiteral::Struct(entries) => {
            let fields = entries
                .iter()
                .map(|(name, literal)| ResolvedColumn {
                    id: ColumnId(0),
                    name: name.clone(),
                    qualifier: None,
                    data_type: literal_type(literal),
                    is_factorized: false,
                    nullable: true,
                })
                .collect::<Vec<_>>();
            VeinDataType::Struct(fields.into())
        }
        LogicalLiteral::Map(entries) => {
            let key_type = entries
                .first()
                .map(|(key, _)| literal_type(key))
                .unwrap_or(VeinDataType::Null);
            let value_type = entries
                .first()
                .map(|(_, value)| literal_type(value))
                .unwrap_or(VeinDataType::Null);
            VeinDataType::Map(Arc::new(key_type), Arc::new(value_type))
        }
        LogicalLiteral::Vector(values) => VeinDataType::Vector(values.len() as u32),
    }
}

/// Returns the first non-null element type, or `Null` for an empty or all-null list.
/// Returns the first non-null element type, or `Null` for an empty or all-null list.
fn common_element_type(items: &[LogicalLiteral]) -> VeinDataType {
    items
        .iter()
        .find_map(|literal| {
            if matches!(literal, LogicalLiteral::Null(_)) {
                None
            } else {
                Some(literal_type(literal))
            }
        })
        .unwrap_or(VeinDataType::Null)
}
#[cfg(test)]
mod tests;
