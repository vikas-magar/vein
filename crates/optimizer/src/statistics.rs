//! Borrowed literal comparisons and numeric distances for estimation.

use std::cmp::Ordering;
use plan::LogicalLiteral;
use parser::ast::expr::BinaryOp;

/// Orients comparisons when the literal or join key is on the left.
pub(crate) fn reverse(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::Leq => BinaryOp::Geq,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::Geq => BinaryOp::Leq,
        _ => op,
    }
}

/// Comparisons are exact for supported, compatible types. Decimal scale changes
/// are checked; unsupported types or overflow leave estimates unknown.
pub(crate) fn compare(a: &LogicalLiteral, b: &LogicalLiteral) -> Option<Ordering> {
    a.try_cmp(b)
}

/// Subtract before conversion so close large integers do not collapse to the
/// same floating-point value. Distances are only used by uniform range models.
pub(super) fn distance(a: &LogicalLiteral, b: &LogicalLiteral) -> Option<f64> {
    use LogicalLiteral::*;
    let distance = match (a, b) {
        (Int64(a), Int64(b)) | (Time(a), Time(b)) | (Timestamp(a), Timestamp(b)) =>
            (*a as i128 - *b as i128) as f64,
        (Date(a), Date(b)) => (*a as i64 - *b as i64) as f64,
        (Float64(a), Float64(b)) if a.is_finite() && b.is_finite() => a - b,
        (Decimal(a, _, sa), Decimal(b, _, sb)) if sa == sb =>
            a.checked_sub(*b)? as f64,
        _ => return None,
    };
    distance.is_finite().then_some(distance)
}

/// Validates population and supported bound invariants before any statistics
/// consumer uses them. NDV is a non-null count and is never a uniqueness proof.
pub(crate) fn valid_column(stats: &plan::ColumnStatistics, rows: u64, nullable: bool) -> bool {
    let nulls = stats.null_count;
    if nulls.is_some_and(|n| n > rows || (!nullable && n != 0))
        || stats.distinct_count.is_some_and(|d| d > rows - nulls.unwrap_or(0))
        || (rows > nulls.unwrap_or(0) && stats.distinct_count == Some(0))
    { return false; }
    let min = stats.min_value.as_ref();
    let max = stats.max_value.as_ref();
    !(min.into_iter().chain(max).any(|v| matches!(v, LogicalLiteral::Null(_))
        || matches!(v, LogicalLiteral::Float64(v) if !v.is_finite()))
        || min.zip(max).is_some_and(|(a, b)| compare(a, b).is_none_or(|o| o.is_gt()
        || (o.is_eq() && stats.distinct_count.is_some_and(|d| d > 1))))
        || (nulls == Some(rows) && (min.is_some() || max.is_some())))
}
