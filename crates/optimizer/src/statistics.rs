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

/// Why an individual column synopsis cannot be used for estimation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ColumnStatisticsValidationError {
    NullCountExceedsRows,
    NullsInNonNullableColumn,
    DistinctCountExceedsNonNullRows,
    ZeroDistinctForNonEmptyPopulation,
    NullBound,
    NonFiniteBound,
    IncomparableBounds,
    ReversedBounds,
    ConstantBoundsWithMultipleDistinctValues,
    BoundsForAllNullPopulation,
}

/// Validates population and supported bound invariants before any statistics
/// consumer uses them. NDV is a non-null count and is never a uniqueness proof.
pub(crate) fn validate_column(
    stats: &plan::ColumnStatistics,
    rows: u64,
    nullable: bool,
) -> Result<(), ColumnStatisticsValidationError> {
    let nulls = stats.null_count;
    if nulls.is_some_and(|n| n > rows) {
        return Err(ColumnStatisticsValidationError::NullCountExceedsRows);
    }
    if nulls.is_some_and(|n| !nullable && n != 0) {
        return Err(ColumnStatisticsValidationError::NullsInNonNullableColumn);
    }
    let non_null_rows = rows - nulls.unwrap_or(0);
    if stats.distinct_count.is_some_and(|distinct| distinct > non_null_rows) {
        return Err(ColumnStatisticsValidationError::DistinctCountExceedsNonNullRows);
    }
    if non_null_rows > 0 && stats.distinct_count == Some(0) {
        return Err(ColumnStatisticsValidationError::ZeroDistinctForNonEmptyPopulation);
    }
    let min = stats.min_value.as_ref();
    let max = stats.max_value.as_ref();
    if min.into_iter().chain(max).any(|value| matches!(value, LogicalLiteral::Null(_))) {
        return Err(ColumnStatisticsValidationError::NullBound);
    }
    if min.into_iter().chain(max).any(|value| {
        matches!(value, LogicalLiteral::Float64(value) if !value.is_finite())
    }) {
        return Err(ColumnStatisticsValidationError::NonFiniteBound);
    }
    if let Some((minimum, maximum)) = min.zip(max) {
        let Some(order) = compare(minimum, maximum) else {
            return Err(ColumnStatisticsValidationError::IncomparableBounds);
        };
        if order.is_gt() {
            return Err(ColumnStatisticsValidationError::ReversedBounds);
        }
        if order.is_eq() && stats.distinct_count.is_some_and(|distinct| distinct > 1) {
            return Err(ColumnStatisticsValidationError::ConstantBoundsWithMultipleDistinctValues);
        }
    }
    if nulls == Some(rows) && (min.is_some() || max.is_some()) {
        return Err(ColumnStatisticsValidationError::BoundsForAllNullPopulation);
    }
    Ok(())
}

pub(crate) fn valid_column(stats: &plan::ColumnStatistics, rows: u64, nullable: bool) -> bool {
    validate_column(stats, rows, nullable).is_ok()
}
