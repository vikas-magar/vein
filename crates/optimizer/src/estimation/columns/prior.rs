//! Explicit cost-model priors for columns lacking a measured NDV.
use plan::{ColumnStatistics, LogicalLiteral};

/// Caps the possible domain by validated discrete bounds and non-null population.
/// Uniform occupancy of this domain is a model, never a uniqueness or pruning proof.
pub(super) fn distinct_upper_bound(stats: &ColumnStatistics, rows: u64) -> f64 {
    let population = rows.saturating_sub(stats.null_count.unwrap_or(0)) as f64;
    let Some((min, max)) = stats.min_value.as_ref().zip(stats.max_value.as_ref()) else {
        return population;
    };
    population.min(bounded_domain(min, max).unwrap_or(population))
}

pub(super) fn bounded_domain(min: &LogicalLiteral, max: &LogicalLiteral) -> Option<f64> {
    if crate::statistics::compare(min, max).is_some_and(|order| order.is_eq()) { return Some(1.0); }
    let discrete = matches!((min, max),
        (LogicalLiteral::Int64(_), LogicalLiteral::Int64(_))
        | (LogicalLiteral::Date(_), LogicalLiteral::Date(_))
        | (LogicalLiteral::Time(_), LogicalLiteral::Time(_))
        | (LogicalLiteral::Timestamp(_), LogicalLiteral::Timestamp(_))
        | (LogicalLiteral::Decimal(..), LogicalLiteral::Decimal(..)));
    if discrete && let Some(width) = crate::statistics::distance(max, min).filter(|width| *width >= 0.0) {
        return Some(width + 1.0);
    }
    None
}
