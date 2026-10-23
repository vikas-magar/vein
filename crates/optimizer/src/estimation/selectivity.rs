//! Three-valued predicate estimates with an injectable distribution strategy.

use plan::{ColumnId, LogicalExpr, LogicalLiteral};
use parser::ast::expr::BinaryOp;
use super::{Estimate, EstimateSource};
use super::columns::ColumnEstimate;
use crate::statistics::reverse;

/// Borrowed statistics visible to a predicate strategy.
pub trait ColumnStatisticsLookup {
    /// Returns distinct count and null fraction for a query-local column.
    fn distribution(&self, id: ColumnId) -> Option<(Option<f64>, Option<f64>)>;
    /// Returns borrowed minimum and maximum non-null values.
    fn bounds(&self, id: ColumnId) -> Option<(Option<&LogicalLiteral>, Option<&LogicalLiteral>)>;
    /// Returns a validated borrowed synopsis for distribution-aware estimates.
    fn synopsis(&self, _id: ColumnId) -> Option<&plan::ColumnStatistics> {
        None
    }
    /// Returns the represented non-null, non-NaN row count for that synopsis.
    fn synopsis_population(&self, _id: ColumnId) -> Option<u64> {
        None
    }
}

/// Estimated TRUE and FALSE fractions; the remaining population is UNKNOWN.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PredicateEstimate {
    /// Fraction passing a WHERE predicate.
    pub true_fraction: Estimate,
    /// Fraction passing NOT(predicate), excluding SQL NULL.
    pub false_fraction: Estimate,
}

impl PredicateEstimate {
    pub(crate) fn unknown() -> Self {
        let unknown = Estimate::unknown(EstimateSource::UnsupportedExpression);
        Self { true_fraction: unknown, false_fraction: unknown }
    }

    pub(crate) fn logical(truth: f64, falsity: f64) -> Self {
        Self { true_fraction: Estimate::logical(truth), false_fraction: Estimate::logical(falsity) }
    }

    pub(crate) fn model(truth: Option<f64>, falsity: Option<f64>) -> Self {
        let estimate = |v: Option<f64>| v.map(|v| Estimate::model(v.clamp(0.0, 1.0), EstimateSource::Filter))
            .unwrap_or(Estimate::unknown(EstimateSource::MissingColumnStatistics));
        Self { true_fraction: estimate(truth), false_fraction: estimate(falsity) }
    }

    pub(crate) fn and(self, other: Self) -> Self {
        Self {
            true_fraction: product(self.true_fraction, other.true_fraction),
            false_fraction: union(self.false_fraction, other.false_fraction),
        }
    }

    fn or(self, other: Self) -> Self {
        Self {
            true_fraction: union(self.true_fraction, other.true_fraction),
            false_fraction: product(self.false_fraction, other.false_fraction),
        }
    }
}

fn product(a: Estimate, b: Estimate) -> Estimate {
    if a.value() == Some(0.0) { return a; }
    if b.value() == Some(0.0) { return b; }
    a.combine(b, EstimateSource::Filter, |a, b| a * b)
}

fn union(a: Estimate, b: Estimate) -> Estimate {
    if a.value() == Some(1.0) { return a; }
    if b.value() == Some(1.0) { return b; }
    a.combine(b, EstimateSource::Filter, |a, b| a + b - a * b)
}

/// Strategy seam for histogram, sampled, or statistics-based selectivity.
pub trait SelectivityEstimator: Send + Sync {
    /// Estimates a borrowed predicate without inspecting or copying row data.
    fn estimate(&self, expr: &LogicalExpr, columns: &dyn ColumnStatisticsLookup) -> PredicateEstimate;
}

impl<T: SelectivityEstimator + ?Sized> SelectivityEstimator for &T {
    fn estimate(&self, expr: &LogicalExpr, columns: &dyn ColumnStatisticsLookup) -> PredicateEstimate {
        (**self).estimate(expr, columns)
    }
}

/// Uniform NDV/range model; unsupported expressions remain explicitly unknown.
#[derive(Debug, Default)]
pub struct StatisticsSelectivity;

impl SelectivityEstimator for StatisticsSelectivity {
    fn estimate(&self, expr: &LogicalExpr, columns: &dyn ColumnStatisticsLookup) -> PredicateEstimate {
        estimate(expr, columns, 0)
    }
}

// Bound expression recursion independently of the iterative plan traversal.
const MAX_PREDICATE_DEPTH: usize = 128;

fn estimate(expr: &LogicalExpr, columns: &dyn ColumnStatisticsLookup, depth: usize) -> PredicateEstimate {
    if depth >= MAX_PREDICATE_DEPTH { return PredicateEstimate::unknown(); }
    match expr {
        LogicalExpr::Literal(LogicalLiteral::Boolean(v)) =>
            PredicateEstimate::logical(u8::from(*v) as f64, u8::from(!*v) as f64),
        LogicalExpr::Literal(LogicalLiteral::Null(_)) => PredicateEstimate::logical(0.0, 0.0),
        LogicalExpr::Not { expr } => {
            let result = estimate(expr, columns, depth + 1);
            PredicateEstimate { true_fraction: result.false_fraction, false_fraction: result.true_fraction }
        }
        LogicalExpr::BinaryOp { left, op: BinaryOp::And | BinaryOp::Or, right, .. } => {
            let a = estimate(left, columns, depth + 1);
            if left == right { return a; }
            let b = estimate(right, columns, depth + 1);
            if matches!(expr, LogicalExpr::BinaryOp { op: BinaryOp::And, .. }) { a.and(b) } else { a.or(b) }
        }
        LogicalExpr::IsNull { expr: operand } | LogicalExpr::IsNotNull { expr: operand } => {
            let LogicalExpr::Column(column) = operand.as_ref() else { return PredicateEstimate::unknown(); };
            let nulls = columns.distribution(column.id).and_then(|(_, n)| n);
            let result = PredicateEstimate::model(nulls, nulls.map(|n| 1.0 - n));
            if matches!(expr, LogicalExpr::IsNotNull { .. }) {
                PredicateEstimate { true_fraction: result.false_fraction, false_fraction: result.true_fraction }
            } else { result }
        }
        LogicalExpr::BinaryOp { left, op, right, .. } => {
            let (column, value, op) = match (left.as_ref(), right.as_ref()) {
                (LogicalExpr::Column(c), LogicalExpr::Literal(v)) => (c, v, *op),
                (LogicalExpr::Literal(v), LogicalExpr::Column(c)) => (c, v, reverse(*op)),
                _ => return PredicateEstimate::unknown(),
            };
            if matches!(op, BinaryOp::Eq | BinaryOp::Neq | BinaryOp::Lt | BinaryOp::Leq | BinaryOp::Gt | BinaryOp::Geq)
                && matches!(value, LogicalLiteral::Null(_)) {
                return PredicateEstimate::logical(0.0, 0.0);
            }
            let Some((distinct, null_fraction)) = columns.distribution(column.id) else {
                return PredicateEstimate::model(None, None);
            };
            let (min, max) = columns.bounds(column.id).unwrap_or((None, None));
            let synopsis = columns.synopsis(column.id);
            let synopsis_population = columns.synopsis_population(column.id);
            super::comparison::estimate(
                ColumnEstimate {
                    distinct,
                    distinct_is_prior: false,
                    null_fraction,
                    min,
                    max,
                    synopsis,
                    synopsis_population,
                },
                value,
                op,
            )
        }
        _ => PredicateEstimate::unknown(),
    }
}
