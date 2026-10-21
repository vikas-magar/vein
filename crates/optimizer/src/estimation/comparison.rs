//! Uniform comparison selectivity with SQL NULL semantics.

use std::cmp::Ordering::{Equal, Greater, Less};
use plan::{HistogramStatistics, LogicalLiteral};
use parser::ast::expr::BinaryOp;
use super::{columns::ColumnEstimate, selectivity::PredicateEstimate};
use crate::statistics::{compare, distance};

pub(super) fn estimate(column: ColumnEstimate<'_>, value: &LogicalLiteral, op: BinaryOp) -> PredicateEstimate {
    if matches!(value, LogicalLiteral::Null(_)) { return PredicateEstimate::logical(0.0, 0.0); }
    let non_null = column.null_fraction.map(|n| 1.0 - n);
    if non_null == Some(0.0) { return PredicateEstimate::model(Some(0.0), Some(0.0)); }
    let lower = column.min.and_then(|min| compare(value, min));
    let upper = column.max.and_then(|max| compare(value, max));
    let equality = || {
        if lower == Some(Less) || upper == Some(Greater) { return Some(0.0); }
        if column.min.zip(column.max).is_some_and(|(min, max)| compare(min, max) == Some(Equal))
            && lower == Some(Equal) {
            return non_null;
        }
        if let Some(frequency) = most_common_frequency(column, value, non_null) {
            return Some(frequency);
        }
        column.distinct.filter(|d| *d > 0.0).zip(non_null).map(|(d, n)| n / d)
    };
    let less_than = |inclusive: bool| {
        if lower == Some(Less) || (!inclusive && lower == Some(Equal)) { return Some(0.0); }
        if upper == Some(Greater) || (inclusive && upper == Some(Equal)) { return non_null; }
        if let Some(histogram) = column.synopsis.and_then(|stats| stats.histogram.as_ref())
            && let Some(population) = column.synopsis_population.filter(|count| {
                histogram.population_count == *count && *count > 0
            })
                && let Some(rank) = histogram_rank(histogram, value, inclusive) {
                    return non_null.map(|n| n * (rank / population as f64).clamp(0.0, 1.0));
                }
        let (min, max) = column.min.zip(column.max)?;
        let width = distance(max, min)?;
        if width <= 0.0 { return None; }
        let fraction = (distance(value, min)? / width).clamp(0.0, 1.0);
        // Uniform interpolation is an approximation, never a pruning proof.
        non_null.map(|n| n * fraction)
    };
    let truth = match op {
        BinaryOp::Eq | BinaryOp::Neq => equality(),
        BinaryOp::Lt => less_than(false),
        BinaryOp::Leq => less_than(true),
        BinaryOp::Gt => less_than(true).zip(non_null).map(|(l, n)| n - l),
        BinaryOp::Geq => less_than(false).zip(non_null).map(|(l, n)| n - l),
        _ => return PredicateEstimate::unknown(),
    };
    let falsehood = truth.zip(non_null).map(|(t, n)| n - t);
    if op == BinaryOp::Neq {
        PredicateEstimate::model(falsehood, truth)
    } else {
        PredicateEstimate::model(truth, falsehood)
    }
}

/// Estimates equality from Misra-Gries candidates and distributes residual
/// frequency over the non-candidate NDV. The midpoint reflects the bounded
/// candidate undercount; impossible totals are rejected and fall back to NDV.
fn most_common_frequency(
    column: ColumnEstimate<'_>,
    value: &LogicalLiteral,
    non_null_fraction: Option<f64>,
) -> Option<f64> {
    let stats = column.synopsis?;
    let mcv = stats.most_common_values.as_ref()?;
    let population = column.synopsis_population?;
    if mcv.analyzed_count != population || population == 0 || mcv.candidates.is_empty() {
        return None;
    }
    let lower_total = mcv.candidates.iter().try_fold(0_u64, |sum, candidate| {
        sum.checked_add(candidate.lower_count)
    })?;
    let remaining = population.checked_sub(lower_total)? as f64;
    let per_candidate_error = if mcv.candidates.is_empty() {
        0.0
    } else {
        (mcv.error_bound as f64 / 2.0).min(remaining / mcv.candidates.len() as f64)
    };
    let estimate_count = |candidate: &plan::MostCommonValue| {
        candidate.lower_count as f64 + per_candidate_error
    };
    let candidate_total = mcv.candidates.iter().map(estimate_count).sum::<f64>();
    if !candidate_total.is_finite() || candidate_total > population as f64 {
        return None;
    }
    let frequency = if let Some(candidate) = mcv.candidates.iter()
        .find(|candidate| compare(&candidate.value, value) == Some(Equal))
    {
        estimate_count(candidate)
    } else {
        let distinct = column.distinct?;
        let candidate_count = u64::try_from(mcv.candidates.len()).ok()?;
        let remaining_distinct = distinct - candidate_count as f64;
        if remaining_distinct > 0.0 {
            (population as f64 - candidate_total) / remaining_distinct
        } else if stats.distinct_count_relative_error.is_none()
            && stats.distinct_count == Some(candidate_count)
        {
            0.0
        } else {
            return None;
        }
    };
    let analyzed_fraction = (frequency / population as f64).clamp(0.0, 1.0);
    Some(non_null_fraction? * analyzed_fraction)
}

/// Uses the equal-depth buckets and their corresponding reservoir samples to
/// estimate a rank. This handles ordered logical types without assuming a
/// numeric distance (for example, strings and dates).
fn histogram_rank(
    histogram: &HistogramStatistics,
    value: &LogicalLiteral,
    inclusive: bool,
) -> Option<f64> {
    let sample_count = usize::try_from(histogram.sample_count).ok()?;
    if sample_count == 0
        || sample_count != histogram.samples.len()
        || histogram.buckets.is_empty()
    {
        return None;
    }
    let bucket_count = histogram.buckets.len();
    let mut cumulative = 0.0;
    for (index, bucket) in histogram.buckets.iter().enumerate() {
        let lower_order = compare(value, &bucket.lower_bound)?;
        if lower_order.is_lt() {
            return Some(cumulative);
        }
        let upper_order = compare(value, &bucket.upper_bound)?;
        if upper_order.is_gt() || (inclusive && upper_order.is_eq()) {
            cumulative += bucket.estimated_count as f64;
            continue;
        }

        let start = index.checked_mul(sample_count)? / bucket_count;
        let end = (index + 1).checked_mul(sample_count)? / bucket_count;
        let samples = histogram.samples.get(start..end)?;
        if samples.is_empty() || samples.len() as u64 != bucket.sample_count {
            return None;
        }
        let passing = samples.iter().filter(|sample| {
            compare(&sample.value, value).is_some_and(|order| {
                order.is_lt() || (inclusive && order.is_eq())
            })
        }).count();
        cumulative += bucket.estimated_count as f64 * passing as f64 / samples.len() as f64;
        return Some(cumulative.min(histogram.population_count as f64));
    }
    Some(cumulative.min(histogram.population_count as f64))
}
