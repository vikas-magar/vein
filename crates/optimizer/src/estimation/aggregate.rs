//! Group cardinality estimation shared by AGGREGATE and DISTINCT.

use std::collections::HashSet;
use plan::{ColumnId, LogicalExpr, VeinSchema};
use super::{Estimate, EstimateSource, columns::RelationEstimate};

/// Both grouping expressions and DISTINCT schema columns feed the same borrowed
/// key iterator. No temporary expressions or reference-count increments.
pub(super) fn groups(
    input: &RelationEstimate<'_>, keys: impl IntoIterator<Item=Option<ColumnId>>,
) -> Estimate {
    let mut keys = keys.into_iter().peekable();
    if keys.peek().is_none() { return Estimate::logical(1.0); }
    if input.rows.value() == Some(0.0) { return input.rows; }
    let mut seen = HashSet::new();
    let mut product = 1.0_f64;
    for key in keys {
        let Some(id) = key else { return Estimate::unknown(EstimateSource::UnsupportedExpression); };
        if !seen.insert(id) { continue; }
        let Some(column) = input.columns.get(&id) else {
            return Estimate::unknown(EstimateSource::MissingColumnStatistics);
        };
        let Some((distinct, nulls)) = column.distinct.zip(column.null_fraction) else {
            return Estimate::unknown(EstimateSource::MissingColumnStatistics);
        };
        product *= distinct + if nulls > 0.0 { 1.0 } else { 0.0 };
        // Bound each multiplication, avoiding overflow for many grouping keys.
        if let Some(rows) = input.rows.value() { product = product.min(rows); }
        if !product.is_finite() { return Estimate::unknown(EstimateSource::ArithmeticOverflow); }
    }
    input.rows.combine(Estimate::model(product, EstimateSource::Aggregate),
                       EstimateSource::Aggregate, f64::min)
}

pub(super) fn aggregate<'a>(
    input: RelationEstimate<'a>, sets: &[Vec<LogicalExpr>], schema: &VeinSchema,
) -> RelationEstimate<'a> {
    if sets.is_empty() { return RelationEstimate::unknown(); }
    let rows = sets.iter().fold(Estimate::logical(0.0), |sum, keys| {
        let keys = keys.iter().map(|key| match key {
            LogicalExpr::Column(c) => Some(c.id),
            _ => None,
        });
        sum.combine(groups(&input, keys), EstimateSource::Aggregate, |a, b| a + b)
    });
    // Multiple grouping sets inject NULLs and require new distributions.
    let mut result = RelationEstimate::new(rows);
    if sets.len() == 1 {
        let keys = sets[0].iter().enumerate()
            .filter(|(i, key)| !sets[0][..*i].contains(key))
            .map(|(_, key)| key);
        for (key, output) in keys.zip(&schema.fields) {
            if let LogicalExpr::Column(column) = key {
                if let Some(stats) = input.columns.get(&column.id) {
                    let mut stats = *stats;
                    stats.after_subset(rows);
                    result.columns.insert(output.id, stats);
                }
            }
        }
    }
    result
}
