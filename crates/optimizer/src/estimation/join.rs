//! Join estimates use query-local identities, never table names or assumed keys.

use parser::ast::{expr::BinaryOp, pipe_op::JoinType};
use plan::{LogicalExpr, VeinSchema};
use super::{Estimate, EstimateSource, columns::RelationEstimate};

fn equality(left: &RelationEstimate<'_>, right: &RelationEstimate<'_>,
            expr: &LogicalExpr) -> Option<f64>
{
    let LogicalExpr::BinaryOp { left: a, op: BinaryOp::Eq, right: b, .. } = expr else { return None; };
    let (LogicalExpr::Column(a), LogicalExpr::Column(b)) = (a.as_ref(), b.as_ref()) else { return None; };
    let pair = left.columns.get(&a.id).zip(right.columns.get(&b.id))
        .or_else(|| left.columns.get(&b.id).zip(right.columns.get(&a.id)))?;
    let (a, b) = pair;
    if a.min.zip(b.max).is_some_and(|(a, b)| crate::statistics::compare(a, b).is_some_and(|o| o.is_gt()))
        || b.min.zip(a.max).is_some_and(|(b, a)| crate::statistics::compare(b, a).is_some_and(|o| o.is_gt())) {
        return Some(0.0);
    }
    let (da, db) = a.distinct.zip(b.distinct)?;
    let (na, nb) = a.null_fraction.zip(b.null_fraction)?;
    if da == 0.0 || db == 0.0 { return Some(0.0); }
    Some((1.0 - na) * (1.0 - nb) / da.max(db))
}

fn join_selectivity<'e>(
    left: &RelationEstimate<'_>,
    right: &RelationEstimate<'_>,
    on: Option<&'e LogicalExpr>,
    additional: &[&'e LogicalExpr],
) -> Estimate
{
    // For composite keys, use the most selective supported key rather than
    // multiplying correlated key estimates. Residual predicates stay unknown.
    let mut pending = Vec::with_capacity(additional.len() + usize::from(on.is_some()));
    pending.extend(additional.iter().copied());
    pending.extend(on);
    let mut fraction = 1.0_f64;
    while let Some(expr) = pending.pop() {
        if let LogicalExpr::BinaryOp { left, op: BinaryOp::And, right, .. } = expr {
            pending.extend([left.as_ref(), right.as_ref()]);
        } else if let Some(value) = equality(left, right, expr) {
            fraction = fraction.min(value);
        } else {
            return Estimate::unknown(EstimateSource::UnsupportedExpression);
        }
    }
    Estimate::model(fraction, EstimateSource::Join)
}

pub(super) fn join<'a>(
    left: RelationEstimate<'a>, right: RelationEstimate<'a>,
    kind: &JoinType, on: Option<&LogicalExpr>, schema: &VeinSchema,
) -> RelationEstimate<'a> {
    join_with_predicates(left, right, kind, on, &[], schema)
}

/// Estimates an inner join with predicates crossing the selected relation
/// subsets. The same selectivity and distribution rules power ordinary joins.
pub(crate) fn inner_join<'a>(
    left: RelationEstimate<'a>,
    right: RelationEstimate<'a>,
    predicates: &[&LogicalExpr],
    schema: &VeinSchema,
) -> RelationEstimate<'a> {
    join_with_predicates(left, right, &JoinType::Inner, None, predicates, schema)
}

fn join_with_predicates<'a>(
    mut left: RelationEstimate<'a>,
    mut right: RelationEstimate<'a>,
    kind: &JoinType,
    on: Option<&LogicalExpr>,
    additional: &[&LogicalExpr],
    schema: &VeinSchema,
) -> RelationEstimate<'a> {
    use JoinType::*;
    let product = if left.rows.value() == Some(0.0) {
        left.rows
    } else if right.rows.value() == Some(0.0) {
        right.rows
    } else {
        left.rows.combine(right.rows, EstimateSource::Join, |l, r| l * r)
    };
    let no_predicates = on.is_none() && additional.is_empty();
    let matches = if *kind == Cross || no_predicates || product.value() == Some(0.0) {
        product
    } else {
        product.combine(
            join_selectivity(&left, &right, on, additional),
            EstimateSource::Join,
            |p, s| p * s,
        )
    };
    let rows = match kind {
        Inner | Cross => matches,
        Left => matches.combine(left.rows, EstimateSource::Join, f64::max),
        Right => matches.combine(right.rows, EstimateSource::Join, f64::max),
        Full if left.rows.value() == Some(0.0) => right.rows,
        Full if right.rows.value() == Some(0.0) => left.rows,
        Full => matches.combine(left.rows, EstimateSource::Join, f64::max)
            .combine(right.rows, EstimateSource::Join, f64::max),
        Semi => matches.combine(left.rows, EstimateSource::Join, f64::min),
        Anti => matches.combine(left.rows, EstimateSource::Join, |m, l| (l - m.min(l)).max(0.0)),
        Single => left.rows,
    };
    left.subset(rows);
    right.subset(rows);
    if matches!(kind, Right | Full) {
        for stats in left.columns.values_mut() { stats.null_fraction = None; }
    }
    if matches!(kind, Left | Full | Single) {
        for stats in right.columns.values_mut() { stats.null_fraction = None; }
    }
    if !matches!(kind, Semi | Anti) { left.columns.extend(right.columns); }
    left.retain_schema(schema);
    left.rows = rows;
    left.selectivity = None;
    left
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parser::ast::expr::BinaryOp;
    use plan::{ColumnId, LogicalExpr, ResolvedColumn, VeinDataType};

    use super::{join_selectivity, Estimate, EstimateSource};
    use crate::estimation::columns::{ColumnEstimate, RelationEstimate};
    use crate::estimation::EstimateConfidence;

    fn relation(column_id: usize) -> RelationEstimate<'static> {
        let mut relation = RelationEstimate::new(Estimate::known(
            100.0,
            EstimateSource::Statistics,
            EstimateConfidence::Statistics,
        ));
        relation.columns.insert(
            ColumnId(column_id),
            ColumnEstimate {
                distinct: Some(10.0),
                null_fraction: Some(0.0),
                min: None,
                max: None,
            },
        );
        relation
    }

    fn key_equality() -> LogicalExpr {
        LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(Arc::new(ResolvedColumn::new(
                ColumnId(1),
                "left_key",
                None,
                VeinDataType::Int64,
            )))),
            op: BinaryOp::Eq,
            right: Box::new(LogicalExpr::Column(Arc::new(ResolvedColumn::new(
                ColumnId(2),
                "right_key",
                None,
                VeinDataType::Int64,
            )))),
            return_type: VeinDataType::Boolean,
        }
    }

    fn exact_join_rows(left: &[u64], right: &[u64]) -> u64 {
        left.iter()
            .zip(right)
            .map(|(left, right)| left * right)
            .sum()
    }

    #[test]
    fn ndv_model_exposes_its_uniformity_limit_under_skew() {
        let left = relation(1);
        let right = relation(2);
        let predicate = key_equality();
        let selectivity = join_selectivity(&left, &right, None, &[&predicate]);
        let estimated_rows = left.rows.value().unwrap()
            * right.rows.value().unwrap()
            * selectivity.value().unwrap();

        let uniform = [10_u64; 10];
        let mut aligned_skew = [1_u64; 10];
        aligned_skew[0] = 91;
        let uniform_actual = exact_join_rows(&uniform, &uniform);
        let skewed_actual = exact_join_rows(&aligned_skew, &aligned_skew);

        assert_eq!(estimated_rows, 1_000.0);
        assert_eq!(uniform_actual, 1_000);
        assert_eq!(skewed_actual, 8_290);
        assert_eq!(selectivity.confidence, EstimateConfidence::Model);
    }
}
