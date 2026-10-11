//! Small column summaries borrow literal bounds; column payloads are never read.

use std::collections::HashMap;
use plan::{ColumnId, ColumnStatistics, LogicalExpr, LogicalLiteral, VeinSchema};
use parser::ast::expr::BinaryOp;
use super::{Estimate, EstimateSource};

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ColumnEstimate<'a> {
    pub distinct: Option<f64>,
    pub null_fraction: Option<f64>,
    pub min: Option<&'a LogicalLiteral>,
    pub max: Option<&'a LogicalLiteral>,
}

impl<'a> ColumnEstimate<'a> {
    pub fn from_statistics(stats: &'a ColumnStatistics, rows: u64, nullable: bool) -> Option<Self> {
        if !crate::statistics::valid_column(stats, rows, nullable) { return None; }
        let nulls = stats.null_count;
        let min = stats.min_value.as_ref();
        let max = stats.max_value.as_ref();
        Some(Self {
            distinct: stats.distinct_count.map(|d| d as f64),
            null_fraction: nulls.map(|n| if rows == 0 { 0.0 } else { n as f64 / rows as f64 })
                .or((!nullable).then_some(0.0)),
            min,
            max,
        })
    }

    pub fn after_subset(&mut self, rows: Estimate) {
        self.distinct = self.distinct.zip(rows.value()).map(|(d, r)| d.min(r));
        if self.null_fraction != Some(0.0) {
            self.null_fraction = None;
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RelationEstimate<'a> {
    pub rows: Estimate,
    pub columns: HashMap<ColumnId, ColumnEstimate<'a>>,
    pub selectivity: Option<Estimate>,
}

impl<'a> RelationEstimate<'a> {
    pub fn new(rows: Estimate) -> Self {
        Self { rows, columns: HashMap::new(), selectivity: None }
    }

    pub fn subset(&mut self, rows: Estimate) {
        self.rows = rows;
        for column in self.columns.values_mut() {
            column.after_subset(rows);
        }
    }

    pub fn retain_schema(&mut self, schema: &VeinSchema) {
        self.columns.retain(|id, _| schema.fields.iter().any(|c| c.id == *id));
    }

    /// Checks whether both summaries provide the same inputs to a later join estimate.
    /// Selectivity is excluded because the join estimator uses row counts and
    /// column distributions/bounds, then recomputes selectivity for each join.
    pub fn has_same_join_summary(&self, other: &Self) -> bool {
        self.rows == other.rows && self.columns == other.columns
    }

    /// Propagates predicate-implied distributions without reading any values.
    pub fn restrict(&mut self, predicate: &'a LogicalExpr) {
        let mut pending = vec![predicate];
        while let Some(expr) = pending.pop() {
            match expr {
                LogicalExpr::BinaryOp { left, op: BinaryOp::And, right, .. } =>
                    pending.extend([left.as_ref(), right.as_ref()]),
                LogicalExpr::BinaryOp { left, op: BinaryOp::Eq, right, .. } => {
                    let (column, value) = match (left.as_ref(), right.as_ref()) {
                        (LogicalExpr::Column(c), LogicalExpr::Literal(v))
                        | (LogicalExpr::Literal(v), LogicalExpr::Column(c)) => (c, v),
                        _ => continue,
                    };
                    if !matches!(value, LogicalLiteral::Null(_)) {
                        if let Some(stats) = self.columns.get_mut(&column.id) {
                            stats.distinct = Some(self.rows.value().unwrap_or(1.0).min(1.0));
                            stats.null_fraction = Some(0.0);
                            stats.min = Some(value);
                            stats.max = Some(value);
                        }
                    }
                }
                LogicalExpr::IsNull { expr: operand } | LogicalExpr::IsNotNull { expr: operand } => {
                    if let LogicalExpr::Column(column) = operand.as_ref() {
                        if let Some(stats) = self.columns.get_mut(&column.id) {
                            let is_null = matches!(expr, LogicalExpr::IsNull { .. });
                            stats.null_fraction = Some(if is_null { 1.0 } else { 0.0 });
                            if is_null {
                                stats.distinct = Some(0.0);
                                stats.min = None;
                                stats.max = None;
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    pub fn unknown() -> Self {
        Self::new(Estimate::unknown(EstimateSource::UnsupportedOperator))
    }
}

#[cfg(test)]
mod tests {
    use plan::ColumnId;

    use super::*;
    use crate::estimation::EstimateConfidence;

    fn relation(distinct: f64) -> RelationEstimate<'static> {
        let mut relation = RelationEstimate::new(Estimate::known(
            50.0,
            EstimateSource::Join,
            EstimateConfidence::Model,
        ));
        relation.columns.insert(
            ColumnId(1),
            ColumnEstimate {
                distinct: Some(distinct),
                null_fraction: Some(0.0),
                min: None,
                max: None,
            },
        );
        relation
    }

    #[test]
    fn join_summary_comparison_includes_column_distributions() {
        let first = relation(20.0);
        let equivalent = relation(20.0);
        let different_distribution = relation(10.0);

        assert!(first.has_same_join_summary(&equivalent));
        assert!(!first.has_same_join_summary(&different_distribution));
    }
}
