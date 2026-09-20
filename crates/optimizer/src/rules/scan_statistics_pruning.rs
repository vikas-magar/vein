//! Conservative statistics-based pruning for provably empty scans.

use std::cmp::Ordering;

use error::Result;
use parser::ast::expr::BinaryOp;
use plan::{LogicalExpr, LogicalLiteral, LogicalPlan};

use crate::context::OptimizerContext;
use crate::rule::{OptimizerRule, RuleType};

/// Replaces a scan with a zero-row limit when table statistics prove that a
/// deterministic filter cannot match any row.
pub struct ScanStatisticsPruningRule;

impl OptimizerRule for ScanStatisticsPruningRule {
    fn name(&self) -> &str {
        "ScanStatisticsPruningRule"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Heuristic
    }

    fn apply(
        &self,
        plan: &LogicalPlan,
        _context: &OptimizerContext,
    ) -> Result<Option<LogicalPlan>> {
        let LogicalPlan::Filter { input, predicate } = plan else {
            return Ok(None);
        };
        if predicate.is_volatile() {
            return Ok(None);
        }

        let LogicalPlan::Scan { table, schema, .. } = input.as_ref() else {
            return Ok(None);
        };
        let Some(statistics) = table.statistics.as_ref() else {
            return Ok(None);
        };

        if !predicate_is_provably_empty(predicate, statistics) {
            return Ok(None);
        }

        Ok(Some(LogicalPlan::Limit {
            input: Box::new(input.as_ref().clone()),
            offset: 0,
            count: 0,
            schema: schema.clone(),
        }))
    }
}

fn predicate_is_provably_empty(
    predicate: &LogicalExpr,
    statistics: &plan::TableStatistics,
) -> bool {
    match predicate {
        LogicalExpr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
            ..
        } => {
            predicate_is_provably_empty(left, statistics)
                || predicate_is_provably_empty(right, statistics)
        }
        LogicalExpr::BinaryOp {
            left, op, right, ..
        } => {
            let (column, operator, literal) = match (left.as_ref(), right.as_ref()) {
                (LogicalExpr::Column(column), LogicalExpr::Literal(literal)) => {
                    (column, *op, literal)
                }
                (LogicalExpr::Literal(literal), LogicalExpr::Column(column)) => {
                    (column, reverse_operator(*op), literal)
                }
                _ => return false,
            };
            let Some(column_statistics) = statistics.column_statistics.get(&column.name) else {
                return false;
            };
            let (Some(min), Some(max)) = (
                column_statistics.min_value.as_ref(),
                column_statistics.max_value.as_ref(),
            ) else {
                return false;
            };
            comparison_is_empty(operator, literal, min, max)
        }
        LogicalExpr::IsNull { expr } => {
            let LogicalExpr::Column(column) = expr.as_ref() else {
                return false;
            };
            statistics
                .column_statistics
                .get(&column.name)
                .and_then(|stats| stats.null_count)
                == Some(0)
        }
        LogicalExpr::IsNotNull { expr } => {
            let LogicalExpr::Column(column) = expr.as_ref() else {
                return false;
            };
            let Some(num_rows) = statistics.num_rows else {
                return false;
            };
            statistics
                .column_statistics
                .get(&column.name)
                .and_then(|stats| stats.null_count)
                .is_some_and(|nulls| nulls >= num_rows)
        }
        _ => false,
    }
}

fn reverse_operator(operator: BinaryOp) -> BinaryOp {
    match operator {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::Leq => BinaryOp::Geq,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::Geq => BinaryOp::Leq,
        operator => operator,
    }
}

fn comparison_is_empty(
    operator: BinaryOp,
    literal: &LogicalLiteral,
    min: &LogicalLiteral,
    max: &LogicalLiteral,
) -> bool {
    match operator {
        BinaryOp::Eq => {
            compare(literal, min).is_some_and(|ordering| ordering == Ordering::Less)
                || compare(literal, max).is_some_and(|ordering| ordering == Ordering::Greater)
        }
        BinaryOp::Neq => {
            compare(min, max) == Some(Ordering::Equal)
                && compare(literal, min) == Some(Ordering::Equal)
        }
        BinaryOp::Lt => compare(literal, min).is_some_and(|ordering| ordering != Ordering::Greater),
        BinaryOp::Leq => compare(literal, min).is_some_and(|ordering| ordering == Ordering::Less),
        BinaryOp::Gt => compare(literal, max).is_some_and(|ordering| ordering != Ordering::Less),
        BinaryOp::Geq => {
            compare(literal, max).is_some_and(|ordering| ordering == Ordering::Greater)
        }
        _ => false,
    }
}

fn compare(left: &LogicalLiteral, right: &LogicalLiteral) -> Option<Ordering> {
    match (left, right) {
        (LogicalLiteral::Boolean(left), LogicalLiteral::Boolean(right)) => Some(left.cmp(right)),
        (LogicalLiteral::Int64(left), LogicalLiteral::Int64(right)) => Some(left.cmp(right)),
        (LogicalLiteral::Float64(left), LogicalLiteral::Float64(right)) => left.partial_cmp(right),
        (LogicalLiteral::String(left), LogicalLiteral::String(right)) => Some(left.cmp(right)),
        (LogicalLiteral::Date(left), LogicalLiteral::Date(right)) => Some(left.cmp(right)),
        (LogicalLiteral::Timestamp(left), LogicalLiteral::Timestamp(right)) => {
            Some(left.cmp(right))
        }
        (LogicalLiteral::Time(left), LogicalLiteral::Time(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plan::{
        ColumnId, ColumnStatistics, ResolvedColumn, TableMetadata, TableStatistics, VeinDataType,
        VeinSchema,
    };
    use std::sync::Arc;

    fn scan_with_stats() -> (LogicalPlan, ResolvedColumn) {
        let column = ResolvedColumn::new(ColumnId(1), "id", None, VeinDataType::Int64);
        let schema = VeinSchema {
            fields: vec![column.clone()],
        };
        let mut table_stats = TableStatistics::default();
        table_stats.column_statistics.insert(
            "id".to_string(),
            ColumnStatistics {
                min_value: Some(LogicalLiteral::Int64(10)),
                max_value: Some(LogicalLiteral::Int64(20)),
                null_count: Some(0),
                ..ColumnStatistics::default()
            },
        );
        table_stats.num_rows = Some(11);
        let table = TableMetadata::new("t", Arc::new(schema.clone())).with_statistics(table_stats);
        (
            LogicalPlan::Scan {
                table: Arc::new(table),
                projection: None,
                filters: vec![],
                schema: Box::new(schema),
            },
            column,
        )
    }

    fn predicate(column: &ResolvedColumn, op: BinaryOp, value: i64) -> LogicalExpr {
        LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(column.clone())),
            op,
            right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(value))),
            return_type: VeinDataType::Boolean,
        }
    }

    #[test]
    fn prunes_out_of_range_equality() {
        let (scan, column) = scan_with_stats();
        let plan = LogicalPlan::Filter {
            input: Box::new(scan),
            predicate: predicate(&column, BinaryOp::Eq, 99),
        };
        let rewritten = ScanStatisticsPruningRule
            .apply(&plan, &OptimizerContext::default())
            .unwrap()
            .expect("statistics prove empty");
        assert!(matches!(rewritten, LogicalPlan::Limit { count: 0, .. }));
    }

    #[test]
    fn prunes_impossible_null_checks() {
        let (scan, column) = scan_with_stats();
        let plan = LogicalPlan::Filter {
            input: Box::new(scan),
            predicate: LogicalExpr::IsNull {
                expr: Box::new(LogicalExpr::Column(column)),
            },
        };
        assert!(matches!(
            ScanStatisticsPruningRule
                .apply(&plan, &OptimizerContext::default())
                .unwrap(),
            Some(LogicalPlan::Limit { count: 0, .. })
        ));
    }

    #[test]
    fn preserves_uncertain_predicates() {
        let (scan, column) = scan_with_stats();
        let plan = LogicalPlan::Filter {
            input: Box::new(scan),
            predicate: predicate(&column, BinaryOp::Gt, 15),
        };
        assert!(
            ScanStatisticsPruningRule
                .apply(&plan, &OptimizerContext::default())
                .unwrap()
                .is_none()
        );
    }
}
