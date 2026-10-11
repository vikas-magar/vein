//! Conservative statistics-based pruning for provably empty scans.

use std::cmp::Ordering;

use error::Result;
use parser::ast::expr::BinaryOp;
use plan::{LogicalExpr, LogicalLiteral, LogicalPlan};

use crate::context::{OptimizerContext, StatisticsFreshness};
use crate::statistics::{compare, reverse as reverse_operator, valid_column};
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
        context: &OptimizerContext,
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
        let Some(snapshot) = context.table_statistics(table) else {
            return Ok(None);
        };
        if snapshot.freshness == StatisticsFreshness::Stale {
            return Ok(None);
        }
        let statistics = snapshot.statistics;
        let Some(rows) = statistics.num_rows else { return Ok(None); };
        if schema.fields.iter().any(|c| statistics.column_statistics.get(&c.name)
            .is_some_and(|stats| !valid_column(stats, rows, c.nullable))) {
            return Ok(None);
        }

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


#[cfg(test)]
mod tests;
