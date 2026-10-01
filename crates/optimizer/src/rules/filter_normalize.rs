//! Normalization of filter predicates and filter plan nodes.
//!
//! Deterministic conjunctions are represented as independent filter nodes so
//! later predicate-pushdown rules can move them separately.

use error::Result;
use parser::ast::expr::BinaryOp;
use plan::{LogicalExpr, LogicalLiteral, LogicalPlan};

use crate::context::OptimizerContext;
use crate::rule::{OptimizerRule, RuleType};

/// A heuristic rule that canonicalizes filter predicates.
pub struct FilterNormalizationRule;

impl OptimizerRule for FilterNormalizationRule {
    fn name(&self) -> &str {
        "FilterNormalizationRule"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Heuristic
    }

    fn apply(
        &self,
        plan: &LogicalPlan,
        _context: &OptimizerContext,
    ) -> Result<Option<LogicalPlan>> {
        let rewritten = match plan {
            LogicalPlan::Filter { input, predicate } => normalize_filter(input, predicate),
            LogicalPlan::Scan {
                table,
                projection,
                filters,
                schema,
            } => LogicalPlan::Scan {
                table: table.clone(),
                projection: projection.clone(),
                filters: normalize_filter_list(filters),
                schema: schema.clone(),
            },
            _ => plan.clone(),
        };

        Ok((rewritten != *plan).then_some(rewritten))
    }
}

/// Normalizes a filter and any directly nested filter nodes.
fn normalize_filter(input: &LogicalPlan, predicate: &LogicalExpr) -> LogicalPlan {
    let mut conjuncts = Vec::new();
    let mut base = input.clone();

    // Consecutive filters are equivalent to one conjunction. Pull them into
    // one canonical chain only when all predicates are deterministic.
    let mut nested = Vec::new();
    loop {
        let LogicalPlan::Filter {
            input: nested_input,
            predicate: nested_predicate,
        } = &base
        else {
            break;
        };
        if nested_predicate.is_volatile() {
            return LogicalPlan::Filter {
                input: Box::new(base),
                predicate: predicate.clone(),
            };
        }
        nested.push(nested_predicate.clone());
        base = (**nested_input).clone();
    }

    for nested_predicate in nested.into_iter().rev() {
        flatten_and(&nested_predicate, &mut conjuncts);
    }
    flatten_and(predicate, &mut conjuncts);

    if conjuncts.iter().any(|expr| is_boolean_literal(expr, false)) {
        return LogicalPlan::Filter {
            input: Box::new(base),
            predicate: boolean_literal(false),
        };
    }

    conjuncts.retain(|expr| !is_boolean_literal(expr, true));
    deduplicate_deterministic(&mut conjuncts);

    if conjuncts.is_empty() {
        return base;
    }

    build_filter_chain(base, conjuncts)
}

/// Normalizes filters already attached to a scan.
fn normalize_filter_list(filters: &[LogicalExpr]) -> Vec<LogicalExpr> {
    let mut conjuncts = Vec::new();
    for filter in filters {
        flatten_and(filter, &mut conjuncts);
    }

    if conjuncts.iter().any(|expr| is_boolean_literal(expr, false)) {
        return vec![boolean_literal(false)];
    }

    conjuncts.retain(|expr| !is_boolean_literal(expr, true));
    deduplicate_deterministic(&mut conjuncts);
    conjuncts
}

/// Flattens deterministic AND trees while preserving conjunct order.
fn flatten_and(expr: &LogicalExpr, output: &mut Vec<LogicalExpr>) {
    if expr.is_volatile() {
        output.push(expr.clone());
        return;
    }

    match expr {
        LogicalExpr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
            ..
        } => {
            flatten_and(left, output);
            flatten_and(right, output);
        }
        _ => output.push(expr.clone()),
    }
}

/// Removes duplicate deterministic conjuncts without changing volatile calls.
fn deduplicate_deterministic(conjuncts: &mut Vec<LogicalExpr>) {
    let mut unique = Vec::with_capacity(conjuncts.len());
    for conjunct in conjuncts.drain(..) {
        if conjunct.is_volatile() || !unique.contains(&conjunct) {
            unique.push(conjunct);
        }
    }
    *conjuncts = unique;
}

/// Builds a filter chain whose first conjunct is evaluated closest to input.
fn build_filter_chain(mut input: LogicalPlan, conjuncts: Vec<LogicalExpr>) -> LogicalPlan {
    for conjunct in conjuncts.into_iter().rev() {
        input = LogicalPlan::Filter {
            input: Box::new(input),
            predicate: conjunct,
        };
    }
    input
}

fn is_boolean_literal(expr: &LogicalExpr, value: bool) -> bool {
    matches!(
        expr,
        LogicalExpr::Literal(LogicalLiteral::Boolean(actual)) if *actual == value
    )
}

fn boolean_literal(value: bool) -> LogicalExpr {
    LogicalExpr::Literal(LogicalLiteral::Boolean(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use plan::{ColumnId, ResolvedColumn, VeinDataType};

    fn scan() -> LogicalPlan {
        let schema = Box::new(plan::VeinSchema { fields: vec![] });
        let table = std::sync::Arc::new(plan::TableMetadata::new(
            "t",
            std::sync::Arc::new(*schema.clone()),
        ));
        LogicalPlan::Scan {
            table,
            projection: None,
            filters: vec![],
            schema,
        }
    }

    fn bool_column(id: usize, name: &str) -> LogicalExpr {
        LogicalExpr::Column(std::sync::Arc::new(ResolvedColumn {
            id: ColumnId(id),
            name: name.to_string(),
            qualifier: None,
            data_type: VeinDataType::Boolean,
            is_factorized: false,
            nullable: true,
        }))
    }

    fn and(left: LogicalExpr, right: LogicalExpr) -> LogicalExpr {
        LogicalExpr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::And,
            right: Box::new(right),
            return_type: VeinDataType::Boolean,
        }
    }

    fn filter(input: LogicalPlan, predicate: LogicalExpr) -> LogicalPlan {
        LogicalPlan::Filter {
            input: Box::new(input),
            predicate,
        }
    }

    fn predicates(plan: &LogicalPlan) -> Vec<&LogicalExpr> {
        let mut result = Vec::new();
        let mut current = plan;
        while let LogicalPlan::Filter { input, predicate } = current {
            result.push(predicate);
            current = input;
        }
        result
    }

    #[test]
    fn splits_and_and_removes_true() {
        let a = bool_column(1, "a");
        let b = bool_column(2, "b");
        let plan = filter(
            scan(),
            and(
                a.clone(),
                and(
                    LogicalExpr::Literal(LogicalLiteral::Boolean(true)),
                    b.clone(),
                ),
            ),
        );

        let normalized = normalize_filter(
            match &plan {
                LogicalPlan::Filter { input, .. } => input,
                _ => unreachable!(),
            },
            match &plan {
                LogicalPlan::Filter { predicate, .. } => predicate,
                _ => unreachable!(),
            },
        );
        assert_eq!(predicates(&normalized), vec![&a, &b]);
    }

    #[test]
    fn removes_true_filter_and_keeps_false_canonical() {
        assert_eq!(
            normalize_filter(
                &scan(),
                &LogicalExpr::Literal(LogicalLiteral::Boolean(true))
            ),
            scan()
        );
        let normalized = normalize_filter(
            &scan(),
            &and(
                bool_column(1, "a"),
                LogicalExpr::Literal(LogicalLiteral::Boolean(false)),
            ),
        );
        let false_predicate = boolean_literal(false);
        assert_eq!(predicates(&normalized), vec![&false_predicate]);
    }

    #[test]
    fn removes_duplicate_deterministic_conjuncts() {
        let a = bool_column(1, "a");
        let normalized = normalize_filter(&scan(), &and(a.clone(), a.clone()));
        assert_eq!(predicates(&normalized), vec![&a]);
    }

    #[test]
    fn preserves_volatile_predicate_as_atomic() {
        let random = LogicalExpr::FunctionCall {
            name: "random".to_string(),
            args: vec![],
            is_aggregate: false,
            distinct: false,
            return_type: VeinDataType::Boolean,
        };
        let predicate = and(bool_column(1, "a"), random.clone());
        let normalized = normalize_filter(&scan(), &predicate);
        assert_eq!(predicates(&normalized), vec![&predicate]);
    }

    #[test]
    fn normalizes_scan_filter_list() {
        let a = bool_column(1, "a");
        let b = bool_column(2, "b");
        assert_eq!(
            normalize_filter_list(&[and(a.clone(), b.clone()), a.clone()]),
            vec![a, b]
        );
    }
}
