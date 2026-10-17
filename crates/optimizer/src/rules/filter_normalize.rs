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

        if rewritten == *plan {
            Ok(None)
        } else {
            Ok(Some(rewritten))
        }
    }
}

/// Normalizes a filter and any directly nested filter nodes.
fn normalize_filter(input: &LogicalPlan, predicate: &LogicalExpr) -> LogicalPlan {
    let mut conjuncts = Vec::new();
    let mut base = input.clone();

    // Consecutive filters are equivalent to one conjunction. Pull them into
    // one canonical chain only when all predicates are deterministic.
    let mut nested = Vec::new();
    while let LogicalPlan::Filter {
            input: nested_input,
            predicate: nested_predicate,
        } = &base
    {
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
    for conjunct in conjuncts {
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
mod tests;
