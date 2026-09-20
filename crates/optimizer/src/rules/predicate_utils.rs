//! Shared analysis helpers for safe predicate rewrites.

use std::collections::{HashMap, HashSet};

use plan::LogicalExpr;

/// Collects current-scope columns, rejecting correlated and subquery expressions.
pub(crate) fn referenced_columns(expr: &LogicalExpr) -> Option<HashSet<plan::ColumnId>> {
    if expr.is_volatile() {
        return None;
    }

    let mut columns = HashSet::new();
    collect_referenced_columns(expr, &mut columns)?;
    Some(columns)
}

/// Substitutes current-scope columns while rejecting correlated expressions.
pub(crate) fn substitute_columns(
    expr: &LogicalExpr,
    mapping: &HashMap<plan::ColumnId, LogicalExpr>,
) -> Option<LogicalExpr> {
    if let LogicalExpr::Column(column) = expr {
        return mapping.get(&column.id).cloned();
    }
    if matches!(
        expr,
        LogicalExpr::OuterColumn(_) | LogicalExpr::ScalarSubquery(_) | LogicalExpr::Exists { .. }
    ) {
        return None;
    }

    expr.map_children(&mut |child| {
        substitute_columns(&child, mapping)
            .ok_or_else(|| error::VeinError::internal("predicate substitution failed"))
    })
    .ok()
}

fn collect_referenced_columns(
    expr: &LogicalExpr,
    output: &mut HashSet<plan::ColumnId>,
) -> Option<()> {
    match expr {
        LogicalExpr::Column(column) => {
            output.insert(column.id);
            Some(())
        }
        LogicalExpr::OuterColumn(_)
        | LogicalExpr::ScalarSubquery(_)
        | LogicalExpr::Exists { .. } => None,
        _ => {
            for child in expr.children() {
                collect_referenced_columns(child, output)?;
            }
            Some(())
        }
    }
}
