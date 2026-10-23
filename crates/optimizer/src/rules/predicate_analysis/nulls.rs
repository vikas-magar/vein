//! Conservative abstract interpretation with selected columns replaced by NULL.

use std::collections::HashSet;

use parser::ast::expr::BinaryOp;
use plan::{ColumnId, LogicalExpr, LogicalLiteral};

use super::truth::TruthSet;

const MAX_DEPTH: usize = 100;

pub(super) fn truth_values(expr: &LogicalExpr, columns: &HashSet<ColumnId>, depth: usize) -> TruthSet {
    use LogicalExpr as E;
    if depth >= MAX_DEPTH { return TruthSet::ANY; }
    let next = depth + 1;
    match expr {
        E::Literal(LogicalLiteral::Boolean(true)) => TruthSet::TRUE,
        E::Literal(LogicalLiteral::Boolean(false)) => TruthSet::FALSE,
        E::Literal(LogicalLiteral::Null(_)) => TruthSet::NULL,
        E::Column(column) if columns.contains(&column.id) => TruthSet::NULL,
        E::Not { expr } => truth_values(expr, columns, next).not(),
        E::BinaryOp { left, op: BinaryOp::And | BinaryOp::Or, right, .. } => {
            let E::BinaryOp { op, .. } = expr else { unreachable!() };
            truth_values(left, columns, next).combine(
                truth_values(right, columns, next), *op == BinaryOp::And)
        }
        E::BinaryOp { left, op, right, .. } if matches!(op,
            BinaryOp::Eq | BinaryOp::Neq | BinaryOp::Lt | BinaryOp::Leq | BinaryOp::Gt | BinaryOp::Geq
        ) => strict_result([left.as_ref(), right.as_ref()], columns, next),
        E::Like { expr, pattern, .. } => strict_result([expr.as_ref(), pattern.as_ref()], columns, next),
        E::InList { expr, list, negated } if !list.is_empty() => {
            if definitely_null(expr, columns, next) { TruthSet::NULL } else if *negated && list.iter().any(|item| definitely_null(item, columns, next)) {
                // A NULL list element prevents NOT IN from ever being TRUE.
                TruthSet::NON_TRUE
            } else { TruthSet::ANY }
        }
        E::IsNull { expr } if definitely_null(expr, columns, next) => TruthSet::TRUE,
        E::IsNotNull { expr } if definitely_null(expr, columns, next) => TruthSet::FALSE,
        E::IsNull { .. } | E::IsNotNull { .. } => TruthSet::BOOLEAN,
        E::Cast { expr, .. } => truth_values(expr, columns, next),
        _ => TruthSet::ANY,
    }
}

fn strict_result<'a>(
    operands: impl IntoIterator<Item=&'a LogicalExpr>,
    columns: &HashSet<ColumnId>,
    depth: usize,
) -> TruthSet {
    if operands.into_iter().any(|expr| definitely_null(expr, columns, depth)) {
        TruthSet::NULL
    } else { TruthSet::ANY }
}

fn definitely_null(expr: &LogicalExpr, columns: &HashSet<ColumnId>, depth: usize) -> bool {
    if depth >= MAX_DEPTH { return false; }
    match expr {
        LogicalExpr::Column(column) => columns.contains(&column.id),
        LogicalExpr::Literal(LogicalLiteral::Null(_)) => true,
        LogicalExpr::Cast { expr, .. } | LogicalExpr::Negate { expr } => {
            definitely_null(expr, columns, depth + 1)
        }
        _ => false,
    }
}
