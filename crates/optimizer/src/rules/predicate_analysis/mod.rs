//! Shared predicate ownership and semantic proofs for relational rewrites.

use std::collections::HashSet;

use parser::ast::expr::BinaryOp;
use plan::{ColumnId, LogicalExpr, VeinDataType};

mod nulls;
mod truth;
mod ownership;
pub(crate) use ownership::{JoinInputs, PredicateOwner, on_target};

/// Semantic extension point used by join simplification.
/// Implementations may decline proofs; an incorrect positive changes results.
pub trait PredicateSemantics {
    /// Whether changing this expression's evaluation point is safe.
    fn reorder_safe(&self, predicate: &LogicalExpr) -> bool;
    /// Proves that replacing these columns by NULL prevents a TRUE result.
    fn rejects_nulls(&self, predicate: &LogicalExpr, columns: &HashSet<ColumnId>) -> bool;
}

/// Conservative semantics for built-in predicates; unknown functions yield no proof.
#[derive(Default)]
pub struct BuiltinPredicateSemantics;

impl PredicateSemantics for BuiltinPredicateSemantics {
    fn reorder_safe(&self, predicate: &LogicalExpr) -> bool {
        predicate.is_reorder_safe() && !predicate.is_volatile()
    }

    fn rejects_nulls(&self, predicate: &LogicalExpr, columns: &HashSet<ColumnId>) -> bool {
        !nulls::truth_values(predicate, columns, 0).can_be_true()
    }
}

pub(crate) fn conjuncts(expression: &LogicalExpr) -> Vec<&LogicalExpr> {
    let mut result = Vec::new();
    let mut pending = vec![expression];
    while let Some(expression) = pending.pop() {
        if let LogicalExpr::BinaryOp { left, op: BinaryOp::And, right, .. } = expression {
            pending.push(right);
            pending.push(left);
        } else {
            result.push(expression);
        }
    }
    result
}

pub(crate) fn conjunction(terms: impl IntoIterator<Item=LogicalExpr>) -> Option<LogicalExpr> {
    terms.into_iter().reduce(|left, right| LogicalExpr::BinaryOp {
        left: Box::new(left),
        op: BinaryOp::And,
        right: Box::new(right),
        return_type: VeinDataType::Boolean,
    })
}

#[cfg(test)]
mod tests;
