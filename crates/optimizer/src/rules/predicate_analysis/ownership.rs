//! Identity-based ownership of predicates at a relational join boundary.

use std::collections::HashSet;
use parser::ast::pipe_op::JoinType;
use plan::{ColumnId, LogicalExpr, VeinSchema};

use crate::rules::predicate_utils::referenced_columns;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PredicateOwner { Constant, Left, Right, Both, Unknown }

pub(crate) struct JoinInputs {
    left: HashSet<ColumnId>,
    right: HashSet<ColumnId>,
}

impl JoinInputs {
    pub(crate) fn new(left: &VeinSchema, right: &VeinSchema) -> Self {
        Self {
            left: left.fields.iter().map(|column| column.id).collect(),
            right: right.fields.iter().map(|column| column.id).collect(),
        }
    }

    pub(crate) fn owner(&self, predicate: &LogicalExpr) -> PredicateOwner {
        let Some(columns) = referenced_columns(predicate) else { return PredicateOwner::Unknown };
        if columns.is_empty() { return PredicateOwner::Constant; }
        if columns.iter().any(|id| self.left.contains(id) == self.right.contains(id)) {
            return PredicateOwner::Unknown;
        }
        if columns.is_subset(&self.left) { PredicateOwner::Left } else if columns.is_subset(&self.right) { PredicateOwner::Right } else { PredicateOwner::Both }
    }
}

/// Which input can enforce an ON conjunct without altering preserved rows.
pub(crate) fn on_target(kind: JoinType, owner: PredicateOwner) -> Option<PredicateOwner> {
    use PredicateOwner as O;
    match (kind, owner) {
        (JoinType::Inner | JoinType::Semi | JoinType::Right, O::Left) => Some(O::Left),
        (JoinType::Inner | JoinType::Semi | JoinType::Left | JoinType::Anti, O::Right | O::Constant) => Some(O::Right),
        (JoinType::Right, O::Constant) => Some(O::Left),
        _ => None,
    }
}
