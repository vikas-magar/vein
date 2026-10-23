//! Relation ownership and exact-once placement of join conjuncts.
use super::*;
use crate::rules::predicate_analysis::conjuncts;
use crate::rules::predicate_utils::referenced_columns;

/// A conjunct belongs at the lowest split covering its complete relation set.
pub(super) fn crosses(relations: usize, left: usize, right: usize) -> bool {
    relations & (left | right) == relations
        && relations & left != 0 && relations & right != 0
}

/// Every executable split must retain a direct hash-key equality.
pub(super) fn has_edge(left: usize, right: usize, adjacency: &[usize]) -> bool {
    adjacency.iter().enumerate().any(|(index, neighbors)| {
        left & (1usize << index) != 0 && neighbors & right != 0
    })
}

pub(super) fn collect<'a>(
    on: &'a LogicalExpr,
    columns: &HashMap<plan::ColumnId, usize>,
    original_mask: usize,
    adjacency: &mut [usize],
) -> Option<Vec<JoinPredicate<'a>>> {
    let mut predicates = Vec::new();
    for expression in conjuncts(on) {
        let mut relations = 0usize;
        for column in referenced_columns(expression)? {
            relations |= 1usize << columns.get(&column)?;
        }
        if relations & original_mask != relations { return None; }
        if let LogicalExpr::BinaryOp { left, op: BinaryOp::Eq, right, .. } = expression {
            if let (LogicalExpr::Column(a), LogicalExpr::Column(b)) = (left.as_ref(), right.as_ref()) {
                let (&a, &b) = (columns.get(&a.id)?, columns.get(&b.id)?);
                if a != b {
                    adjacency[a] |= 1usize << b;
                    adjacency[b] |= 1usize << a;
                }
            }
        }
        // A constant or one-input ON term remains attached to its original
        // join's relation set instead of disappearing inside a leaf.
        if relations.count_ones() < 2 { relations = original_mask; }
        predicates.push(JoinPredicate { expression, relations });
    }
    Some(predicates)
}
