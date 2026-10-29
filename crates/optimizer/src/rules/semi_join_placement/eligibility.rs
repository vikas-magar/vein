//! Ownership and evaluation-safety proofs for semi/inner reassociation.

use std::collections::HashSet;

use parser::ast::pipe_op::JoinType;
use plan::{ColumnId, LogicalPlan, PlanTraversal, VeinSchema};

use crate::rules::predicate_analysis::PredicateSemantics;
use crate::rules::predicate_utils::referenced_columns;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Input { Left, Right }

pub(super) fn target<S: PredicateSemantics>(plan: &LogicalPlan, semantics: &S) -> Option<Input> {
    let LogicalPlan::Join {
        join_type: JoinType::Semi, left: probe, right: build,
        on: Some(predicate), using, ..
    } = plan else { return None; };
    if !using.is_empty() || !semantics.reorder_safe(predicate) {
        return None;
    }
    let LogicalPlan::Join {
        join_type: JoinType::Inner, left, right, on: Some(inner_on), using, ..
    } = probe.as_ref() else { return None; };
    if !using.is_empty() || !semantics.reorder_safe(inner_on) {
        return None;
    }
    let left = ids(left.schema())?;
    let right = ids(right.schema())?;
    let build = ids(build.schema())?;
    if !left.is_disjoint(&right) || !left.is_disjoint(&build) || !right.is_disjoint(&build) {
        return None;
    }
    let referenced = referenced_columns(predicate)?;
    if referenced.iter().any(|id| !left.contains(id) && !right.contains(id) && !build.contains(id)) {
        return None;
    }
    // A build-only or constant predicate has no unique probe input. Decline
    // rather than inventing a placement or changing evaluation multiplicity.
    let probe: HashSet<_> = referenced.difference(&build).copied().collect();
    if probe.is_empty() {
        None
    } else if probe.is_subset(&left) {
        Some(Input::Left)
    } else if probe.is_subset(&right) {
        Some(Input::Right)
    } else {
        None
    }
}

fn ids(schema: &VeinSchema) -> Option<HashSet<ColumnId>> {
    let ids: HashSet<_> = schema.fields.iter().map(|column| column.id).collect();
    (ids.len() == schema.fields.len()).then_some(ids)
}
