//! Shared eligibility analysis for rewriting and checkpoint scheduling.

use super::aggregate::{self, GroupedInput};
use crate::rules::join_transitivity::{equi_join_keys, schema_ids};
use parser::ast::pipe_op::JoinType;
use plan::{ColumnId, LogicalPlan, PlanTraversal};

#[derive(Clone, Copy)]
pub(super) enum GroupedSide {
    Left,
    Right,
}

pub(super) struct Candidate<'a> {
    pub probe: &'a LogicalPlan,
    pub grouped: GroupedInput<'a>,
    pub probe_keys: Vec<ColumnId>,
    pub side: GroupedSide,
}

pub(super) fn candidates(plan: &LogicalPlan) -> impl Iterator<Item = Candidate<'_>> {
    [GroupedSide::Right, GroupedSide::Left]
        .into_iter()
        .filter_map(move |side| analyze(plan, side))
}

fn analyze(plan: &LogicalPlan, side: GroupedSide) -> Option<Candidate<'_>> {
    let LogicalPlan::Join {
        join_type,
        left,
        right,
        on: Some(on),
        using,
        ..
    } = plan
    else {
        return None;
    };
    if !using.is_empty() || !on.is_reorder_safe() {
        return None;
    }
    let (probe, grouped) = match (join_type, side) {
        (JoinType::Left | JoinType::Inner, GroupedSide::Right) => (left, right),
        (JoinType::Inner, GroupedSide::Left) => (right, left),
        _ => return None,
    };
    let pairs = equi_join_keys(
        on,
        &schema_ids(probe.schema()),
        &schema_ids(grouped.schema()),
    );
    if pairs.is_empty() {
        return None;
    }
    let grouped = aggregate::locate(grouped, pairs.iter().map(|pair| pair.1).collect())?;
    if !schema_ids(probe.schema()).is_disjoint(&schema_ids(grouped.input.schema())) {
        return None;
    }
    let probe_keys: Vec<_> = pairs.iter().map(|pair| pair.0).collect();
    for (probe_key, inner_key) in probe_keys.iter().zip(&grouped.keys) {
        let a = &probe.schema().fields[probe.schema().position_of_id(*probe_key)?];
        let b =
            &grouped.input.schema().fields[grouped.input.schema().position_of_id(*inner_key)?];
        if a.data_type != b.data_type || a.data_type.hash_join_key_type().is_none() {
            return None;
        }
    }
    Some(Candidate {
        probe,
        grouped,
        probe_keys,
        side,
    })
}
