//! Prove a necessary grouped-key restriction and suppress repeated insertion.

use super::{
    candidate::{self, GroupedSide},
    domain,
};
use crate::rules::join_transitivity::{equi_join_keys, schema_ids};
use parser::ast::pipe_op::JoinType;
use plan::{ColumnId, LogicalPlan, PlanTraversal, ResolvedColumnRef};

pub(super) struct Restriction {
    pub domain: LogicalPlan,
    pub keys: Vec<(ResolvedColumnRef, ResolvedColumnRef)>,
    pub source_id: u128,
    pub rows: u64,
    pub depth: usize,
    pub side: GroupedSide,
}

pub(super) fn prepare(plan: &LogicalPlan) -> Option<Restriction> {
    for candidate in candidate::candidates(plan) {
        let grouped = candidate.grouped;
        let Some(domain) = domain::find(candidate.probe, &candidate.probe_keys) else {
            continue;
        };
        if !schema_ids(grouped.input.schema()).is_disjoint(&schema_ids(domain.source.schema())) {
            continue;
        }
        let keys = grouped
            .keys
            .iter()
            .zip(domain.keys)
            .map(|(inner, outer)| {
                let column = grouped
                    .input
                    .schema()
                    .fields
                    .get(grouped.input.schema().position_of_id(*inner)?)?;
                if column.data_type != outer.data_type
                    || column.data_type.hash_join_key_type().is_none()
                {
                    return None;
                }
                Some((column.clone(), outer))
            })
            .collect::<Option<Vec<_>>>()?;
        let ids: Vec<_> = keys.iter().map(|(a, b)| (a.id, b.id)).collect();
        if already_restricted(grouped.input, domain.source_id, &ids) {
            continue;
        }
        return Some(Restriction {
            domain: domain.source.clone(),
            keys,
            source_id: domain.source_id,
            rows: domain.rows,
            depth: grouped.depth,
            side: candidate.side,
        });
    }
    None
}

fn already_restricted(plan: &LogicalPlan, source: u128, keys: &[(ColumnId, ColumnId)]) -> bool {
    if let LogicalPlan::Join {
        join_type: JoinType::Semi,
        left,
        right,
        on: Some(on),
        ..
    } = plan
    {
        if replay_source(right) == Some(source) {
            let existing =
                equi_join_keys(on, &schema_ids(left.schema()), &schema_ids(right.schema()));
            if keys.iter().all(|key| existing.contains(key)) {
                return true;
            }
        }
    }
    plan.children()
        .into_iter()
        .any(|child| already_restricted(child, source, keys))
}

fn replay_source(plan: &LogicalPlan) -> Option<u128> {
    match plan {
        LogicalPlan::Intermediate { source_id, .. } => Some(*source_id),
        LogicalPlan::Project { input, .. } => replay_source(input),
        _ => None,
    }
}
