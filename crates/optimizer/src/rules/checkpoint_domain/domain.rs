//! Locate a completed population that contains every relevant outer key tuple.

use parser::ast::pipe_op::JoinType;
use plan::{ColumnId, LogicalExpr, LogicalPlan, PlanTraversal, ResolvedColumnRef, VeinSchema};

pub(super) struct Domain<'a> {
    pub source: &'a LogicalPlan,
    pub keys: Vec<ResolvedColumnRef>,
    pub source_id: u128,
    pub rows: u64,
}

pub(super) fn find<'a>(plan: &'a LogicalPlan, keys: &[ColumnId]) -> Option<Domain<'a>> {
    // All keys must come from the same retained population. Independent key
    // sets would lose composite-key correlations and are not a tuple domain.
    if keys.iter().any(|key| plan.schema().position_of_id(*key).is_none()) { return None; }
    match plan {
        LogicalPlan::Intermediate { source_id, row_count, schema } => Some(Domain {
            source: plan,
            source_id: *source_id,
            rows: *row_count,
            keys: keys.iter().map(|key| {
                schema.fields.get(schema.position_of_id(*key)?).cloned()
            }).collect::<Option<Vec<_>>>()?,
        }),
        LogicalPlan::Filter { input, predicate } if predicate.is_reorder_safe() => find(input, keys),
        LogicalPlan::Project { input, exprs, schema } => find(input, &project_keys(keys, exprs, schema)?),
        LogicalPlan::SubqueryAlias { input, schema, .. } => {
            let keys = keys.iter().map(|key| {
                input.schema().fields.get(schema.position_of_id(*key)?).map(|column| column.id)
            }).collect::<Option<Vec<_>>>()?;
            find(input, &keys)
        }
        LogicalPlan::Join { join_type: JoinType::Inner, left, right, on: Some(on), using, .. }
        if using.is_empty() && on.is_reorder_safe() => {
            let in_left = keys.iter().all(|key| left.schema().position_of_id(*key).is_some());
            let in_right = keys.iter().all(|key| right.schema().position_of_id(*key).is_some());
            match (in_left, in_right) {
                (true, false) => find(left, keys),
                (false, true) => find(right, keys),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Map selected output identities through a pure column projection.
pub(super) fn project_keys(
    keys: &[ColumnId], exprs: &[LogicalExpr], schema: &VeinSchema,
) -> Option<Vec<ColumnId>> {
    if exprs.len() != schema.fields.len() { return None; }
    keys.iter().map(|key| {
        let LogicalExpr::Column(column) = exprs.get(schema.position_of_id(*key)?)? else { return None; };
        Some(column.id)
    }).collect()
}
