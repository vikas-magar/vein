//! Trace join output keys through row-preserving wrappers to grouping inputs.

use super::domain::project_keys;
use plan::{ColumnId, LogicalExpr, LogicalPlan, PlanTraversal};

pub(super) struct GroupedInput<'a> {
    pub input: &'a LogicalPlan,
    pub keys: Vec<ColumnId>,
    pub depth: usize,
}

pub(super) fn locate(plan: &LogicalPlan, keys: Vec<ColumnId>) -> Option<GroupedInput<'_>> {
    match plan {
        LogicalPlan::Project { input, exprs, schema } => {
            if exprs.iter().any(LogicalExpr::is_volatile) { return None; }
            let keys = project_keys(&keys, exprs, schema)?;
            let mut result = locate(input, keys)?;
            result.depth += 1;
            Some(result)
        }
        LogicalPlan::Filter { input, predicate } if predicate.is_reorder_safe() => {
            let mut result = locate(input, keys)?;
            result.depth += 1;
            Some(result)
        }
        LogicalPlan::Aggregate { input, grouping_sets, schema, aggs } => {
            let [groups] = grouping_sets.as_slice() else { return None; };
            if groups.is_empty() || aggs.iter().any(LogicalExpr::is_volatile) { return None; }
            let keys = keys.iter().map(|key| {
                let index = schema.position_of_id(*key)?;
                let LogicalExpr::Column(column) = groups.get(index)? else { return None; };
                input.schema().position_of_id(column.id)?;
                Some(column.id)
            }).collect::<Option<Vec<_>>>()?;
            Some(GroupedInput { input, keys, depth: 0 })
        }
        _ => None,
    }
}
