//! Pushes safe outer predicates through adjacent filters to their input operator.

use error::Result;
use plan::{LogicalExpr, LogicalPlan};

use crate::context::OptimizerContext;
use crate::rule::OptimizerRule;

use super::PredicatePushdownRule;

pub(super) fn push_through_filter_chain(
    input: &LogicalPlan,
    predicate: &LogicalExpr,
    context: &OptimizerContext,
) -> Result<Option<LogicalPlan>> {
    if !predicate.is_reorder_safe() {
        return Ok(None);
    }

    let mut base = input;
    let mut filters = Vec::new();
    while let LogicalPlan::Filter {
        input: nested_input,
        predicate: nested_predicate,
    } = base
    {
        if !nested_predicate.is_reorder_safe() {
            return Ok(None);
        }
        filters.push(nested_predicate);
        base = nested_input;
    }

    if filters.is_empty() {
        return Ok(None);
    }

    let candidate = LogicalPlan::Filter {
        input: Box::new(base.clone()),
        predicate: predicate.clone(),
    };
    let Some(mut rewritten) = PredicatePushdownRule.apply(&candidate, context)? else {
        return Ok(None);
    };

    for filter in filters.into_iter().rev() {
        rewritten = LogicalPlan::Filter {
            input: Box::new(rewritten),
            predicate: filter.clone(),
        };
    }
    Ok(Some(rewritten))
}
