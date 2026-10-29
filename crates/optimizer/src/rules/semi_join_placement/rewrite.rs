//! Owned tree surgery after the eligibility module proves reassociation safe.

use parser::ast::pipe_op::JoinType;
use plan::{LogicalPlan, PlanTraversal};

use super::eligibility::{self, Input};
use crate::context::OptimizerContext;
use crate::rules::predicate_analysis::PredicateSemantics;

pub(super) fn place<S: PredicateSemantics>(
    plan: LogicalPlan, semantics: &S, context: &OptimizerContext,
) -> (LogicalPlan, usize) {
    let Some(target) = eligibility::target(&plan, semantics) else { return (plan, 0); };
    if !super::cost::allows(&plan, target, context) { return (plan, 0); }
    let LogicalPlan::Join {
        left: probe, right: build, on, using, schema, ..
    } = plan else { unreachable!("eligibility proved semi join"); };
    let LogicalPlan::Join {
        left, right, on: inner_on, using: inner_using, ..
    } = *probe else { unreachable!("eligibility proved inner join"); };
    let (input, other) = match target {
        Input::Left => (left, right),
        Input::Right => (right, left),
    };
    let probe_schema = Box::new(input.schema().clone());
    let semi = LogicalPlan::Join {
        join_type: JoinType::Semi,
        left: input,
        right: build,
        on,
        using,
        schema: probe_schema,
    };
    // Continue down a join chain in this pass. Every movement strictly lowers
    // the existence test, so this cannot oscillate with join-order search.
    let (semi, movements) = place(semi, semantics, context);
    let (left, right) = match target {
        Input::Left => (Box::new(semi), other),
        Input::Right => (other, Box::new(semi)),
    };
    (LogicalPlan::Join {
        join_type: JoinType::Inner,
        left,
        right,
        on: inner_on,
        using: inner_using,
        schema,
    }, movements + 1)
}
