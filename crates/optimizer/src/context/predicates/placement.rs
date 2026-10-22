//! Bounded metadata records; neither expression literals nor rows are logged.

use parser::ast::pipe_op::JoinType;
use plan::{ColumnId, LogicalExpr, LogicalPlan};

use crate::rules::predicate_analysis::{JoinInputs, PredicateOwner};

/// Where a predicate is enforced in the final logical tree.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PredicateLocation {
    /// Evaluated by the source scan provider.
    Scan,
    /// Evaluated after a relational input.
    Filter,
    /// Part of join matching semantics.
    Join,
}

/// A visible boundary retaining a predicate in its final position.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PredicateRetention {
    /// Evaluation may fail or depend on evaluation order.
    EvaluationSafety,
    /// References values from both join inputs.
    MultipleInputs,
    /// Moving to the nullable side could change unmatched rows.
    OuterJoinSemantics,
    /// Semi/anti matching and preserved-output semantics apply.
    ExistenceJoinSemantics,
    /// Moving through LIMIT changes which rows are considered.
    RowLimit,
    /// This operator needs a separate, supported predicate mapping proof.
    OperatorBoundary,
}

/// One predicate in the final plan, linked to unchanged input expressions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PredicatePlacement {
    /// Preorder identifier in this final logical tree, including subqueries.
    pub node_id: usize,
    /// Operator enforcing the predicate.
    pub operator: &'static str,
    /// Predicate role at this operator.
    pub location: PredicateLocation,
    /// Referenced column identities, sorted and deduplicated.
    pub columns: Vec<ColumnId>,
    /// Input conjunct ordinals with exactly equal expressions. Empty means the
    /// expression was introduced or rewritten; it does not invent provenance.
    pub matching_input_predicates: Vec<usize>,
    /// Whether built-in analysis permits changing evaluation order.
    pub reorder_safe: bool,
    /// Visible boundary for a retained relational filter, when present.
    pub retention: Option<PredicateRetention>,
}

impl PredicatePlacement {
    pub(super) fn new(
        node_id: usize, owner: &LogicalPlan, location: PredicateLocation,
        expression: &LogicalExpr, matching_input_predicates: Vec<usize>,
    ) -> Self {
        let mut columns = Vec::new();
        let mut pending = vec![expression];
        while let Some(expression) = pending.pop() {
            if let LogicalExpr::Column(column) | LogicalExpr::OuterColumn(column) = expression {
                columns.push(column.id);
            }
            pending.extend(expression.children());
        }
        columns.sort_unstable_by_key(|id| id.0);
        columns.dedup();
        let reorder_safe = expression.is_reorder_safe();
        Self {
            node_id,
            operator: owner.operator_name(),
            location,
            columns,
            matching_input_predicates,
            reorder_safe,
            retention: retention(owner, expression),
        }
    }
}

fn retention(owner: &LogicalPlan, expression: &LogicalExpr) -> Option<PredicateRetention> {
    let LogicalPlan::Filter { input, .. } = owner else { return None };
    if !expression.is_reorder_safe() { return Some(PredicateRetention::EvaluationSafety); }
    Some(match input.as_ref() {
        LogicalPlan::Limit { .. } => PredicateRetention::RowLimit,
        LogicalPlan::Join { join_type: JoinType::Left | JoinType::Right | JoinType::Full, .. } => PredicateRetention::OuterJoinSemantics,
        LogicalPlan::Join { join_type: JoinType::Semi | JoinType::Anti | JoinType::NullAwareAnti, .. } => PredicateRetention::ExistenceJoinSemantics,
        LogicalPlan::Join { left, right, .. }
        if JoinInputs::new(left.schema(), right.schema()).owner(expression) == PredicateOwner::Both => PredicateRetention::MultipleInputs,
        _ => PredicateRetention::OperatorBoundary,
    })
}

use plan::PlanTraversal;
