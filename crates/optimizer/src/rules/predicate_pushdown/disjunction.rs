//! Derives safe side-local prefilters from disjunctions over inner joins.

use std::collections::HashSet;

use parser::ast::expr::BinaryOp;
use parser::ast::pipe_op::JoinType;
use plan::{LogicalExpr, LogicalPlan, PlanTraversal, ResolvedColumnRef, VeinDataType, VeinSchema};

use super::super::predicate_utils::referenced_columns;

pub(super) struct InnerJoinContext<'a> {
    pub(super) left: &'a LogicalPlan,
    pub(super) right: &'a LogicalPlan,
    pub(super) on: &'a Option<LogicalExpr>,
    pub(super) using: &'a [ResolvedColumnRef],
    pub(super) schema: &'a VeinSchema,
    pub(super) left_columns: &'a HashSet<plan::ColumnId>,
    pub(super) right_columns: &'a HashSet<plan::ColumnId>,
}

pub(super) fn pushdown_inner_join(
    join: InnerJoinContext<'_>,
    residual: &LogicalExpr,
) -> Option<LogicalPlan> {
    let InnerJoinContext {
        left,
        right,
        on,
        using,
        schema,
        left_columns,
        right_columns,
    } = join;
    if !residual.is_reorder_safe() {
        return None;
    }

    let all_columns = left_columns.union(right_columns).copied().collect::<HashSet<_>>();
    let referenced = referenced_columns(residual)?;
    if referenced.is_empty() || !referenced.is_subset(&all_columns) {
        return None;
    }

    let left_predicate = project_to_side(residual, left_columns, 0);
    let right_predicate = project_to_side(residual, right_columns, 0);
    let left_predicate = left_predicate.filter(|predicate| !contains_filter(left, predicate));
    let right_predicate = right_predicate.filter(|predicate| !contains_filter(right, predicate));
    if left_predicate.is_none() && right_predicate.is_none() {
        return None;
    }
    tracing::info!(
        target: "vein.optimizer",
        event = "optimizer_disjunctive_join_prefilter",
        left_derived = left_predicate.is_some(),
        right_derived = right_predicate.is_some(),
        residual_retained = true,
        "optimizer_disjunctive_join_prefilter"
    );

    let left = left_predicate.map_or_else(
        || left.clone(),
        |predicate| LogicalPlan::Filter {
            input: Box::new(left.clone()),
            predicate,
        },
    );
    let right = right_predicate.map_or_else(
        || right.clone(),
        |predicate| LogicalPlan::Filter {
            input: Box::new(right.clone()),
            predicate,
        },
    );
    let join = LogicalPlan::Join {
        join_type: JoinType::Inner,
        left: Box::new(left),
        right: Box::new(right),
        on: on.clone(),
        using: using.to_vec(),
        schema: Box::new(schema.clone()),
    };

    Some(LogicalPlan::Filter {
        input: Box::new(join),
        predicate: residual.clone(),
    })
}

/// Projects a predicate to conditions necessary for every TRUE result on one
/// join side. `None` means that this expression branch does not constrain it.
fn project_to_side(
    expression: &LogicalExpr,
    side_columns: &HashSet<plan::ColumnId>,
    depth: usize,
) -> Option<LogicalExpr> {
    if depth >= 100 {
        return None;
    }

    if let LogicalExpr::BinaryOp {
        left,
        op: BinaryOp::And,
        right,
        return_type,
    } = expression
    {
        return combine_and(
            project_to_side(left, side_columns, depth + 1),
            project_to_side(right, side_columns, depth + 1),
            return_type,
        );
    }

    if let LogicalExpr::BinaryOp {
        left,
        op: BinaryOp::Or,
        right,
        return_type,
    } = expression
    {
        let (Some(left), Some(right)) = (
            project_to_side(left, side_columns, depth + 1),
            project_to_side(right, side_columns, depth + 1),
        ) else {
            return None;
        };
        return Some(binary(left, BinaryOp::Or, right, return_type));
    }

    let referenced = referenced_columns(expression)?;
    (!referenced.is_empty() && referenced.is_subset(side_columns)).then(|| expression.clone())
}

fn combine_and(
    left: Option<LogicalExpr>,
    right: Option<LogicalExpr>,
    return_type: &VeinDataType,
) -> Option<LogicalExpr> {
    match (left, right) {
        (Some(left), Some(right)) => Some(binary(left, BinaryOp::And, right, return_type)),
        (Some(predicate), None) | (None, Some(predicate)) => Some(predicate),
        (None, None) => None,
    }
}

fn binary(
    left: LogicalExpr,
    op: BinaryOp,
    right: LogicalExpr,
    return_type: &VeinDataType,
) -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
        return_type: return_type.clone(),
    }
}

fn contains_filter(plan: &LogicalPlan, predicate: &LogicalExpr) -> bool {
    let found_here = match plan {
        LogicalPlan::Filter {
            predicate: existing, ..
        } => existing == predicate,
        LogicalPlan::Scan { filters, .. } => filters.contains(predicate),
        _ => false,
    };
    found_here || plan.children().into_iter().any(|child| contains_filter(child, predicate))
}
