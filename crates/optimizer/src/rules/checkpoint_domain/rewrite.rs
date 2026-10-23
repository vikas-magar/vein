//! Insert a reference-backed tuple-membership join below the grouped aggregate.

use super::candidate::GroupedSide;
use super::restriction::Restriction;
use crate::rules::predicate_analysis::{conjunction, equality_expr};
use parser::ast::pipe_op::JoinType;
use plan::{LogicalExpr, LogicalPlan, PlanTraversal, VeinSchema};
use std::collections::HashSet;

pub(super) fn apply(plan: LogicalPlan, restriction: Restriction) -> LogicalPlan {
    let LogicalPlan::Join {
        join_type,
        left,
        right,
        on,
        using,
        schema,
    } = plan
    else {
        unreachable!("proved join");
    };
    let (left, right) = match restriction.side {
        GroupedSide::Right => (
            left,
            Box::new(insert(*right, restriction.depth, restriction)),
        ),
        GroupedSide::Left => (
            Box::new(insert(*left, restriction.depth, restriction)),
            right,
        ),
    };
    LogicalPlan::Join {
        join_type,
        left,
        right,
        on,
        using,
        schema,
    }
}

fn insert(plan: LogicalPlan, depth: usize, restriction: Restriction) -> LogicalPlan {
    if depth != 0 {
        return match plan {
            LogicalPlan::Project {
                input,
                exprs,
                schema,
            } => LogicalPlan::Project {
                input: Box::new(insert(*input, depth - 1, restriction)),
                exprs,
                schema,
            },
            LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
                input: Box::new(insert(*input, depth - 1, restriction)),
                predicate,
            },
            _ => unreachable!("proved aggregate wrapper"),
        };
    }
    let LogicalPlan::Aggregate {
        input,
        grouping_sets,
        aggs,
        schema,
    } = plan
    else {
        unreachable!("proved aggregate");
    };
    let mut seen = HashSet::new();
    let fields: Vec<_> = restriction
        .keys
        .iter()
        .filter_map(|(_, key)| seen.insert(key.id).then(|| key.clone()))
        .collect();
    let domain = LogicalPlan::Project {
        exprs: fields.iter().cloned().map(LogicalExpr::Column).collect(),
        input: Box::new(restriction.domain),
        schema: Box::new(VeinSchema::new(fields)),
    };
    let predicate = conjunction(
        restriction
            .keys
            .iter()
            .map(|(inner, outer)| equality_expr(inner.clone(), outer.clone())),
    );
    let input_schema = Box::new(input.schema().clone());
    let input = LogicalPlan::Join {
        join_type: JoinType::Semi,
        left: input,
        right: Box::new(domain),
        on: predicate,
        using: vec![],
        schema: input_schema,
    };
    tracing::info!(target: "vein.optimizer", event = "optimizer_checkpoint_aggregate_domain",
        domain_source_id = %format_args!("{:032x}", restriction.source_id), domain_rows = restriction.rows,
        key_count = restriction.keys.len(), payload_bytes_gathered = 0,
        "grouped input restricted to retained outer key tuples");
    LogicalPlan::Aggregate {
        input: Box::new(input),
        grouping_sets,
        aggs,
        schema,
    }
}
