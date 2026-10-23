//! Query-local, finite stage transitions over the canonical optimizer and planner.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use common::cancellation::CancellationToken;
use common::identifiers::{IntermediateSourceId, QueryId, RequestId};
use common::protocol::ExecuteTask;
use common::protocol::{PhysicalPlanNode, PhysicalPlanOperator};
use common::statistics::QueryStatisticsContext;
use exec::runtime::checkpoint::IntermediateSourceRegistration;
use exec::runtime::{QueryExecutionResources, TaskExecutor};
use physical::PhysicalPlanner;
use plan::{LogicalPlan, PlanTraversal};

use super::{AdaptiveBoundary, AdaptiveCheckpointPolicy};
mod coordinator;
mod stage;
use super::super::query::PreparedAdaptiveCheckpoint;
use super::super::tasks::tasks_from_job;
#[cfg(test)]
pub(super) use stage::followup_needed;

#[cfg(test)]
mod tests;

pub(in crate::local) struct AdaptiveCoordinator {
    pub executor: Arc<dyn TaskExecutor>,
    pub optimizer: Arc<optimizer::OptimizerPipeline>,
    pub planner: Arc<dyn PhysicalPlanner>,
    pub policy: Arc<dyn AdaptiveCheckpointPolicy>,
    pub statistics: Arc<QueryStatisticsContext>,
    pub request_id: RequestId,
    pub query_id: QueryId,
}

/// Retains registrations and cancellation for the entire result stream lifetime.
pub(in crate::local) struct AdaptiveExecution {
    pub tasks: Vec<ExecuteTask>,
    pub resources: Option<Arc<QueryExecutionResources>>,
    registrations: HashMap<u128, IntermediateSourceRegistration>,
    cancel: CancellationToken,
}

impl Drop for AdaptiveExecution {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

pub(in crate::local) fn prepare_checkpoint(
    planner: &dyn PhysicalPlanner,
    mut boundary: AdaptiveBoundary,
    statistics: Arc<QueryStatisticsContext>,
    request_id: RequestId,
    query_id: QueryId,
    remaining: &[ExecuteTask],
    remaining_plan: &LogicalPlan,
) -> Option<PreparedAdaptiveCheckpoint> {
    let result = planner
        .create_physical_plan_with_statistics(
            request_id,
            boundary.producer.clone(),
            Arc::clone(&statistics),
        )
        .and_then(|job| tasks_from_job(job, statistics, query_id));
    match result {
        Ok(mut tasks) if tasks.len() == 1 => {
            // Physical pruning defines the emitted population schema. Keep its
            // column identities rather than the binder's unpruned superset.
            boundary.output_schema = Box::new(tasks[0].plan.root.output_schema.as_ref().clone());
            let domain_checkpoint = supplies_domain(remaining_plan, &boundary.path);
            if join_count(remaining_plan) == 1
                && !domain_checkpoint
                && remaining
                    .iter()
                    .any(|task| already_builds(&task.plan.root, &boundary.output_schema))
            {
                tracing::info!(target: "vein.adaptive", event = "adaptive_checkpoint_skipped",
                    source = boundary.table_name(), reason = "only_join_already_builds_this_input",
                    "existing build barrier already provides feedback before probing");
                return None;
            }
            tracing::info!(target: "vein.adaptive", event = "adaptive_checkpoint_selected",
                source = boundary.table_name(), estimated_rows = ?boundary.estimated_rows(),
                reason = if domain_checkpoint { "tuple_domain_required" } else { "uncertain_input_to_remaining_join" },
                "selected adaptive boundary");
            Some(PreparedAdaptiveCheckpoint {
                boundary,
                producer_task: tasks.remove(0),
            })
        }
        result => {
            tracing::info!(target: "vein.adaptive", event = "adaptive_checkpoint_skipped",
                reason = "producer_not_single_task", error = ?result.err(), "keeping current continuation");
            None
        }
    }
}

fn supplies_domain(plan: &LogicalPlan, path: &[usize]) -> bool {
    let Some((&index, ancestors)) = path.split_last() else {
        return false;
    };
    let mut parent = plan;
    for index in ancestors {
        let children = parent.children();
        let Some(child) = children.get(*index) else {
            return false;
        };
        parent = child;
    }
    let children = parent.children();
    let Some(child) = children.get(index) else {
        return false;
    };
    optimizer::rules::CheckpointAggregateDomainRule::checkpoint_input(parent)
        .is_some_and(|probe| std::ptr::eq(probe, *child))
}

fn relation_ids(plan: &LogicalPlan, ids: &mut HashSet<u128>) {
    if let LogicalPlan::Intermediate { source_id, .. } = plan {
        ids.insert(*source_id);
    }
    for child in plan.children() {
        relation_ids(child, ids);
    }
}

fn node_count(plan: &LogicalPlan) -> usize {
    1 + plan.children().into_iter().map(node_count).sum::<usize>()
}

pub(super) fn join_count(plan: &LogicalPlan) -> usize {
    usize::from(matches!(plan, LogicalPlan::Join { .. }))
        + plan.children().into_iter().map(join_count).sum::<usize>()
}

pub(super) fn join_checkpoint_has_search_space(
    plan: &LogicalPlan,
    node: &LogicalPlan,
    path: &[usize],
) -> bool {
    if !matches!(node, LogicalPlan::Join { .. }) {
        return true;
    }
    let remaining = continuation_join_count(plan, path);
    if remaining >= 2 {
        return true;
    }
    tracing::info!(target: "vein.adaptive", event = "adaptive_checkpoint_candidate_skipped",
        boundary_operator = node.operator_name(), remaining_join_count = remaining,
        reason = "checkpoint_leaves_no_join_order_alternatives",
        "skipped join checkpoint before statistics collection");
    false
}

fn continuation_join_count(plan: &LogicalPlan, path: &[usize]) -> usize {
    let Some((&target, rest)) = path.split_first() else {
        return 0;
    };
    usize::from(matches!(plan, LogicalPlan::Join { .. }))
        + plan
            .children()
            .into_iter()
            .enumerate()
            .map(|(index, child)| {
                if index == target {
                    continuation_join_count(child, rest)
                } else {
                    join_count(child)
                }
            })
            .sum::<usize>()
}

fn already_builds(node: &PhysicalPlanNode, schema: &plan::VeinSchema) -> bool {
    match &node.operator {
        PhysicalPlanOperator::Join { build, .. } => build.output_schema.as_ref() == schema,
        PhysicalPlanOperator::Transform { input, .. }
        | PhysicalPlanOperator::Aggregate { input, .. }
        | PhysicalPlanOperator::Distinct { input, .. }
        | PhysicalPlanOperator::Sort { input, .. }
        | PhysicalPlanOperator::Limit { input, .. }
        | PhysicalPlanOperator::Sink { input, .. } => already_builds(input, schema),
        PhysicalPlanOperator::Pipeline { .. } => false,
    }
}
