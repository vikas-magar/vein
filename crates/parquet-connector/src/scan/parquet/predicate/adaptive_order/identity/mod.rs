//! Invalidates observations when a worker binds a different predicate pipeline.

use std::sync::Arc;

use common::protocol::{ScanPredicate, ScanPredicateStage};
use exec::runtime::filter::RuntimeFilterSet;
use types::{ResolvedColumnRef, VeinSchema};

use super::{AdaptiveFilterConfig, AdaptiveFilterOrder};

pub(super) struct ScanIdentity {
    stages: Vec<ScanPredicateStage>,
    projected: Vec<ResolvedColumnRef>,
    runtime_filters: RuntimeFilterSet,
}

impl ScanIdentity {
    fn matches(
        &self,
        pushdown: Option<&ScanPredicate>,
        projected: &VeinSchema,
        runtime_filters: &RuntimeFilterSet,
    ) -> bool {
        let stages = pushdown.map_or(&[][..], |predicate| predicate.stages.as_slice());
        self.projected == projected.fields
            && self.runtime_filters.same_filter_instances(runtime_filters)
            && self.stages.len() == stages.len()
            && self.stages.iter().zip(stages).all(|(left, right)| {
            left.reorder_safe == right.reorder_safe
                && left.input == right.input
                && left.predicate == right.predicate
                && left.predicate_count == right.predicate_count
        })
    }
}

impl AdaptiveFilterOrder {
    pub(crate) fn for_scan(
        previous: Option<Arc<Self>>,
        stage_count: usize,
        pushdown: Option<&ScanPredicate>,
        projected: &VeinSchema,
        runtime_filters: &RuntimeFilterSet,
    ) -> Option<Arc<Self>> {
        if let Some(policy) = previous {
            if policy.stage_count() == stage_count
                && policy.identity.as_ref().is_some_and(|identity| {
                identity.matches(pushdown, projected, runtime_filters)
            })
            {
                tracing::debug!(target: "vein.parquet_adaptive_filter",
                    event = "parquet_adaptive_filter_state_reused",
                    stage_count, observations = policy.snapshot().observations,
                    "reusing this worker's observations for the same predicate pipeline");
                return Some(policy);
            }
            tracing::debug!(target: "vein.parquet_adaptive_filter",
                event = "parquet_adaptive_filter_state_reset",
                stage_count, reason = "predicate_pipeline_changed",
                "discarding observations for a different predicate pipeline");
        }
        let mut policy = Self::new(stage_count, AdaptiveFilterConfig::default())?;
        policy.identity = Some(ScanIdentity {
            stages: pushdown.map_or_else(Vec::new, |predicate| predicate.stages.clone()),
            projected: projected.fields.clone(),
            runtime_filters: runtime_filters.clone(),
        });
        Some(Arc::new(policy))
    }
}

#[cfg(test)]
mod tests;
