//! Finite adaptive execution loop, retaining the last valid continuation.
use super::*;
use tracing::Instrument;

impl AdaptiveCoordinator {
    pub async fn execute(
        self,
        tasks: Vec<ExecuteTask>,
        plan: Arc<LogicalPlan>,
        mut pending: Option<PreparedAdaptiveCheckpoint>,
    ) -> AdaptiveExecution {
        let mut execution = AdaptiveExecution {
            tasks,
            resources: None,
            registrations: HashMap::new(),
            cancel: CancellationToken::new(),
        };
        let mut current = plan;
        let mut completed_stages = 0_usize;
        let mut followups_scheduled = 0_usize;
        // Each boundary removes at least one unexecuted operator. The original
        // node count also caps attempts if a rewrite introduces fresh wrappers.
        for stage in 0..node_count(&current) {
            let Some(prepared) = pending.take() else { break; };
            if execution.resources.is_none() {
                match self.executor.create_query_resources(execution.cancel.clone()) {
                    Ok(resources) => execution.resources = Some(resources),
                    Err(error) => {
                        tracing::info!(target: "vein.adaptive", event = "adaptive_reoptimization_fallback",
                            stage, error = %error, reason = "resources_unavailable", "keeping current plan");
                        break;
                    }
                }
            }
            let source_id = IntermediateSourceId::new();
            let transition = self.transition(&prepared, &current, &execution, stage, source_id)
                .instrument(tracing::info_span!(target: "vein.adaptive", "adaptive_stage",
                    query_id = %self.query_id, stage, source_id = %source_id)).await;
            match transition {
                Ok((tasks, optimized, registration, schedule_followup)) => {
                    completed_stages += 1;
                    followups_scheduled += usize::from(schedule_followup);
                    execution.tasks = tasks;
                    current = Arc::new(optimized);
                    execution.registrations.insert(source_id.into_raw(), registration);
                    let mut active = HashSet::new();
                    relation_ids(&current, &mut active);
                    execution.registrations.retain(|id, _| active.contains(id));
                    if schedule_followup {
                        pending = self.policy.choose_boundary(&current, Arc::clone(&self.statistics))
                            .and_then(|boundary| prepare_checkpoint(self.planner.as_ref(), boundary,
                                                                    Arc::clone(&self.statistics), self.request_id, self.query_id,
                                                                    &execution.tasks, &current));
                    } else {
                        pending = None;
                        tracing::info!(target: "vein.adaptive", event = "adaptive_checkpoint_skipped",
                            stage, reason = "no_statistics_blocked_join_order_decisions",
                            "optimizer found no remaining join-order decision that needs more statistics");
                    }
                }
                Err(error) => {
                    tracing::warn!(target: "vein.adaptive", event = "adaptive_reoptimization_fallback",
                        stage, error = %error, reason = "stage_failed", "keeping last valid continuation");
                    break;
                }
            }
        }
        tracing::info!(target: "vein.adaptive.summary", event = "adaptive_execution_summary",
            query_id = %self.query_id,
            completed_stages,
            followups_scheduled,
            retained_intermediate_sources = execution.registrations.len(),
            stage_budget_exhausted = pending.is_some(),
            "adaptive execution stages completed");
        execution
    }
}
