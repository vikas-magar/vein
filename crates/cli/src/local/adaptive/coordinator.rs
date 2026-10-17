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
                Ok((tasks, optimized, registration)) => {
                    execution.tasks = tasks;
                    current = Arc::new(optimized);
                    execution.registrations.insert(source_id.into_raw(), registration);
                    let mut active = HashSet::new();
                    relation_ids(&current, &mut active);
                    execution.registrations.retain(|id, _| active.contains(id));
                    pending = self.policy.choose_boundary(&current, Arc::clone(&self.statistics))
                        .and_then(|boundary| prepare_checkpoint(self.planner.as_ref(), boundary,
                                                                Arc::clone(&self.statistics), self.request_id, self.query_id,
                                                                &execution.tasks, &current));
                }
                Err(error) => {
                    tracing::warn!(target: "vein.adaptive", event = "adaptive_reoptimization_fallback",
                        stage, error = %error, reason = "stage_failed", "keeping last valid continuation");
                    break;
                }
            }
        }
        execution
    }
}
