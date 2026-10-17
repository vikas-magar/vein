//! One atomic transition: execute, measure, optimize, lower, then register.
use super::*;

impl AdaptiveCoordinator {
    pub(super) async fn transition(
        &self, prepared: &PreparedAdaptiveCheckpoint, current: &LogicalPlan,
        execution: &AdaptiveExecution, stage: usize, source_id: IntermediateSourceId,
    ) -> common::Result<(Vec<ExecuteTask>, LogicalPlan, IntermediateSourceRegistration)> {
        let resources = execution.resources.as_ref().expect("resources created before transition");
        tracing::info!(target: "vein.adaptive", event = "adaptive_checkpoint_started", stage,
        source = prepared.boundary.table_name(), "executing checkpoint producer");
        let checkpoint = self.executor.execute_to_checkpoint_with_resources(
            &prepared.producer_task, execution.cancel.clone(), Arc::clone(resources),
        ).await?;
        let rows = checkpoint.relation.row_count()?;
        match checkpoint.relation.collect_statistics(prepared.boundary.output_schema()).await {
            Ok(statistics) => self.statistics.record_intermediate(source_id.into_raw(), statistics)?,
            Err(error) => tracing::warn!(target: "vein.adaptive", event = "adaptive_synopsis_unavailable",
            stage, error = %error, "retaining exact row count without column synopses"),
        }
        let rewritten = prepared.boundary.replace(current.clone(), source_id, rows)?;
        let before = rewritten.clone();
        let started = std::time::Instant::now();
        let (optimized, diagnostics) = match self.optimizer.optimize_with_statistics_context(
            rewritten, Arc::clone(&self.statistics),
        ) {
            Ok(result) => result,
            Err(error) => {
                tracing::warn!(target: "vein.adaptive", event = "adaptive_optimizer_fallback",
                stage, error = %error, "preserving checkpoint and original remaining operator order");
                (before.clone(), Default::default())
            }
        };
        for decision in &diagnostics.join_order_decisions {
            tracing::info!(target: "vein.adaptive", event = "adaptive_join_order_decision", stage,
            relations = ?decision.relation_labels, candidate_splits = decision.candidate_splits,
            original_cost = ?decision.original_cost, selected_cost = ?decision.selected_cost,
            selected_order = ?decision.selected_order, outcome = ?decision.outcome,
            "remaining join-order decision");
        }
        let plan_changed = optimized != before;
        let job = self.planner.create_physical_plan_with_statistics(self.request_id, optimized.clone(), Arc::clone(&self.statistics))?;
        let tasks = tasks_from_job(job, Arc::clone(&self.statistics), self.query_id)?;
        let registration = self.executor.register_intermediate_source(
            self.query_id, source_id, checkpoint.relation,
        )?;
        tracing::info!(target: "vein.adaptive", event = "adaptive_reoptimization_completed", stage,
        observed_rows = rows, estimated_rows = ?prepared.boundary.estimated_rows(),
        logical_plan_changed = plan_changed, physical_replanned = true,
        reoptimization_us = started.elapsed().as_micros() as u64,
        checkpoint_duration_ns = checkpoint.metrics.duration_ns,
        checkpoint_bytes_read = checkpoint.metrics.bytes_read,
        memory_used_bytes = resources.memory_used_bytes(), memory_peak_bytes = resources.memory_peak_bytes(),
        "remaining physical plan replaced using completed population statistics");
        Ok::<_, common::VeinError>((tasks, optimized, registration))
    }
}
