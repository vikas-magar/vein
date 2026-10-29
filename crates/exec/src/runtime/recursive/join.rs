use super::*;
use common::protocol::JoinPlan;

impl DefaultTaskExecutor {
    pub(super) async fn execute_join(
        &self, probe: &Arc<PhysicalPlanNode>, build: &Arc<PhysicalPlanNode>, plan: &JoinPlan,
        next: Box<dyn PipelineNode>, state: NodeExecution<'_>,
    ) -> Result<u64> {
        let NodeExecution { node: input, context, depth } = state;
        let RecursiveExecutionContext { memory, trace, schedule, .. } = *context;
        let node_id = trace.node_id(input);
        let started = Instant::now();
        let operator_memory = trace.operator_memory(node_id, memory);
        let build_sink = Arc::new(Mutex::new(Some(BuildSink::with_spill_support(
            join_keys(&plan.probe_keys),
            join_keys(&plan.build_keys),
            &join_types(&plan.build_types),
            operator_memory.clone(),
        )?)));
        let build_bytes = self
            .execute_input(
                build,
                Box::new(SharedBuildSink(Arc::clone(&build_sink))),
                context,
                depth + 1,
            )
            .await?;
        schedule.complete_boundary(input, PipelineBoundaryKind::JoinBuild)?;
        let build_sink = build_sink
            .lock()
            .map_err(|_| VeinError::internal("join build mutex poisoned"))?
            .take()
            .ok_or_else(|| {
                VeinError::internal("join build sink missing after execution")
            })?;
        let build_result = build_sink.finish_with_spill()?;
        let build_rows = match &build_result {
            BuildResult::InMemory(table) => table.len() as u64,
            BuildResult::Spilled { run, .. } => run.rows,
        };
        let build_us = started.elapsed().as_micros() as u64;
        let build_bytes = match &build_result {
            BuildResult::InMemory(_) => build_bytes,
            BuildResult::Spilled { run, .. } => {
                tracing::info!(
        target: "vein.audit",
        event = "join_build_spill_run_ready",
        rows = run.rows,
        bytes = run.bytes,
        "join build spill run ready"
    );
                run.bytes
            }
        };
        let runtime_filters = self.runtime_filters_for_join(
            probe,
            plan,
            &build_result,
            &operator_memory,
            trace,
            node_id,
        )?;
        let probe_rows = Arc::new(AtomicU64::new(0));
        let probe_batches = Arc::new(AtomicU64::new(0));
        let probe_started = Instant::now();
        let dependencies = schedule.dependencies_for_operator(input)?;
        let join: Box<dyn PipelineNode> = match build_result {
            BuildResult::InMemory(table) => {
                let join = HashJoin::new(
                    table,
                    join_type(plan.join_type)?,
                    join_outputs(&plan.outputs),
                    join_types(&plan.probe_types),
                    join_types(&plan.build_types),
                    next,
                );
                let join = install_join_residual(self.builder.as_ref(), plan, join)?;
                Box::new(PipelineDependencyGuard::new(Box::new(join), dependencies))
            }
            BuildResult::Spilled { run, schema } => {
                let probe_schema = JoinSpillSchema::new(&join_types(&plan.probe_types));
                let probe_writer =
                    JoinSpillWriter::new(operator_memory.clone(), probe_schema.clone())?;
                let downstream: SharedJoinOutput = Arc::new(tokio::sync::Mutex::new(next));
                let spilled = SpilledJoinProbe {
                    probe_writer: Some(probe_writer),
                    build_run: run,
                    build_schema: schema,
                    probe_schema,
                    plan: plan.clone(),
                    builder: Arc::clone(&self.builder),
                    tracker: operator_memory.clone(),
                    next: downstream,
                };
                Box::new(PipelineDependencyGuard::new(
                    Box::new(spilled),
                    dependencies,
                ))
            }
        };
        let probe_bytes = self
            .execute_input_with_runtime_filters(
                probe,
                Box::new(JoinProbeCounter {
                    inner: join,
                    rows: Arc::clone(&probe_rows),
                    batches: Arc::clone(&probe_batches),
                }),
                context,
                runtime_filters.clone(),
                depth + 1,
            )
            .await?;
        let probe_us = probe_started.elapsed().as_micros() as u64;
        if let Some(filters) = &runtime_filters {
            let rows_seen = filters.rows_seen();
            let rows_kept = filters.rows_kept();
            tracing::info!(
        target: "vein.statistics",
        event = "join_runtime_filter_completed",
        node_id,
        filter_names = ?filters.names(),
        filter_bytes = filters.memory_bytes(),
        rows_seen,
        rows_kept,
        rows_rejected = rows_seen.saturating_sub(rows_kept),
        "join runtime filter completed"
    );
        }
        tracing::info!(
target: "vein.audit",
event = "join_node_finished",
depth,
join_type = ?plan.join_type,
build_rows,
build_bytes,
build_us,
probe_rows = probe_rows.load(Ordering::Relaxed),
probe_batches = probe_batches.load(Ordering::Relaxed),
probe_bytes,
probe_us,
"join_node_finished"
                    );
        Ok(build_bytes.saturating_add(probe_bytes))
    }
}
