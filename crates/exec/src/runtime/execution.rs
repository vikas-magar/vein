//! Recursive task execution and execution-flow diagnostics.

use super::*;

impl DefaultTaskExecutor {
    pub(super) fn trace_data_flow(
        node: &PhysicalPlanNode,
        task: &ExecuteTask,
        trace: &ExecutionTrace,
        depth: usize,
    ) {
        let contract = node.operator.data_flow();
        tracing::debug!(
            target: "vein.audit",
            event = "physical_operator_data_flow",
            query_id = %task.query_id,
            task_id = %task.task_id,
            node_id = trace.node_id(node),
            depth,
            operator = contract.operator,
            input_materialization = ?contract.input_materialization,
            output_representation = ?contract.output,
            output_columns = node.output_schema.fields.len(),
            "physical_operator_data_flow"
        );
    }

    /// Execute a recursive input tree with bounded batches between operators.
    pub(super) async fn execute_with_output(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
        make_sink: impl FnOnce(
            &dyn ExecutorBuilder,
            &DataSink,
            &QueryMemoryTracker,
        ) -> Result<Box<dyn Sink>>
        + Send,
    ) -> Result<TaskResult> {
        self.execute_with_output_and_resources(task, cancel, None, make_sink)
            .await
    }

    pub(super) async fn execute_with_output_and_resources(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
        resources: Option<Arc<QueryExecutionResources>>,
        make_sink: impl FnOnce(
            &dyn ExecutorBuilder,
            &DataSink,
            &QueryMemoryTracker,
        ) -> Result<Box<dyn Sink>>
        + Send,
    ) -> Result<TaskResult> {
        let start = Instant::now();
        if cancel.is_cancelled() {
            return Err(VeinError::cancelled());
        }
        let query_id = task.query_id.to_string();
        let task_id = task.task_id.to_string();
        let statistics_context = task
            .statistics_context
            .clone()
            .unwrap_or_else(|| Arc::new(QueryStatisticsContext::new([])));
        let mut trace = ExecutionTrace::new_with_query_context(
            &task.plan.root,
            query_id.clone(),
            task_id.clone(),
            task.task_id.into_raw(),
            statistics_context,
        );
        // Emit the compact plan before validation so malformed plans still have
        // node IDs, schemas, mappings, and flow contracts in the debug trace.
        trace.log_plan();
        Self::trace_data_flow(&task.plan.root, task, &trace, 0);
        task.plan.validate()?;
        let PhysicalPlanOperator::Sink {
            input,
            sink: sink_plan,
        } = &task.plan.root.operator
        else {
            return Err(VeinError::planning(
                "physical plan root must be a sink",
                None,
            ));
        };
        let schedule = PipelineExecutionSchedule::derive(&task.plan, &trace)?;
        let memory = match resources {
            Some(resources) => resources.memory.clone(),
            None => {
                let spill_storage = self.spill_storage_factory.create_for_query()?;
                QueryMemoryTracker::with_spill_storage(
                    self.resource_limits,
                    cancel.clone(),
                    spill_storage,
                )
            }
        };
        trace.attach_memory_tracker(memory.clone());

        let sink_started = Instant::now();
        let sink = make_sink(self.builder.as_ref(), sink_plan, &memory)?;
        let sink: Arc<Mutex<Box<dyn Sink>>> = Arc::new(Mutex::new(sink));
        let sink_build_us = sink_started.elapsed().as_micros() as u64;
        let task_span = tracing::info_span!(
            target: "vein.audit",
            "task_execution",
            request_id = %task.request_id,
            query_id = %task.query_id,
            stage_id = %task.stage_id,
            task_id = %task.task_id,
            attempt = task.attempt,
        );
        let head: Box<dyn PipelineNode> = Box::new(CancelGuard::new(
            Box::new(SinkHandle(Arc::clone(&sink))),
            cancel.clone(),
        ));
        let run_started = Instant::now();
        let source_started = Instant::now();
        let mut root_guard = trace.execution_guard(trace.node_id(&task.plan.root));
        let execution_context = recursive::RecursiveExecutionContext {
            task,
            cancel: &cancel,
            memory: &memory,
            span: &task_span,
            schedule: &schedule,
            trace: &trace,
        };
        let execution_result = self.execute_input(input, head, &execution_context, 0).await;
        root_guard.finish(&execution_result);
        let bytes_read = execution_result?;
        schedule.complete_boundary(&task.plan.root, PipelineBoundaryKind::ResultSink)?;
        schedule.verify_complete()?;
        let source_build_us = source_started.elapsed().as_micros() as u64;
        let run_us = run_started.elapsed().as_micros() as u64;

        let collect_started = Instant::now();
        let mut sink = sink
            .lock()
            .map_err(|_| VeinError::internal("sink mutex poisoned"))?;
        let output = sink.collect();
        let row_count = sink
            .row_count()
            .max(output.iter().map(|batch| batch.num_rows() as u64).sum());
        let write_receipt = sink.write_receipt().cloned();
        let collect_us = collect_started.elapsed().as_micros() as u64;
        let duration_ns = start.elapsed().as_nanos() as u64;
        tracing::info!(
            target: "vein.audit",
            event = "task_finished",
            task_id = %task.task_id,
            sink_build_us,
            source_build_us,
            run_us,
            collect_us,
            rows = row_count,
            output_batches = output.len(),
            bytes_read,
            memory_limit_bytes = memory.limit(),
            memory_peak_bytes = memory.peak(),
            memory_reserved_bytes = memory.used(),
            spill_disk_limit_bytes = memory.spill_bytes_limit(),
            spill_disk_peak_bytes = memory.spill_bytes_peak(),
            spill_disk_reserved_bytes = memory.spill_bytes_used(),
            total_us = duration_ns / 1000,
            "task_finished"
        );
        trace.mark_completed();
        Ok(TaskResult {
            row_count,
            output,
            write_receipt,
            metrics: TaskMetrics {
                bytes_read,
                duration_ns,
            },
        })
    }
}
