use super::*;

#[derive(Clone, Copy)]
pub(super) struct RecursiveExecutionContext<'a> {
    pub(super) task: &'a ExecuteTask,
    pub(super) cancel: &'a CancellationToken,
    pub(super) memory: &'a QueryMemoryTracker,
    pub(super) span: &'a tracing::Span,
    pub(super) schedule: &'a PipelineExecutionSchedule,
    pub(super) trace: &'a ExecutionTrace,
}

impl DefaultTaskExecutor {
    pub(super) fn execute_input<'a>(
        &'a self,
        input: &'a Arc<PhysicalPlanNode>,
        next: Box<dyn PipelineNode<DataChunkView>>,
        context: &'a RecursiveExecutionContext<'a>,
        depth: usize,
    ) -> Pin<Box<dyn Future<Output = Result<u64>> + Send + 'a>> {
        self.execute_input_with_runtime_filters(input, next, context, None, depth)
    }

    /// Execute one input node with filters scoped to its direct source.
    /// Recursive operators use `execute_input` unless they can prove a safe
    /// column-preserving path, which keeps unsupported plans on exact execution.
    fn execute_input_with_runtime_filters<'a>(
        &'a self,
        input: &'a Arc<PhysicalPlanNode>,
        next: Box<dyn PipelineNode<DataChunkView>>,
        context: &'a RecursiveExecutionContext<'a>,
        runtime_filters: Option<Arc<RuntimeFilterSet>>,
        depth: usize,
    ) -> Pin<Box<dyn Future<Output = Result<u64>> + Send + 'a>> {
        let RecursiveExecutionContext {
            task,
            cancel,
            memory,
            span,
            schedule,
            trace,
        } = *context;
        let node_id = trace.node_id(input);
        let operator = input.operator.data_flow().operator;
        let node_span = tracing::debug_span!(
            target: "vein.audit",
            "physical_operator",
            query_id = %task.query_id,
            task_id = %task.task_id,
            node_id,
            operator,
            depth,
        );
        let trace_for_metrics = trace;
        Box::pin(async move {
            let mut execution_guard = trace_for_metrics.execution_guard(node_id);
            let result = async move {
                if cancel.is_cancelled() {
                    return Err(VeinError::cancelled());
                }
                tracing::debug!(
                target: "vein.audit",
                event = "physical_operator_started",
                node_id,
                operator,
                depth,
                "physical_operator_started"
            );
                // Execution depth starts at the sink's child; diagnostics include
                // the sink itself as canonical tree depth zero.
                Self::trace_data_flow(input, task, trace, depth.saturating_add(1));
                let mut next = trace.observer(
                    trace.node_id(input),
                    next,
                    task.query_id.to_string(),
                    task.task_id.to_string(),
                );
                match &input.operator {
                    PhysicalPlanOperator::Pipeline {
                        source: data_source,
                        program,
                        ..
                    } => {
                        if let DataSource::Intermediate(source_id) = data_source.as_ref() {
                            let mut source = self
                                .intermediate_sources
                                .source(task.query_id, *source_id)?;
                            let mut pipeline = if program.is_empty() {
                                next
                            } else {
                                self.builder.build_expression_executor(program, next)?
                            };
                            tracing::info!(
                                target: "vein.audit",
                                event = "intermediate_source_started",
                                query_id = %task.query_id,
                                source_id = %source_id,
                                task_id = %task.task_id,
                                "replaying query-local checkpoint"
                            );
                            source.execute(pipeline.as_mut()).await?;
                            trace.record_source_bytes(node_id, 0);
                            return Ok(0);
                        }
                        let max_parallelism = self.builder.max_parallelism().max(1);
                        let sources = self
                            .builder
                            .partition_scan_source_with_runtime_filters(
                                data_source,
                                max_parallelism,
                                runtime_filters.as_deref(),
                            )?;
                        if sources.is_empty() {
                            tracing::debug!(
                                target: "vein.audit",
                                event = "scan_work_scheduled",
                                work_unit_count = 0,
                                worker_count = 0,
                                "scan has no candidate storage units"
                            );
                            let mut empty_pipeline = if program.is_empty() {
                                next
                            } else {
                                self.builder.build_expression_executor(program, next)?
                            };
                            empty_pipeline.flush()?;
                            trace.record_source_bytes(node_id, 0);
                            return Ok(0);
                        }
                        if sources.len() > 1 {
                            let worker_count = max_parallelism.min(sources.len());
                            tracing::debug!(
                                target: "vein.audit",
                                event = "scan_work_scheduled",
                                work_unit_count = sources.len(),
                                worker_count,
                                "scan storage units scheduled"
                            );
                            let mut local_nodes = Vec::new();
                            local_nodes.try_reserve(worker_count).map_err(|error| {
                                VeinError::internal(format!(
                                    "cannot reserve scan-local pipeline states: {error}"
                                ))
                            })?;
                            let mut fan_in_fallback = if worker_count == 1 {
                                Some("parallelism_one")
                            } else {
                                None
                            };
                            if fan_in_fallback.is_none() {
                                for _ in 0..worker_count {
                                    match next.fork_local() {
                                        Ok(Some(local)) => local_nodes.push(local),
                                        Ok(None) => {
                                            fan_in_fallback = Some("operator_chain_not_combinable");
                                            break;
                                        }
                                        Err(VeinError::ResourceExhausted { .. }) => {
                                            fan_in_fallback = Some("worker_state_memory_budget");
                                            break;
                                        }
                                        Err(error) => return Err(error),
                                    }
                                }
                            }
                            if local_nodes.len() == worker_count && fan_in_fallback.is_none() {
                                let mut canonical: Box<dyn PipelineNode<DataChunkView>> =
                                    if program.is_empty() {
                                        next
                                    } else {
                                        Box::new(TransformHandle(
                                            self.builder.build_expression_executor(program, next)?,
                                        ))
                                    };
                                tracing::debug!(
                                    target: "vein.audit",
                                    event = "scan_fan_in_selected",
                                    mode = "worker_local_combine",
                                    work_unit_count = sources.len(),
                                    worker_count,
                                    "scan workers own independent downstream operator state"
                                );
                                let pending = Arc::new(Mutex::new(VecDeque::from(sources)));
                                let runtime = tokio::runtime::Handle::current();
                                let mut workers = tokio::task::JoinSet::new();
                                let query_statistics = trace.query_statistics_context();
                                for local in local_nodes {
                                    let builder = Arc::clone(&self.builder);
                                    let pending = Arc::clone(&pending);
                                    let program = Arc::clone(program);
                                    let task_id = task.task_id;
                                    let attempt = task.attempt;
                                    let cancel = cancel.clone();
                                    let span = span.clone();
                                    let runtime = runtime.clone();
                                    let runtime_filters = runtime_filters.clone();
                                    let query_statistics = Arc::clone(&query_statistics);
                                    workers.spawn_blocking(move || {
                                        let worker_started = std::time::Instant::now();
                                        let mut bytes = 0u64;
                                        let mut work_units = 0u64;
                                        let mut source_elapsed = std::time::Duration::ZERO;
                                        let worker_tail: Box<dyn PipelineNode<DataChunkView>> =
                                            Box::new(SuppressFlush::new(local));
                                        let mut pipeline: Box<dyn PipelineNode<DataChunkView>> =
                                            if program.is_empty() {
                                                worker_tail
                                            } else {
                                                Box::new(TransformHandle(
                                                    builder.build_expression_executor(&program, worker_tail)?,
                                                ))
                                            };
                                        loop {
                                            if cancel.is_cancelled() {
                                                return Err(VeinError::cancelled());
                                            }
                                            let source_spec = pending
                                                .lock()
                                                .map_err(|_| {
                                                    VeinError::internal(
                                                        "scan work queue mutex poisoned",
                                                    )
                                                })?
                                                .pop_front();
                                            let Some(source_spec) = source_spec else {
                                                break;
                                            };
                                            let mut source = builder.build_source_with_query_context(
                                                &source_spec,
                                                task_id,
                                                attempt,
                                                runtime_filters.as_deref(),
                                                Some(Arc::clone(&query_statistics)),
                                            )?;
                                            let source_started = std::time::Instant::now();
                                            runtime.block_on(async {
                                                tokio::select! {
                                                    biased;
                                                    _ = cancel.cancelled() => Err(VeinError::cancelled()),
                                                    result = source.execute(pipeline.as_mut()).instrument(span.clone()) => result,
                                                }
                                            })?;
                                            source_elapsed += source_started.elapsed();
                                            work_units += 1;
                                            bytes = bytes.saturating_add(source.bytes_read());
                                        }
                                        tracing::info!(target: "vein.audit",
                                            event = "scan_worker_completed",
                                            task_id = %task_id, node_id, work_units, bytes_read = bytes,
                                            source_elapsed_us = source_elapsed.as_micros() as u64,
                                            elapsed_us = worker_started.elapsed().as_micros() as u64,
                                            "scan worker completed");
                                        Ok((bytes, pipeline))
                                    });
                                }

                                let mut active_workers = worker_count;
                                let mut bytes = 0u64;
                                let mut local_states = Vec::new();
                                local_states.try_reserve(worker_count).map_err(|error| {
                                    VeinError::internal(format!(
                                        "cannot reserve completed scan-local states: {error}"
                                    ))
                                })?;
                                let mut worker_error = None;
                                while active_workers > 0 {
                                    tokio::select! {
                                        biased;
                                        _ = cancel.cancelled() => {
                                            worker_error.get_or_insert_with(VeinError::cancelled);
                                            break;
                                        }
                                        worker = workers.join_next() => {
                                            match worker {
                                                Some(Ok(Ok((worker_bytes, local)))) => {
                                                    active_workers -= 1;
                                                    bytes = bytes.saturating_add(worker_bytes);
                                                    local_states.push(local);
                                                }
                                                Some(Ok(Err(error))) => {
                                                    worker_error = Some(error);
                                                    cancel.cancel();
                                                    break;
                                                }
                                                Some(Err(error)) => {
                                                    worker_error = Some(VeinError::internal(format!(
                                                        "local scan worker failed: {error}"
                                                    )));
                                                    cancel.cancel();
                                                    break;
                                                }
                                                None => active_workers = 0,
                                            }
                                        }
                                    }
                                }
                                if let Some(error) = worker_error {
                                    cancel.cancel();
                                    while workers.join_next().await.is_some() {}
                                    return Err(error);
                                }
                                for local in local_states {
                                    local.combine_local(canonical.as_mut())?;
                                }
                                tracing::debug!(
                                    target: "vein.audit",
                                    event = "scan_fan_in_combined",
                                    worker_count,
                                    "worker-local pipeline state combined"
                                );
                                canonical.flush()?;
                                trace.record_source_bytes(node_id, bytes);
                                return Ok(bytes);
                            }
                            tracing::debug!(
                                target: "vein.audit",
                                event = "scan_fan_in_selected",
                                mode = "bounded_channel_serial",
                                work_unit_count = sources.len(),
                                worker_count,
                                fallback_reason = fan_in_fallback.unwrap_or("local_state_unavailable"),
                                "scan workers use the serial downstream fallback"
                            );
                            let pending = Arc::new(Mutex::new(VecDeque::from(sources)));
                            let finished = Arc::new(AtomicBool::new(false));
                            let (sender, receiver) = flume::bounded(worker_count);
                            let runtime = tokio::runtime::Handle::current();
                            let mut workers = tokio::task::JoinSet::new();
                            for _ in 0..worker_count {
                                let builder = Arc::clone(&self.builder);
                                let pending = Arc::clone(&pending);
                                let sender = sender.clone();
                                let finished = Arc::clone(&finished);
                                let program = Arc::clone(program);
                                let task_id = task.task_id;
                                let attempt = task.attempt;
                                let cancel = cancel.clone();
                                let span = span.clone();
                                let runtime = runtime.clone();
                                let runtime_filters = runtime_filters.clone();
                                workers.spawn_blocking(move || {
                                    let mut bytes = 0u64;
                                    loop {
                                        if cancel.is_cancelled() {
                                            return Err(VeinError::cancelled());
                                        }
                                        let source_spec = pending
                                            .lock()
                                            .map_err(|_| {
                                                VeinError::internal("scan work queue mutex poisoned")
                                            })?
                                            .pop_front();
                                        let Some(source_spec) = source_spec else {
                                            return Ok(bytes);
                                        };
                                        let mut source = match runtime_filters.as_deref() {
                                            Some(filters) if !filters.is_empty() => builder
                                                .build_source_with_runtime_filters(
                                                    &source_spec,
                                                    task_id,
                                                    attempt,
                                                    filters,
                                                )?,
                                            _ => builder.build_source(&source_spec, task_id, attempt)?,
                                        };
                                        let channel: Box<dyn PipelineNode<DataChunkView>> = Box::new(
                                            SuppressFlush::new(Box::new(ChannelPipelineNode {
                                                sender: sender.clone(),
                                                finished: Arc::clone(&finished),
                                            })),
                                        );
                                        let result = if program.is_empty() {
                                            let mut next = channel;
                                            runtime.block_on(async {
                                                tokio::select! {
                                                biased;
                                                _ = cancel.cancelled() => Err(VeinError::cancelled()),
                                                result = source.execute(next.as_mut()).instrument(span.clone()) => result,
                                            }
                                            })
                                        } else {
                                            let expression_executor = builder.build_expression_executor(&program, channel)?;
                                            let mut next = expression_executor;
                                            runtime.block_on(async {
                                                tokio::select! {
                                                biased;
                                                _ = cancel.cancelled() => Err(VeinError::cancelled()),
                                                result = source.execute(next.as_mut()).instrument(span.clone()) => result,
                                            }
                                            })
                                        };
                                        result?;
                                        bytes = bytes.saturating_add(source.bytes_read());
                                        if finished.load(Ordering::Acquire) {
                                            return Ok(bytes);
                                        }
                                    }
                                });
                            }
                            drop(sender);

                            let mut active_workers = worker_count;
                            let mut receiver_open = true;
                            let mut bytes = 0u64;
                            let mut worker_error = None;
                            while active_workers > 0 || receiver_open {
                                tokio::select! {
                                biased;
                                _ = cancel.cancelled() => {
                                    worker_error.get_or_insert_with(VeinError::cancelled);
                                    break;
                                }
                                        message = receiver.recv_async(), if receiver_open => {
                                    match message {
                                        Ok(chunk) => {
                                            if let Err(error) = next.push(chunk) {
                                                worker_error = Some(error);
                                                cancel.cancel();
                                                break;
                                            }
                                            if next.finished() {
                                                finished.store(true, Ordering::Release);
                                            }
                                        }
                                        Err(_) => receiver_open = false,
                                    }
                                }
                                worker = workers.join_next(), if active_workers > 0 => {
                                    match worker {
                                        Some(Ok(Ok(worker_bytes))) => {
                                            active_workers -= 1;
                                            bytes = bytes.saturating_add(worker_bytes);
                                        }
                                        Some(Ok(Err(error))) => {
                                            worker_error = Some(error);
                                            cancel.cancel();
                                            break;
                                        }
                                        Some(Err(error)) => {
                                            worker_error = Some(VeinError::internal(format!(
                                                "input scan worker failed: {error}"
                                            )));
                                            cancel.cancel();
                                            break;
                                        }
                                        None => active_workers = 0,
                                    }
                                }
                            }
                            }
                            drop(receiver);
                            if let Some(error) = worker_error {
                                cancel.cancel();
                                while workers.join_next().await.is_some() {}
                                return Err(error);
                            }
                            next.flush()?;
                            trace.record_source_bytes(node_id, bytes);
                            return Ok(bytes);
                        }
                        let source_spec = &sources[0];
                        let mut source = self.builder.build_source_with_query_context(
                            source_spec,
                            task.task_id,
                            task.attempt,
                            runtime_filters.as_deref(),
                            Some(trace.query_statistics_context()),
                        )?;
                        let bytes_read = if program.is_empty() {
                            let mut source_next = next;
                            tokio::select! {
                            biased;
                            _ = cancel.cancelled() => Err(VeinError::cancelled()),
                            result = source.execute(source_next.as_mut()).instrument(span.clone()) => {
                                result?;
                                Ok(source.bytes_read())
                            }
                        }
                        } else {
                            let expression_executor = self.builder.build_expression_executor(program, next)?;
                            let mut source_next = expression_executor;
                            tokio::select! {
                            biased;
                            _ = cancel.cancelled() => Err(VeinError::cancelled()),
                            result = source.execute(source_next.as_mut()).instrument(span.clone()) => {
                                result?;
                                Ok(source.bytes_read())
                            }
                        }
                        };
                        if let Ok(bytes) = &bytes_read {
                            trace.record_source_bytes(node_id, *bytes);
                        }
                        bytes_read
                    }
                    PhysicalPlanOperator::Transform { input, program } => {
                        let transform = self.builder.build_expression_executor(program, next)?;
                        let bytes = self
                            .execute_input(
                                input,
                                Box::new(TransformHandle(transform)),
                                context,
                                depth + 1,
                            )
                            .await?;
                        Ok(bytes)
                    }
                    PhysicalPlanOperator::Aggregate { input: child, plan }
                    | PhysicalPlanOperator::Distinct { input: child, plan } => {
                        let operator_memory = trace.operator_memory(node_id, memory);
                        let operator =
                            if matches!(&input.operator, PhysicalPlanOperator::Distinct { .. }) {
                                "distinct"
                            } else {
                                "aggregate"
                            };
                        let (accumulator, handle) = aggregate::shared_with_memory_tracker(
                            plan.clone(),
                            operator_memory,
                        )?;
                        let bytes = self
                            .execute_input(
                                child,
                                handle,
                                context,
                                depth + 1,
                            )
                            .await?;
                        let boundary =
                            if matches!(&input.operator, PhysicalPlanOperator::Distinct { .. }) {
                                PipelineBoundaryKind::DistinctInput
                            } else {
                                PipelineBoundaryKind::AggregateInput
                            };
                        schedule.complete_boundary(input, boundary)?;
                        let (metrics, group_count) = {
                            let mut accumulator = accumulator
                                .lock()
                                .map_err(|_| VeinError::internal("aggregate mutex poisoned"))?;
                            accumulator.finish_with(aggregate::AGGREGATE_BATCH_ROWS, |batch| {
                                next.push(batch)
                            })?;
                            (accumulator.metrics(), accumulator.group_count())
                        };
                        next.flush()?;
                        tracing::info!(
                        target: "vein.audit",
                        event = "recursive_aggregate_finished",
                        operator,
                        depth,
                        input_bytes = bytes,
                        input_batches = metrics.input_batches,
                        input_rows = metrics.input_rows,
                        grouping_sets = metrics.grouping_sets,
                        grouping_probes = metrics.grouping_probes,
                        group_index_candidate_checks = metrics.group_index_candidate_checks,
                        group_index_reserve_ns = metrics.group_index_reserve_ns,
                        group_index_capacity_growth_events = metrics.group_index_capacity_growth_events,
                        group_index_entries_at_capacity_growth = metrics.group_index_entries_at_capacity_growth,
                        group_index_capacity = metrics.group_index_capacity,
                        group_index_entries_ns = metrics.group_index_entries_ns,
                        full_hash_matches = metrics.full_hash_matches,
                        group_key_comparisons = metrics.group_key_comparisons,
                        group_collision_retries = metrics.group_collision_retries,
                        group_index_hits = metrics.group_index_hits,
                        group_index_misses = metrics.group_index_misses,
                        new_groups = metrics.new_groups,
                        groups = group_count,
                        retained_key_slots_initialized = metrics.retained_key_slots_initialized,
                        string_key_bytes_copied = metrics.string_key_bytes_copied,
                        hasher_setup_ns = metrics.hasher_setup_ns,
                        key_hash_ns = metrics.key_hash_ns,
                        hash_finalize_ns = metrics.hash_finalize_ns,
                        hash_generation_ns = metrics.hash_generation_ns,
                        group_resolution_ns = metrics.group_resolution_ns,
                        group_materialize_ns = metrics.group_materialize_ns,
                        group_ids_distribution_ns = metrics.group_ids_distribution_ns,
                        state_update_ns = metrics.state_update_ns,
                        output_materialize_ns = metrics.output_materialize_ns,
                        "recursive_aggregate_finished"
                    );
                        Ok(bytes)
                    }
                    PhysicalPlanOperator::Sort {
                        input: child,
                        plan,
                        key_program,
                        output_columns,
                    } => {
                        let operator_memory = trace.operator_memory(node_id, memory);
                        let output_columns = usize::try_from(*output_columns).map_err(|_| {
                            VeinError::planning("recursive sort output width is invalid", None)
                        })?;
                        let sort: Box<dyn PipelineNode<DataChunkView>> =
                            Box::new(SortTransform::with_memory_tracker(
                                plan.clone(),
                                Some(output_columns),
                                operator_memory,
                                next,
                            )?);
                        let next: Box<dyn PipelineNode<DataChunkView>> = match key_program {
                            Some(program) => {
                                Box::new(TransformHandle(self.builder.build_expression_executor(program, sort)?))
                            }
                            None => sort,
                        };
                        let bytes = self
                            .execute_input(child, next, context, depth + 1)
                            .await?;
                        schedule.complete_boundary(input, PipelineBoundaryKind::SortInput)?;
                        Ok(bytes)
                    }
                    PhysicalPlanOperator::Limit { input, spec } => {
                        let limit = LimitTransform::new(spec.offset, spec.count, next);
                        self.execute_input(
                            input,
                            Box::new(limit),
                            context,
                            depth + 1,
                        )
                            .await
                    }
                    PhysicalPlanOperator::Sink { .. } => Err(VeinError::planning(
                        "sink operator may only appear at the physical plan root",
                        None,
                    )),
                    PhysicalPlanOperator::Join { probe, build, plan } => {
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
                        let join: Box<dyn PipelineNode<DataChunkView>> = match build_result {
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
                                let downstream: SharedJoinOutput = Arc::new(Mutex::new(next));
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
            }.instrument(node_span).await;
            execution_guard.finish(&result);
            result
        })
    }
}
