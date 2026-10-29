use super::*;

impl DefaultTaskExecutor {
    pub(super) async fn execute_scan_combine(
        &self, sources: Vec<DataSource>, local_nodes: Vec<Box<dyn PipelineNode>>,
        next: Box<dyn PipelineNode>, worker_count: usize, state: SourceExecution<'_>,
    ) -> Result<u64> {
        let SourceExecution { program, context, runtime_filters, node_id } = state;
        let RecursiveExecutionContext { task, cancel, trace, span, .. } = *context;
        let mut canonical: Box<dyn PipelineNode> =
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
                let worker_started = common::time::Instant::now();
                let mut bytes = 0u64;
                let mut work_units = 0u64;
                let mut source_elapsed = std::time::Duration::ZERO;
                let worker_state = Arc::new(crate::operators::source::scan::ScanWorkerState::default());
                let worker_tail: Box<dyn PipelineNode> =
                    Box::new(SuppressFlush::new(local));
                let mut pipeline: Box<dyn PipelineNode> =
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
                        Arc::clone(&worker_state),
                    )?;
                    let source_started = common::time::Instant::now();
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
        canonical.flush().await?;
        trace.record_source_bytes(node_id, bytes);
        return Ok(bytes);
    }
}
