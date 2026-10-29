use super::*;

impl DefaultTaskExecutor {
    pub(super) async fn execute_scan_fan_in(
        &self, sources: Vec<DataSource>, mut next: Box<dyn PipelineNode>, worker_count: usize,
        fan_in_fallback: Option<&'static str>, state: SourceExecution<'_>,
    ) -> Result<u64> {
        let SourceExecution { program, context, runtime_filters, node_id } = state;
        let RecursiveExecutionContext { task, cancel, trace, span, .. } = *context;
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
        let query_statistics = trace.query_statistics_context();
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
            let query_statistics = Arc::clone(&query_statistics);
            workers.spawn_blocking(move || {
                let mut bytes = 0u64;
                let worker_state = Arc::new(crate::operators::source::scan::ScanWorkerState::default());
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
                    let mut source = builder.build_source_with_query_context(
                        &source_spec,
                        task_id,
                        attempt,
                        runtime_filters.as_deref(),
                        Some(Arc::clone(&query_statistics)),
                        Arc::clone(&worker_state),
                    )?;
                    let channel: Box<dyn PipelineNode> = Box::new(
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
                    if let Err(error) = next.push(chunk).await {
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
        next.flush().await?;
        trace.record_source_bytes(node_id, bytes);
        return Ok(bytes);
    }
}
