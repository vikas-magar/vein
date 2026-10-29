use super::*;
use common::protocol::ExpressionProgram;

pub(super) struct SourceExecution<'a> {
    pub(super) program: &'a Arc<ExpressionProgram>,
    pub(super) context: &'a RecursiveExecutionContext<'a>,
    pub(super) runtime_filters: Option<Arc<RuntimeFilterSet>>,
    pub(super) node_id: u32,
}

impl DefaultTaskExecutor {
    pub(super) async fn execute_source(
        &self, data_source: &DataSource, next: Box<dyn PipelineNode>, state: SourceExecution<'_>,
    ) -> Result<u64> {
        let SourceExecution { program, context, runtime_filters, node_id } = state;
        let RecursiveExecutionContext { task, cancel, trace, span, .. } = *context;
        if let DataSource::Intermediate(source_id) = data_source {
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
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(VeinError::cancelled()),
                result = source.execute(pipeline.as_mut()) => result?,
            }
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
            empty_pipeline.flush().await?;
            trace.record_source_bytes(node_id, 0);
            return Ok(0);
        }
        #[cfg(target_arch = "wasm32")]
        if sources.len() > 1 {
            return wasm_scan::execute(
                self.builder.as_ref(),
                sources,
                program,
                task,
                cancel,
                runtime_filters,
                trace.query_statistics_context(),
                span.clone(),
                next,
            )
                .await;
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
                return self.execute_scan_combine(sources, local_nodes, next, worker_count,
                                                 SourceExecution { program, context, runtime_filters, node_id }).await;
            }
            return self.execute_scan_fan_in(sources, next, worker_count, fan_in_fallback,
                                            SourceExecution { program, context, runtime_filters, node_id }).await;
        }
        let source_spec = &sources[0];
        let mut source = self.builder.build_source_with_query_context(
            source_spec,
            task.task_id,
            task.attempt,
            runtime_filters.as_deref(),
            Some(trace.query_statistics_context()),
            Arc::default(),
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
}
