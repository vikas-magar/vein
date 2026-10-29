use super::*;
mod source;
mod combine;
mod fan_in;
mod join;
mod reduction;
use source::SourceExecution;

#[derive(Clone, Copy)]
pub(super) struct NodeExecution<'a> {
    pub(super) node: &'a Arc<PhysicalPlanNode>,
    pub(super) context: &'a RecursiveExecutionContext<'a>,
    pub(super) depth: usize,
}


#[cfg(target_arch = "wasm32")]
mod wasm_scan;

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
        next: Box<dyn PipelineNode>,
        context: &'a RecursiveExecutionContext<'a>,
        depth: usize,
    ) -> Pin<Box<dyn Future<Output=Result<u64>> + Send + 'a>> {
        self.execute_input_with_runtime_filters(input, next, context, None, depth)
    }

    /// Execute one input node with filters scoped to its direct source.
    /// Recursive operators use `execute_input` unless they can prove a safe
    /// column-preserving path, which keeps unsupported plans on exact execution.
    fn execute_input_with_runtime_filters<'a>(
        &'a self,
        input: &'a Arc<PhysicalPlanNode>,
        next: Box<dyn PipelineNode>,
        context: &'a RecursiveExecutionContext<'a>,
        runtime_filters: Option<Arc<RuntimeFilterSet>>,
        depth: usize,
    ) -> Pin<Box<dyn Future<Output=Result<u64>> + Send + 'a>> {
        let RecursiveExecutionContext {
            task,
            cancel,
            memory,
            span: _,
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
                let next = trace.observer(
                    trace.node_id(input),
                    next,
                    task.query_id.to_string(),
                    task.task_id.to_string(),
                );
                match &input.operator {
                    PhysicalPlanOperator::Extension { .. } => Err(VeinError::unsupported(
                        "registered physical extensions require the resumable pipeline scheduler", None)),
                    PhysicalPlanOperator::Pipeline { source, program, .. } =>
                        self.execute_source(source, next, SourceExecution {
                            program,
                            context,
                            runtime_filters,
                            node_id,
                        }).await,
                    PhysicalPlanOperator::Transform { input, program } => {
                        let transform = self.builder.build_expression_executor(program, next)?;
                        let bytes = self
                            .execute_input_with_runtime_filters(
                                input,
                                Box::new(TransformHandle(transform)),
                                context,
                                runtime_filters,
                                depth + 1,
                            )
                            .await?;
                        Ok(bytes)
                    }
                    PhysicalPlanOperator::Aggregate { input: child, plan }
                    | PhysicalPlanOperator::Distinct { input: child, plan } =>
                        self.execute_reduction(child, plan, next, NodeExecution { node: input, context, depth }).await,
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
                        let sort: Box<dyn PipelineNode> =
                            Box::new(SortTransform::with_memory_tracker(
                                plan.clone(),
                                Some(output_columns),
                                operator_memory,
                                next,
                            )?);
                        let next: Box<dyn PipelineNode> = match key_program {
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
                    PhysicalPlanOperator::Join { probe, build, plan } =>
                        self.execute_join(probe, build, plan, next, NodeExecution { node: input, context, depth }).await,
                }
            }.instrument(node_span).await;
            execution_guard.finish(&result);
            result
        })
    }
}
