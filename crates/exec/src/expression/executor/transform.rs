use std::sync::Arc;
use std::time::Instant;

use common::Result;
use common::protocol::{ExpressionProgram, ExpressionStep};
use error::VeinError;

use crate::expression::eval::{
    ExpressionEvaluator, ExpressionKernelProvider, ScalarFunctionProvider,
};
use crate::pipeline::{DataChunkView, PipelineNode, Transform};

use super::{filter, projection};

#[derive(Default)]
struct ExecutionMetrics {
    chunks: u64,
    rows_in: u64,
    rows_out: u64,
    elapsed_us: u64,
}

impl ExecutionMetrics {
    fn record(&mut self, rows_in: usize, rows_out: usize, elapsed_us: u64) {
        self.chunks += 1;
        self.rows_in += rows_in as u64;
        self.rows_out += rows_out as u64;
        self.elapsed_us += elapsed_us;
    }

    fn combine(&mut self, other: Self) {
        self.chunks += other.chunks;
        self.rows_in += other.rows_in;
        self.rows_out += other.rows_out;
        self.elapsed_us += other.elapsed_us;
    }
}

/// Executes a typed expression program directly over reference-backed views.
pub(crate) struct ExpressionExecutor<K: ?Sized> {
    program: Arc<ExpressionProgram>,
    kernels: Arc<K>,
    functions: Arc<dyn ScalarFunctionProvider>,
    next: Box<dyn PipelineNode>,
    metrics: ExecutionMetrics,
}

impl<K> ExpressionExecutor<K>
where
    K: ExpressionKernelProvider + ?Sized + 'static,
{
    pub(crate) fn new(
        program: Arc<ExpressionProgram>,
        kernels: Arc<K>,
        functions: Arc<dyn ScalarFunctionProvider>,
        next: Box<dyn PipelineNode>,
    ) -> Self {
        Self { program, kernels, functions, next, metrics: ExecutionMetrics::default() }
    }

    fn execute(&self, batch: DataChunkView) -> Result<Option<DataChunkView>> {
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        let mut view = Arc::new(batch);
        for step in &self.program.steps {
            match step {
                ExpressionStep::Filter(expression) => {
                    let evaluator = ExpressionEvaluator::new(
                        self.kernels.as_ref(), self.functions.as_ref(),
                    );
                    let predicate = evaluator.evaluate(expression, &view)?;
                    filter::validate_rows(&predicate, view.num_rows())?;
                    view = Arc::new(filter::apply(&view, &predicate)?);
                }
                ExpressionStep::Projection(expressions) => {
                    view = Arc::new(projection::apply(
                        &view, expressions, self.kernels.as_ref(), self.functions.as_ref(),
                    )?);
                }
            }
            if view.num_rows() == 0 {
                return Ok(None);
            }
        }
        Ok(Some(Arc::try_unwrap(view).unwrap_or_else(|view| view.as_ref().clone())))
    }

    fn fork_with_next(&self, next: Box<dyn PipelineNode>) -> Self {
        Self::new(
            Arc::clone(&self.program), Arc::clone(&self.kernels),
            Arc::clone(&self.functions), next,
        )
    }
}

impl<K> PipelineNode for ExpressionExecutor<K>
where
    K: ExpressionKernelProvider + ?Sized + 'static,
{
    fn push(&mut self, batch: DataChunkView) -> Result<()> {
        let started = Instant::now();
        let rows_in = batch.num_rows();
        let result = self.execute(batch)?;
        let rows_out = result.as_ref().map_or(0, DataChunkView::num_rows);
        self.metrics.record(rows_in, rows_out, started.elapsed().as_micros() as u64);
        if let Some(view) = result {
            self.next.push(view)?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        let metrics = std::mem::take(&mut self.metrics);
        if metrics.chunks > 0 {
            tracing::debug!(target: "vein.audit", event = "expression_executor_finished",
                chunks = metrics.chunks, rows_in = metrics.rows_in,
                rows_out = metrics.rows_out, eval_us = metrics.elapsed_us,
                "expression executor completed");
        }
        self.next.flush()
    }

    fn finished(&self) -> bool {
        self.next.finished()
    }

    fn fork_local(&self) -> Result<Option<Box<dyn PipelineNode>>> {
        let Some(next) = self.next.fork_local()? else { return Ok(None) };
        Ok(Some(Box::new(self.fork_with_next(next))))
    }

    fn combine_local(self: Box<Self>, canonical: &mut dyn PipelineNode) -> Result<()> {
        let target = canonical.as_any_mut().downcast_mut::<Self>()
            .ok_or_else(|| VeinError::internal("expression executor local state type mismatch"))?;
        combine_state(self, target)
    }
}

impl<K> Transform for ExpressionExecutor<K>
where
    K: ExpressionKernelProvider + ?Sized + 'static,
{
    fn fork_transform(&self) -> Result<Option<Box<dyn Transform>>> {
        let Some(next) = self.next.fork_local()? else { return Ok(None) };
        Ok(Some(Box::new(self.fork_with_next(next))))
    }

    fn combine_transform(
        self: Box<Self>,
        canonical: &mut dyn Transform,
    ) -> Result<()> {
        let target = canonical.as_any_mut().downcast_mut::<Self>()
            .ok_or_else(|| VeinError::internal("expression executor transform type mismatch"))?;
        combine_state(self, target)
    }
}

fn combine_state<K>(local: Box<ExpressionExecutor<K>>, canonical: &mut ExpressionExecutor<K>) -> Result<()>
where
    K: ExpressionKernelProvider + ?Sized + 'static,
{
    if !Arc::ptr_eq(&local.program, &canonical.program)
        || !Arc::ptr_eq(&local.kernels, &canonical.kernels)
        || !Arc::ptr_eq(&local.functions, &canonical.functions)
    {
        return Err(VeinError::internal("cannot combine different expression executor configurations"));
    }
    let ExpressionExecutor { next, metrics, .. } = *local;
    next.combine_local(canonical.next.as_mut())?;
    canonical.metrics.combine(metrics);
    Ok(())
}
