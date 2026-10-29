use super::{filter, projection};
use crate::expression::eval::{ExpressionEvaluator, ExpressionKernelProvider};
use crate::pipeline::{DataChunkView, resumable::BatchProcessor};
use common::time::Instant;
use common::{Result, protocol::{ExpressionProgram, ExpressionStep}};
use functions::FunctionRegistry;
use std::sync::Arc;

/// Downstream-free evaluation shared by push adapters and resumable stages.
pub(crate) struct ExpressionProcessor<K: ?Sized> {
    program: Arc<ExpressionProgram>,
    kernels: Arc<K>,
    functions: Arc<FunctionRegistry>,
}
impl<K: ExpressionKernelProvider + ?Sized + 'static> ExpressionProcessor<K> {
    pub(crate) fn new(program: Arc<ExpressionProgram>, kernels: Arc<K>, functions: Arc<FunctionRegistry>) -> Self {
        Self { program, kernels, functions }
    }
    pub(crate) fn fork(&self) -> Self {
        Self::new(Arc::clone(&self.program), Arc::clone(&self.kernels), Arc::clone(&self.functions))
    }
    pub(crate) fn same_configuration(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.program, &other.program) && Arc::ptr_eq(&self.kernels, &other.kernels)
            && Arc::ptr_eq(&self.functions, &other.functions)
    }
    fn evaluate(&self, batch: DataChunkView) -> Result<Option<DataChunkView>> {
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
}
impl<K: ExpressionKernelProvider + ?Sized + 'static> BatchProcessor for ExpressionProcessor<K> {
    fn process(&mut self, batch: DataChunkView) -> Result<Option<DataChunkView>> {
        let started = Instant::now();
        let rows = batch.num_rows();
        let output = self.evaluate(batch)?;
        tracing::trace!(target: "vein.audit", event = "expression_batch_processed",
            rows_in = rows, rows_out = output.as_ref().map_or(0, DataChunkView::num_rows),
            eval_us = started.elapsed().as_micros() as u64, "expression batch processed");
        Ok(output)
    }

    fn fork(&self) -> Result<Box<dyn BatchProcessor>> {
        Ok(Box::new(ExpressionProcessor::fork(self)))
    }

    fn parallelism(&self) -> common::protocol::extension::ParallelismCapability {
        common::protocol::extension::ParallelismCapability::Independent
    }
}
