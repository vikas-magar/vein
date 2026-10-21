mod binary;
mod complex;
mod builtin;
mod kernel;
mod kernel_registry;
mod predicate;
mod unary;

pub use kernel::{
    ExpressionEvaluationContext, ExpressionKernel, ExpressionKernelFn,
    ExpressionKernelProvider, ExpressionNodeKind,
};
pub use kernel_registry::ExpressionKernelRegistry;

use std::sync::Arc;

use arrow::array::BooleanArray;
use common::Result;
use common::protocol::PhysicalExpr;
use error::VeinError;
use functions::FunctionRegistry;

use crate::expression::value::ExpressionValue;
use crate::pipeline::DataChunkView;

/// Generic evaluator that dispatches typed expressions through injected providers.
pub struct ExpressionEvaluator<'a, K: ?Sized>
where
    K: ExpressionKernelProvider,
{
    kernels: &'a K,
    functions: &'a FunctionRegistry,
}

impl<'a, K: ?Sized> ExpressionEvaluator<'a, K>
where
    K: ExpressionKernelProvider,
{
    /// Creates an evaluator over the supplied kernels and shared function registry.
    pub fn new(kernels: &'a K, functions: &'a FunctionRegistry) -> Self {
        Self { kernels, functions }
    }

    /// Evaluates an expression while retaining reference-backed input columns.
    pub fn evaluate(
        &self,
        expression: &PhysicalExpr,
        input: &Arc<DataChunkView>,
    ) -> Result<ExpressionValue> {
        evaluate_inner(expression, input, self.kernels, self.functions)
    }

    /// Evaluates a filter expression into the Boolean mask required by a source
    /// reader, preserving computed Arrow buffers and gathering only view-backed
    /// masks into the reader's required output representation.
    pub fn evaluate_boolean(
        &self,
        expression: &PhysicalExpr,
        input: &Arc<DataChunkView>,
    ) -> Result<BooleanArray> {
        predicate::evaluate_boolean(self.evaluate(expression, input)?, input.num_rows())
    }
}

struct KernelContext<'a, K: ?Sized>
where
    K: ExpressionKernelProvider,
{
    input: &'a Arc<DataChunkView>,
    kernels: &'a K,
    functions: &'a FunctionRegistry,
}

impl<K: ?Sized> ExpressionEvaluationContext for KernelContext<'_, K>
where
    K: ExpressionKernelProvider,
{
    fn input(&self) -> &Arc<DataChunkView> { self.input }

    fn functions(&self) -> &FunctionRegistry { self.functions }

    fn evaluate(&self, expression: &PhysicalExpr) -> Result<ExpressionValue> {
        evaluate_inner(expression, self.input, self.kernels, self.functions)
    }
}

fn evaluate_inner<K: ?Sized>(
    expression: &PhysicalExpr,
    input: &Arc<DataChunkView>,
    kernels: &K,
    functions: &FunctionRegistry,
) -> Result<ExpressionValue>
where
    K: ExpressionKernelProvider,
{
    let kind = ExpressionNodeKind::of(expression);
    let kernel = kernels.kernel(kind).ok_or_else(|| {
        VeinError::unsupported(format!("no expression kernel registered for {kind:?}"), None)
    })?;
    let context = KernelContext { input, kernels, functions };
    let result = kernel.evaluate(expression, &context)?;
    validate_result(expression, &result)?;
    Ok(result)
}

fn validate_result(expression: &PhysicalExpr, value: &ExpressionValue) -> Result<()> {
    let expected = expression.return_type.to_arrow();
    if value.data_type() != &expected {
        return Err(VeinError::internal(format!(
            "physical expression returned {:?}, expected {expected:?}",
            value.data_type()
        )));
    }
    Ok(())
}

fn normalize_scalar(value: ExpressionValue, scalar: bool) -> ExpressionValue {
    if !scalar { return value; }
    match value {
        ExpressionValue::Array(array) if array.len() == 1 => ExpressionValue::Scalar(array),
        value => value,
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
