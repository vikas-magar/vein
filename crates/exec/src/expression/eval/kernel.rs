use common::Result;
use common::protocol::{ExprKind, PhysicalExpr};

use crate::expression::value::ExpressionValue;
use crate::pipeline::DataChunkView;

use super::functions::ScalarFunctionProvider;

/// Identifies a node in the core typed-expression protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExpressionNodeKind {
    /// A reference to an input column.
    Column,
    /// A constant value.
    Literal,
    /// A binary operation.
    Binary,
    /// A cast operation.
    Cast,
    /// A unary operation.
    Unary,
    /// An `IN` or `NOT IN` expression.
    InList,
    /// A `LIKE` or `NOT LIKE` expression.
    Like,
    /// A conditional expression.
    Case,
    /// A registered scalar function call.
    Call,
}

impl ExpressionNodeKind {
    pub(super) fn of(expression: &PhysicalExpr) -> Self {
        match &expression.kind {
            ExprKind::Column { .. } => Self::Column,
            ExprKind::Literal(_) => Self::Literal,
            ExprKind::Binary { .. } => Self::Binary,
            ExprKind::Cast { .. } => Self::Cast,
            ExprKind::Unary { .. } => Self::Unary,
            ExprKind::InList { .. } => Self::InList,
            ExprKind::Like { .. } => Self::Like,
            ExprKind::Case { .. } => Self::Case,
            ExprKind::Call { .. } => Self::Call,
        }
    }
}

/// Services available to a kernel while evaluating one expression tree.
pub trait ExpressionEvaluationContext {
    /// Returns the active input view.
    fn input(&self) -> &std::sync::Arc<DataChunkView>;
    /// Returns the pluggable scalar-function provider.
    fn functions(&self) -> &dyn ScalarFunctionProvider;
    /// Recursively evaluates a child expression with the same providers.
    fn evaluate(&self, expression: &PhysicalExpr) -> Result<ExpressionValue>;
}

/// Evaluates one category of typed expression node.
pub trait ExpressionKernel: Send + Sync {
    /// Returns the node category handled by this kernel.
    fn kind(&self) -> ExpressionNodeKind;
    /// Evaluates the node using shared recursive evaluation services.
    fn evaluate(
        &self,
        expression: &PhysicalExpr,
        context: &dyn ExpressionEvaluationContext,
    ) -> Result<ExpressionValue>;
}

/// Resolves expression nodes to their registered kernel implementations.
pub trait ExpressionKernelProvider: Send + Sync {
    /// Returns the kernel for a node category, if the provider supports it.
    fn kernel(&self, kind: ExpressionNodeKind) -> Option<&dyn ExpressionKernel>;
}

/// Adapts a function to the [`ExpressionKernel`] interface.
pub struct ExpressionKernelFn<F> {
    kind: ExpressionNodeKind,
    evaluate: F,
}

impl<F> ExpressionKernelFn<F> {
    /// Creates a kernel adapter for one typed node category.
    pub fn new(kind: ExpressionNodeKind, evaluate: F) -> Self {
        Self { kind, evaluate }
    }
}

impl<F> ExpressionKernel for ExpressionKernelFn<F>
where
    F: Fn(&PhysicalExpr, &dyn ExpressionEvaluationContext) -> Result<ExpressionValue>
        + Send
        + Sync,
{
    fn kind(&self) -> ExpressionNodeKind {
        self.kind
    }

    fn evaluate(
        &self,
        expression: &PhysicalExpr,
        context: &dyn ExpressionEvaluationContext,
    ) -> Result<ExpressionValue> {
        (self.evaluate)(expression, context)
    }
}
