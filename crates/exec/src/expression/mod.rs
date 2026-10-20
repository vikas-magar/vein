//! Typed scalar expression execution over Arrow-backed views.

/// Registered, extensible kernels for typed expression nodes.
pub mod eval;
/// Pipeline adapter for executing typed expression programs over data views.
pub(crate) mod executor;
/// Arrow kernels used by the expression evaluator and scalar-function providers.
pub(crate) mod kernels;
pub mod value;

pub use eval::{
    ExpressionEvaluationContext, ExpressionEvaluator, ExpressionKernel, ExpressionKernelFn,
    ExpressionKernelProvider, ExpressionKernelRegistry, ExpressionNodeKind,
    ScalarFunctionProvider,
};
pub use value::{ExpressionDatum, ExpressionValue, SegmentedArray, ViewColumnValue};
