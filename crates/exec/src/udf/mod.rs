//! User-Defined Function (UDF) extensibility infrastructure.
//!
//! Provides [`FunctionRegistry`] and [`ScalarFunction`] for registering
//! and dynamically dispatching custom functions from typed expressions.

/// Function registry and Scalar UDF trait definitions.
pub mod registry;
/// Reference-backed UDF arguments and contiguous segment access.
pub mod input;
/// Validated, reference-preserving UDF outputs.
pub mod output;
/// Built-in scalar functions supported by the expression evaluator.
pub mod builtins;
mod segments;
#[cfg(test)]
pub(crate) mod test_support;

pub use builtins::register_builtin_scalars;
pub use input::{FunctionInput, UdfInputRow, FunctionInputSegment};
pub use output::UdfOutput;
pub use registry::{FunctionRegistry, ScalarFunction};
