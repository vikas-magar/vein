//! User-Defined Function (UDF) extensibility infrastructure.
//!
//! Provides [`FunctionRegistry`] and [`ScalarUDFImpl`] for registering
//! and dynamically dispatching custom functions from typed expressions.

/// Function registry and Scalar UDF trait definitions.
pub mod registry;
/// Borrowed row mappings for UDFs that can avoid dense input materialization.
pub mod input;
/// Built-in scalar functions supported by the expression evaluator.
pub mod builtins;

pub use builtins::register_builtin_scalars;
pub use input::{MappedUdfInput, MappedUdfRow};
pub use registry::{FunctionRegistry, ScalarUDFImpl};
