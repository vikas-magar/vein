//! User-Defined Function (UDF) extensibility infrastructure.
//!
//! Provides [`FunctionRegistry`] and [`ScalarUDFImpl`] for registering
//! and dynamically dispatching custom functions within the Vector VM.

/// Function registry and Scalar UDF trait definitions.
pub mod registry;
/// Built-in scalar functions supported by the Vector VM.
pub mod builtins;

pub use builtins::register_builtin_scalars;
pub use registry::{FunctionRegistry, ScalarUDFImpl};
