//! User-Defined Function (UDF) extensibility infrastructure.
//!
//! Provides [`FunctionRegistry`] and [`ScalarUDFImpl`] for registering
//! and dynamically dispatching custom functions within the Vector VM.

/// Function registry and Scalar UDF trait definitions.
pub mod registry;

pub use registry::{FunctionRegistry, ScalarUDFImpl};
