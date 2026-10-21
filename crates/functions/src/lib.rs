#![deny(missing_docs)]

//! Function metadata and runtime implementations shared by binding and execution.
//!
//! This crate is the shared function catalog and runtime extension surface.
//! It has no dependency on the binder, physical planner, or executor.

/// Built-in SQL function definitions and registration.
pub mod builtins;
/// Aggregate identities consumed by the physical aggregate operator.
pub mod aggregate;
/// Function definitions, signatures, categories, and runtime implementations.
pub mod function;
/// Borrowed, reference-backed inputs for function implementations.
pub mod input;
/// Validated function results, including zero-copy input reuse.
pub mod output;
/// Shared registry and catalog interfaces.
pub mod registry;
/// Scalar runtime extension trait.
pub mod scalar;
mod segments;
#[cfg(test)]
mod test_support;

pub use aggregate::{AggregateAccumulator, AggregateFunction, AggregateOperation, aggregate_operation};
pub use function::{
    FunctionDefinition, FunctionImplementation, FunctionKind, FunctionSignature, RuntimeKind,
    Volatility,
};
pub use input::{FunctionInput, FunctionInputRow, FunctionInputSegment, FunctionInputSource};
pub use output::FunctionOutput;
pub use registry::{FunctionCatalog, FunctionRegistry};
pub use scalar::ScalarFunction;
