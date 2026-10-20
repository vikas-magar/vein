use common::Result;

use crate::udf::{FunctionRegistry, FunctionInput, UdfOutput};

/// Plugin interface consumed by expression kernels for scalar functions.
/// Implementations may dispatch to built-ins, user functions, or composed providers.
pub trait ScalarFunctionProvider: Send + Sync {
    /// Evaluates a function against reference-backed logical batch arguments.
    fn invoke(
        &self,
        name: &str,
        arguments: &[FunctionInput<'_>],
        row_count: usize,
    ) -> Result<UdfOutput>;
}

impl ScalarFunctionProvider for FunctionRegistry {
    fn invoke(
        &self,
        name: &str,
        arguments: &[FunctionInput<'_>],
        row_count: usize,
    ) -> Result<UdfOutput> {
        FunctionRegistry::invoke(self, name, arguments, row_count)
    }
}
