use arrow::array::ArrayRef;
use common::Result;

use crate::udf::{FunctionRegistry, MappedUdfInput};

/// Plugin interface consumed by expression kernels for scalar functions.
/// Implementations may dispatch to built-ins, user functions, or composed providers.
pub trait ScalarFunctionProvider: Send + Sync {
    /// Evaluates a function against dense Arrow arguments.
    fn invoke(&self, name: &str, arguments: &[ArrayRef]) -> Result<ArrayRef>;

    /// Evaluates a function against reference-backed arguments when supported.
    fn invoke_mapped(
        &self,
        name: &str,
        arguments: &[MappedUdfInput<'_>],
        row_count: usize,
    ) -> Result<Option<ArrayRef>>;
}

impl ScalarFunctionProvider for FunctionRegistry {
    fn invoke(&self, name: &str, arguments: &[ArrayRef]) -> Result<ArrayRef> {
        crate::expression::kernels::eval_call_udf(self, name, arguments)
    }

    fn invoke_mapped(
        &self,
        name: &str,
        arguments: &[MappedUdfInput<'_>],
        row_count: usize,
    ) -> Result<Option<ArrayRef>> {
        crate::expression::kernels::eval_call_udf_mapped(self, name, arguments, row_count)
    }
}
