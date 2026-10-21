use arrow::datatypes::DataType;
use error::Result;

use crate::{FunctionInput, FunctionOutput};

/// Vectorized scalar function contract over borrowed Arrow-backed inputs.
pub trait ScalarFunction: Send + Sync {
    /// Infers the Arrow output type from the actual argument types.
    fn return_type(&self, argument_types: &[DataType]) -> Result<DataType>;

    /// Evaluates a logical batch without requiring input-column gathering.
    fn invoke(
        &self,
        arguments: &[FunctionInput<'_>],
        row_count: usize,
        return_type: &DataType,
    ) -> Result<FunctionOutput>;
}
