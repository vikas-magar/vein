use std::any::Any;
use std::fmt::Debug;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Schema};
use crate::daemon::pipeline::DataChunk;
use error::Result;

/// A physical expression that can be evaluated against a [`DataChunk`].
pub trait PhysicalExpr: Debug + Send + Sync {
    /// Returns the expression as `Any` so it can be downcast for matching.
    fn as_any(&self) -> &dyn Any;

    /// Get the data type of this expression, given the schema of the input.
    fn data_type(&self, input_schema: &Schema) -> Result<DataType>;

    /// Indicates whether this expression always produces the same output for the same input.
    fn is_deterministic(&self) -> bool {
        true
    }

    /// Evaluate the expression directly against an in-flight [`DataChunk`].
    fn evaluate(&self, chunk: &DataChunk) -> Result<ArrayRef>;
}

/// Binary expression implementations.
pub mod binary;
/// Cast expression implementations.
pub mod cast;
/// Column expression implementations.
pub mod column;
/// Literal expression implementations.
pub mod literal;
/// Function registry and Scalar UDF implementations.
pub mod registry;

pub use binary::{BinaryExpr, BinaryOperator};
pub use cast::CastExpr;
pub use column::ColumnExpr;
pub use literal::{LiteralExpr, LiteralValue};
pub use registry::{FunctionRegistry, ScalarFunctionExpr, ScalarUDFImpl};
