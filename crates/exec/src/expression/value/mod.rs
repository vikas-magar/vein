//! Reference-backed values used by typed expression kernels.

mod segment;
mod segmented;
mod datum;
mod expression_value;
mod function_input;

pub use datum::ExpressionDatum;
pub use segmented::SegmentedArray;
pub use expression_value::{ExpressionValue, ViewColumnValue};
pub use function_input::ExpressionFunctionInputSource;
pub(crate) use function_input::into_expression_value;

#[cfg(test)]
#[path = "function_input_tests.rs"]
mod function_input_tests;
