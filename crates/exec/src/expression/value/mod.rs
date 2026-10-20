//! Reference-backed values used by typed expression kernels.

mod segment;
mod segmented;
mod datum;
mod value;

pub use datum::ExpressionDatum;
pub use segmented::SegmentedArray;
pub use value::{ExpressionValue, ViewColumnValue};
