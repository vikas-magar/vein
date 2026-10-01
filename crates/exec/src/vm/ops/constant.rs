//! Arrow array construction from [`LogicalLiteral`] values.
//!
//! Provides [`literal_to_array`] to materialize constant values into Arrow
//! arrays for vectorized register operations. There is no separate wire literal:
//! the VM loads the engine's own literal type directly.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array,
    ListArray, NullArray, StringArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use common::Result;
use error::VeinError;
use types::LogicalLiteral;

/// Constructs a length-1 Arrow array holding `literal`, for a broadcast scalar register.
///
/// Element-wise kernels broadcast this single value across the chunk, so a
/// literal costs one element per program instead of one element per row per batch.
pub fn literal_to_scalar(literal: &LogicalLiteral) -> Result<ArrayRef> {
    literal_to_array(literal, 1)
}

/// Constructs an Arrow [`ArrayRef`] of length `row_count` holding copies of `literal`.
///
/// Only used where a constant genuinely has to become a full column, such as the
/// input to a UDF or a projected output column.
pub fn literal_to_array(literal: &LogicalLiteral, row_count: usize) -> Result<ArrayRef> {
    let array: ArrayRef = match literal {
        LogicalLiteral::Null(_) => Arc::new(NullArray::new(row_count)),
        LogicalLiteral::Boolean(v) => Arc::new(BooleanArray::from(vec![*v; row_count])),
        LogicalLiteral::Int64(v) => Arc::new(Int64Array::from(vec![*v; row_count])),
        LogicalLiteral::Float64(v) => Arc::new(Float64Array::from(vec![*v; row_count])),
        LogicalLiteral::String(v) => {
            let iter = std::iter::repeat(v.as_str()).take(row_count);
            Arc::new(StringArray::from_iter_values(iter))
        }
        // `Date32` is a day count and `Time64`/`Timestamp` are microsecond
        // counts, so the logical value is already the Arrow payload.
        LogicalLiteral::Date(v) => Arc::new(Int32Array::from(vec![*v; row_count])),
        LogicalLiteral::Timestamp(v) | LogicalLiteral::Time(v) => {
            Arc::new(Int64Array::from(vec![*v; row_count]))
        }
        LogicalLiteral::Decimal(v, precision, scale) => {
            let values = std::iter::repeat(*v).take(row_count);
            Arc::new(
                Decimal128Array::from_iter_values(values)
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|error| {
                        VeinError::internal(format!("invalid decimal literal: {error}"))
                    })?,
            )
        }
        LogicalLiteral::Vector(elements) => {
            let dim = elements.len();
            let mut flat_values = Vec::with_capacity(row_count * dim);
            for _ in 0..row_count {
                flat_values.extend_from_slice(elements);
            }
            let values_array = Arc::new(Float32Array::from(flat_values));
            let offsets = OffsetBuffer::from_lengths(vec![dim; row_count]);
            let field = Arc::new(Field::new("item", DataType::Float32, false));
            Arc::new(ListArray::new(field, offsets, values_array, None))
        }
        other => {
            return Err(VeinError::internal(format!(
                "literal cannot be loaded into a register yet: {other:?}"
            )));
        }
    };
    Ok(array)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_literal_to_array() {
        let i64_arr = literal_to_array(&LogicalLiteral::Int64(42), 3).unwrap();
        assert_eq!(i64_arr.len(), 3);
        let values = i64_arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.value(0), 42);
        assert_eq!(values.value(2), 42);

        let str_arr = literal_to_array(&LogicalLiteral::String("vein".to_string()), 2).unwrap();
        assert_eq!(str_arr.len(), 2);
        let values = str_arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(values.value(0), "vein");
        assert_eq!(values.value(1), "vein");

        let vec_arr = literal_to_array(&LogicalLiteral::Vector(vec![1.0, 2.0]), 2).unwrap();
        assert_eq!(vec_arr.len(), 2);
    }

    /// A decimal literal has no narrower representation to fall back on, so it
    /// must reach a `Decimal128` array carrying its own precision and scale.
    #[test]
    fn decimal_literal_keeps_its_precision_and_scale() {
        let array = literal_to_array(&LogicalLiteral::Decimal(1234, 15, 2), 1).unwrap();
        let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(array.value(0), 1234);
        assert_eq!(array.precision(), 15);
        assert_eq!(array.scale(), 2);
    }
}
