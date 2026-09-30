//! Arrow array construction from [`ScalarConstant`] values.
//!
//! Provides [`scalar_constant_to_array`] to materialize constant scalar values
//! into full-length Arrow arrays for vectorized register operations.

use std::sync::Arc;
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, ListArray, NullArray, StringArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use common::protocol::ScalarConstant;
use common::Result;

/// Constructs a length-1 Arrow array holding `constant`, for a broadcast scalar register.
///
/// Element-wise kernels broadcast this single value across the chunk, so a
/// literal costs one element per program instead of one element per row per batch.
pub fn scalar_constant_to_scalar(constant: &ScalarConstant) -> Result<ArrayRef> {
    scalar_constant_to_array(constant, 1)
}

/// Constructs an Arrow [`ArrayRef`] of length `row_count` holding copies of `constant`.
///
/// Only used where a constant genuinely has to become a full column, such as the
/// input to a UDF or a projected output column.
pub fn scalar_constant_to_array(constant: &ScalarConstant, row_count: usize) -> Result<ArrayRef> {
    let array: ArrayRef = match constant {
        ScalarConstant::Null => Arc::new(NullArray::new(row_count)),
        ScalarConstant::Boolean(v) => Arc::new(BooleanArray::from(vec![*v; row_count])),
        ScalarConstant::Int8(v) => Arc::new(Int8Array::from(vec![*v; row_count])),
        ScalarConstant::Int16(v) => Arc::new(Int16Array::from(vec![*v; row_count])),
        ScalarConstant::Int32(v) => Arc::new(Int32Array::from(vec![*v; row_count])),
        ScalarConstant::Int64(v) => Arc::new(Int64Array::from(vec![*v; row_count])),
        ScalarConstant::UInt8(v) => Arc::new(UInt8Array::from(vec![*v; row_count])),
        ScalarConstant::UInt16(v) => Arc::new(UInt16Array::from(vec![*v; row_count])),
        ScalarConstant::UInt32(v) => Arc::new(UInt32Array::from(vec![*v; row_count])),
        ScalarConstant::UInt64(v) => Arc::new(UInt64Array::from(vec![*v; row_count])),
        ScalarConstant::Float32(v) => Arc::new(Float32Array::from(vec![*v; row_count])),
        ScalarConstant::Float64(v) => Arc::new(Float64Array::from(vec![*v; row_count])),
        ScalarConstant::Utf8(v) => {
            let iter = std::iter::repeat(v.as_str()).take(row_count);
            Arc::new(StringArray::from_iter_values(iter))
        }
        ScalarConstant::Binary(v) => {
            let iter = std::iter::repeat(v.as_slice()).take(row_count);
            Arc::new(BinaryArray::from_iter_values(iter))
        }
        ScalarConstant::Vector(elements) => {
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
    };
    Ok(array)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scalar_constant_to_array() {
        let i64_arr = scalar_constant_to_array(&ScalarConstant::Int64(42), 3).unwrap();
        assert_eq!(i64_arr.len(), 3);
        let downcasted = i64_arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(downcasted.value(0), 42);
        assert_eq!(downcasted.value(1), 42);
        assert_eq!(downcasted.value(2), 42);

        let str_arr = scalar_constant_to_array(&ScalarConstant::Utf8("vein".to_string()), 2).unwrap();
        assert_eq!(str_arr.len(), 2);
        let downcasted_str = str_arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(downcasted_str.value(0), "vein");
        assert_eq!(downcasted_str.value(1), "vein");

        let vec_arr = scalar_constant_to_array(&ScalarConstant::Vector(vec![1.0, 2.0]), 2).unwrap();
        assert_eq!(vec_arr.len(), 2);
    }
}
