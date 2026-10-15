//! Type-conversion execution kernel for the Vector VM.
//!
//! Evaluates [`common::protocol::MicroOp::Cast`] by delegating to Arrow's cast
//! kernel so the conversion rules (numeric widening/narrowing, string parsing,
//! temporal conversion) stay in one place instead of being re-implemented here.

use arrow::array::ArrayRef;
use arrow::compute::{CastOptions, cast_with_options};
use common::Result;
use error::VeinError;
use types::VeinDataType;

/// Converts `src` to the Arrow layout of `target`.
///
/// Casts are checked: a value that does not fit `target` — an out-of-range
/// integer, an unparsable string — fails the task rather than being wrapped.
///
/// Arrow's `safe` flag is inverted from its name: `true` substitutes NULL for a
/// failed element and hides the problem, `false` reports it as an error. It is
/// therefore set explicitly here instead of relying on the default.
pub fn eval_cast(src: &ArrayRef, target: &VeinDataType) -> Result<ArrayRef> {
    let to = target.to_arrow();
    let options = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    cast_with_options(src.as_ref(), &to, &options)
        .map_err(|e| VeinError::internal(format!("cast to {target:?} failed: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow::array::{Array, Float64Array, Int64Array};

    #[test]
    fn test_cast_int64_to_float64() {
        let src: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let out = eval_cast(&src, &VeinDataType::Float64).unwrap();
        assert_eq!(out.data_type(), &arrow::datatypes::DataType::Float64);
        let values = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(values.value(0), 1.0);
        assert_eq!(values.value(2), 3.0);
    }

    #[test]
    fn test_cast_null_preserves_nulls() {
        let src: ArrayRef = Arc::new(Int64Array::from(vec![Some(7), None]));
        let out = eval_cast(&src, &VeinDataType::Float64).unwrap();
        let values = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(values.value(0), 7.0);
        assert!(values.is_null(1));
    }

    #[test]
    fn test_narrowing_cast_reports_overflow() {
        let src: ArrayRef = Arc::new(Int64Array::from(vec![i64::MAX]));
        assert!(eval_cast(&src, &VeinDataType::Int32).is_err());
    }

    #[test]
    fn test_unparsable_string_cast_fails() {
        let src: ArrayRef = Arc::new(arrow::array::StringArray::from(vec!["not-a-number"]));
        assert!(eval_cast(&src, &VeinDataType::Int64).is_err());
    }
}
