use std::sync::Arc;

use arrow::array::{Float64Array, new_null_array};
use arrow::datatypes::DataType;
use error::{Result, VeinError};

use super::super::{ScalarFunction, FunctionInput, UdfOutput};
use crate::udf::segments::invoke_segments;

#[derive(Debug)]
pub(super) struct RoundFunction;

impl ScalarFunction for RoundFunction {
    fn name(&self) -> &str {
        "round"
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types != [DataType::Float64] {
            return Err(VeinError::planning(
                "ROUND expects one Float64 argument",
                None,
            ));
        }
        Ok(DataType::Float64)
    }

    fn invoke(
        &self,
        args: &[FunctionInput<'_>],
        rows: usize,
        return_type: &DataType,
    ) -> Result<UdfOutput> {
        invoke_segments(args, rows, return_type, |segments, segment_rows| {
            let [input] = segments else {
                return Err(VeinError::internal("ROUND expects one argument"));
            };
            let Some(array) = input.array() else {
                return Ok(new_null_array(return_type, segment_rows));
            };
            let input = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| VeinError::internal("ROUND expects a Float64 array"))?;
            let output: Float64Array =
                arrow::compute::kernels::arity::unary(input, f64::round);
            Ok(Arc::new(output))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ArrayRef};

    #[test]
    fn rounds_float64_values_and_preserves_nulls() {
        let input = Arc::new(Float64Array::from(vec![
            Some(1.2),
            Some(1.5),
            Some(-1.5),
            None,
            Some(f64::NAN),
        ])) as ArrayRef;
        let output = crate::udf::test_support::invoke_dense(&RoundFunction, vec![input])
            .expect("round values");
        let values = output
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64 output");

        assert_eq!(values.value(0), 1.0);
        assert_eq!(values.value(1), 2.0);
        assert_eq!(values.value(2), -2.0);
        assert!(values.is_null(3));
        assert!(values.value(4).is_nan());
    }

    #[test]
    fn validates_round_signature_and_runtime_arguments() {
        assert_eq!(
            RoundFunction
                .return_type(&[DataType::Float64])
                .expect("valid signature"),
            DataType::Float64
        );
        assert!(RoundFunction.return_type(&[DataType::Int64]).is_err());
        assert!(RoundFunction.return_type(&[]).is_err());

        let input = Arc::new(arrow::array::Int64Array::from(vec![1])) as ArrayRef;
        assert!(crate::udf::test_support::invoke_dense(&RoundFunction, vec![input]).is_err());
    }
}
