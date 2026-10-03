use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array};
use arrow::datatypes::DataType;
use error::{Result, VeinError};

use super::super::ScalarUDFImpl;

#[derive(Debug)]
pub(super) struct RoundFunction;

impl ScalarUDFImpl for RoundFunction {
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

    fn invoke(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        let [input] = args else {
            return Err(VeinError::internal("ROUND expects one argument"));
        };
        let input = input
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| VeinError::internal("ROUND expects a Float64 array"))?;
        let output: Float64Array =
            arrow::compute::kernels::arity::unary(input, f64::round);
        Ok(Arc::new(output))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    #[test]
    fn rounds_float64_values_and_preserves_nulls() {
        let input = Arc::new(Float64Array::from(vec![
            Some(1.2),
            Some(1.5),
            Some(-1.5),
            None,
            Some(f64::NAN),
        ])) as ArrayRef;
        let output = RoundFunction.invoke(&[input]).expect("round values");
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
        assert!(RoundFunction.invoke(&[]).is_err());

        let input = Arc::new(arrow::array::Int64Array::from(vec![1])) as ArrayRef;
        assert!(RoundFunction.invoke(&[input]).is_err());
    }
}
