//! User-Defined Function (UDF) execution kernel for the Vector VM.
//!
//! Evaluates [`MicroOp::CallUdf`] by dispatching registered functions from
//! [`crate::udf::FunctionRegistry`].

use arrow::array::ArrayRef;
use common::Result;
use error::VeinError;
use crate::udf::{FunctionRegistry, MappedUdfInput};

/// Dispatches a registered UDF by name against evaluated argument registers.
pub fn eval_call_udf(
    registry: &FunctionRegistry,
    name: &str,
    args: &[ArrayRef],
) -> Result<ArrayRef> {
    let udf = registry.get_scalar(name).ok_or_else(|| {
        VeinError::internal(format!("UDF '{name}' not found in function registry"))
    })?;

    udf.invoke(args)
}

/// Offers mapped inputs to a UDF before the VM materializes its dense fallback.
pub fn eval_call_udf_mapped(
    registry: &FunctionRegistry,
    name: &str,
    args: &[MappedUdfInput<'_>],
    row_count: usize,
) -> Result<Option<ArrayRef>> {
    let udf = registry.get_scalar(name).ok_or_else(|| {
        VeinError::internal(format!("UDF '{name}' not found in function registry"))
    })?;
    let result = udf.invoke_mapped(args, row_count)?;
    if let Some(array) = &result {
        let valid_length = array.len() == row_count || (row_count > 0 && array.len() == 1);
        if !valid_length {
            return Err(VeinError::internal(format!(
                "mapped UDF '{name}' returned {} rows for a batch of {row_count}",
                array.len()
            )));
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow::array::Int64Array;
    use arrow::datatypes::DataType;
    use crate::udf::ScalarUDFImpl;

    #[derive(Debug)]
    struct AddOneUdf;

    impl ScalarUDFImpl for AddOneUdf {
        fn name(&self) -> &str {
            "add_one"
        }

        fn return_type(&self, _arg_types: &[DataType]) -> error::Result<DataType> {
            Ok(DataType::Int64)
        }

        fn invoke(&self, args: &[ArrayRef]) -> error::Result<ArrayRef> {
            let int_arr = args[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| VeinError::internal("Expected Int64Array"))?;
            let res: Int64Array = int_arr.iter().map(|v| v.map(|x| x + 1)).collect();
            Ok(Arc::new(res))
        }
    }

    #[test]
    fn test_eval_call_udf() {
        let mut registry = FunctionRegistry::new();
        registry.register_scalar(Arc::new(AddOneUdf));

        let arg: ArrayRef = Arc::new(Int64Array::from(vec![10, 20]));
        let result = eval_call_udf(&registry, "add_one", &[arg]).unwrap();
        let down = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(down.value(0), 11);
        assert_eq!(down.value(1), 21);

        // Unknown UDF
        assert!(eval_call_udf(&registry, "unknown_fn", &[]).is_err());
    }

    #[derive(Debug)]
    struct WrongLengthUdf;

    impl ScalarUDFImpl for WrongLengthUdf {
        fn name(&self) -> &str {
            "wrong_length"
        }

        fn return_type(&self, _arg_types: &[DataType]) -> error::Result<DataType> {
            Ok(DataType::Int64)
        }

        fn invoke(&self, _args: &[ArrayRef]) -> error::Result<ArrayRef> {
            unreachable!("mapped invocation is supported")
        }

        fn invoke_mapped(
            &self,
            _args: &[MappedUdfInput<'_>],
            _row_count: usize,
        ) -> error::Result<Option<ArrayRef>> {
            Ok(Some(Arc::new(Int64Array::from(vec![1, 2, 3]))))
        }
    }

    #[test]
    fn mapped_udf_rejects_output_with_invalid_batch_length() {
        let mut registry = FunctionRegistry::new();
        registry.register_scalar(Arc::new(WrongLengthUdf));

        let error = eval_call_udf_mapped(&registry, "wrong_length", &[], 2)
            .expect_err("mapped UDF output must match the logical batch length");
        assert!(error.to_string().contains("returned 3 rows for a batch of 2"));
    }
}
