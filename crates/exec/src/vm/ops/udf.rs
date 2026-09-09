//! User-Defined Function (UDF) execution kernel for the Vector VM.
//!
//! Evaluates [`MicroOp::CallUdf`] by dispatching registered functions from
//! [`crate::udf::FunctionRegistry`].

use arrow::array::ArrayRef;
use common::Result;
use error::VeinError;
use crate::udf::FunctionRegistry;

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
}
