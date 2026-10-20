use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use error::Result;

use super::MappedUdfInput;

/// Trait defining a User-Defined Scalar Function (UDF).
///
/// This trait is the cornerstone of extensibility. Whether a function is a built-in
/// math function, a WASM UDF, or an Arrow Flight microservice, it implements this trait.
pub trait ScalarUDFImpl: Debug + Send + Sync {
    /// The canonical name of the function (e.g. "abs", "detect_fraud").
    fn name(&self) -> &str;

    /// Predict the output type of the function based on the input argument types.
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType>;

    /// Execute the function in a vectorized manner against batches of data.
    fn invoke(&self, args: &[ArrayRef]) -> Result<ArrayRef>;

    /// Optionally execute against row-mapped source arrays without gathering inputs.
    ///
    /// Return `Ok(None)` to request the dense [`Self::invoke`] fallback. View
    /// execution permits that fallback only when inputs are already dense; mapped
    /// or segmented inputs are rejected rather than copied into dense arrays.
    /// A mapped implementation receives the whole logical batch and may return
    /// either one result per row or a one-value broadcast result for a non-empty batch.
    fn invoke_mapped(
        &self,
        _args: &[MappedUdfInput<'_>],
        _row_count: usize,
    ) -> Result<Option<ArrayRef>> {
        Ok(None)
    }
}

/// A central registry of all available scalar and aggregate functions.
#[derive(Default)]
pub struct FunctionRegistry {
    scalars: HashMap<String, Arc<dyn ScalarUDFImpl>>,
}

impl FunctionRegistry {
    /// Creates a new empty FunctionRegistry.
    pub fn new() -> Self {
        Self {
            scalars: HashMap::new(),
        }
    }

    /// Registers a scalar UDF.
    pub fn register_scalar(&mut self, udf: Arc<dyn ScalarUDFImpl>) {
        self.scalars.insert(udf.name().to_lowercase(), udf);
    }

    /// Retrieves a scalar UDF by name.
    pub fn get_scalar(&self, name: &str) -> Option<Arc<dyn ScalarUDFImpl>> {
        self.scalars.get(&name.to_lowercase()).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::compute::kernels::numeric;
    use error::VeinError;

    /// A dummy UDF that simply adds 1 to an Int64 column.
    #[derive(Debug)]
    struct AddOneUdf;

    impl ScalarUDFImpl for AddOneUdf {
        fn name(&self) -> &str {
            "add_one"
        }

        fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
            if arg_types.len() != 1 {
                return Err(VeinError::internal("add_one expects exactly 1 argument"));
            }
            if arg_types[0] != DataType::Int64 {
                return Err(VeinError::internal("add_one expects an Int64 argument"));
            }
            Ok(DataType::Int64)
        }

        fn invoke(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
            let input = &args[0];
            let ones = Arc::new(Int64Array::from(vec![1; input.len()])) as ArrayRef;
            numeric::add(input, &ones).map_err(|e| VeinError::internal(format!("UDF error: {}", e)))
        }
    }

    #[test]
    fn test_function_registry_and_invocation() {
        let mut registry = FunctionRegistry::new();
        let udf: Arc<dyn ScalarUDFImpl> = Arc::new(AddOneUdf);
        registry.register_scalar(udf.clone());

        // Verify retrieval
        let retrieved = registry.get_scalar("ADD_ONE").unwrap();
        assert_eq!(retrieved.name(), "add_one");

        // Test direct UDF invocation with array batches
        let array1 = Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef;
        let result = retrieved.invoke(&[array1]).unwrap();
        let result_int = result.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(result_int.value(0), 11);
        assert_eq!(result_int.value(1), 21);
        assert_eq!(result_int.value(2), 31);
    }
}
