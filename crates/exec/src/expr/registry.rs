use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Schema};
use error::Result;

use super::PhysicalExpr;
use std::fmt::Debug;

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
}

/// A physical expression that invokes a `ScalarUDFImpl` over a set of child argument expressions.
#[derive(Debug)]
pub struct ScalarFunctionExpr {
    udf: Arc<dyn ScalarUDFImpl>,
    args: Vec<Box<dyn PhysicalExpr>>,
}

impl ScalarFunctionExpr {
    /// Creates a new `ScalarFunctionExpr`.
    pub fn new(udf: Arc<dyn ScalarUDFImpl>, args: Vec<Box<dyn PhysicalExpr>>) -> Self {
        Self { udf, args }
    }
}

impl PhysicalExpr for ScalarFunctionExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        let mut arg_types = Vec::with_capacity(self.args.len());
        for arg in &self.args {
            arg_types.push(arg.data_type(input_schema)?);
        }
        self.udf.return_type(&arg_types)
    }

    fn evaluate(&self, chunk: &crate::daemon::pipeline::DataChunk) -> Result<ArrayRef> {
        // 1. Evaluate all child expressions to get the argument arrays
        let mut arg_arrays = Vec::with_capacity(self.args.len());
        for arg in &self.args {
            arg_arrays.push(arg.evaluate(chunk)?);
        }

        // 2. Invoke the UDF implementation with the materialized arrays
        self.udf.invoke(&arg_arrays)
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
    use crate::expr::column::ColumnExpr;
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

        // Test PhysicalExpr execution
        let array1 = Arc::new(Int64Array::from(vec![10, 20, 30]));
        let arg = Box::new(ColumnExpr::new("a", 0));
        let func_expr = ScalarFunctionExpr::new(retrieved, vec![arg]);

        let chunk = crate::daemon::pipeline::DataChunk::from_vectors(vec![array1 as ArrayRef]);
        let result = func_expr.evaluate(&chunk).unwrap();
        let result_int = result.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(result_int.value(0), 11);
        assert_eq!(result_int.value(1), 21);
        assert_eq!(result_int.value(2), 31);
    }
}
