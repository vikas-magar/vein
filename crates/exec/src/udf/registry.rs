use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::datatypes::DataType;
use error::{Result, VeinError};

use super::{FunctionInput, UdfOutput};

/// Runtime contract for extensible, reference-aware scalar functions.
pub trait ScalarFunction: Debug + Send + Sync {
    /// Canonical function name used by expression calls.
    fn name(&self) -> &str;

    /// Infers the output type from the argument types.
    fn return_type(&self, argument_types: &[DataType]) -> Result<DataType>;

    /// Evaluates a logical batch and returns a reference-backed or computed result.
    fn invoke(
        &self,
        arguments: &[FunctionInput<'_>],
        row_count: usize,
        return_type: &DataType,
    ) -> Result<UdfOutput>;
}

/// Registry of scalar functions available to expression execution.
#[derive(Default)]
pub struct FunctionRegistry {
    scalars: HashMap<String, Arc<dyn ScalarFunction>>,
}

impl FunctionRegistry {
    /// Creates an empty function registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a custom scalar implementation behind the shared runtime interface.
    pub fn register<F>(&mut self, udf: F)
    where
        F: ScalarFunction + 'static,
    {
        self.scalars
            .insert(udf.name().to_lowercase(), Arc::new(udf));
    }

    /// Retrieves a registered scalar function by name.
    pub fn get_scalar(&self, name: &str) -> Option<Arc<dyn ScalarFunction>> {
        self.scalars.get(&name.to_lowercase()).cloned()
    }

    /// Executes one registered function against a logical batch.
    pub fn invoke(
        &self,
        name: &str,
        arguments: &[FunctionInput<'_>],
        row_count: usize,
    ) -> Result<UdfOutput> {
        let udf = self.get_scalar(name).ok_or_else(|| {
            VeinError::planning(format!("UDF '{name}' not found in function registry"), None)
        })?;
        if arguments.iter().any(|argument| argument.len() != row_count) {
            return Err(VeinError::internal(
                "UDF arguments do not match the logical batch row count",
            ));
        }
        let argument_types = arguments
            .iter()
            .map(|argument| argument.data_type().clone())
            .collect::<Vec<_>>();
        let return_type = udf.return_type(&argument_types)?;
        let output = udf.invoke(arguments, row_count, &return_type)?;
        output.validate(row_count, &return_type)?;
        Ok(output)
    }
}

#[cfg(test)]
#[path = "registry_tests.rs"]
mod tests;
