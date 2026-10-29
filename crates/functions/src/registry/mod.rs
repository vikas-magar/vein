use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::DataType;
use error::{Result, VeinError};

use crate::{
    AggregateAccumulator, AggregateFunction, FunctionDefinition, FunctionImplementation,
    FunctionInput, FunctionOutput, FunctionSignature,
};

/// Semantic lookup interface consumed by the binder.
pub trait FunctionCatalog: Send + Sync {
    /// Resolves a function name without transferring runtime implementation ownership.
    fn resolve(&self, name: &str) -> Result<FunctionSignature>;
    /// Enumerates available signatures by reference for tooling; lookup-only catalogs may omit it.
    fn list(&self) -> Vec<(&str, &FunctionSignature)> { Vec::new() }
}

/// Shared case-insensitive function registry for binding and execution.
#[derive(Default, Clone)]
pub struct FunctionRegistry {
    definitions: HashMap<String, Arc<FunctionDefinition>>,
}

impl FunctionRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self { Self::default() }

    /// Creates the registry containing every function currently built into Vein.
    pub fn with_builtins() -> Self {
        let mut registry = Self::new();
        crate::builtins::register(&mut registry)
            .expect("built-in function definitions must have unique names");
        registry
    }

    /// Registers one semantic definition and optional runtime implementation.
    pub fn register(&mut self, definition: FunctionDefinition) -> Result<()> {
        let name = definition.name().trim();
        if name.is_empty() {
            return Err(VeinError::semantic("function name cannot be empty", None));
        }
        if self.definitions.contains_key(&name.to_ascii_lowercase()) {
            return Err(VeinError::semantic(format!("function '{name}' is already registered"), None));
        }
        self.definitions.insert(
            name.to_ascii_lowercase(),
            Arc::new(definition),
        );
        Ok(())
    }

    /// Returns the complete definition for planner or execution dispatch.
    pub fn definition(&self, name: &str) -> Result<Arc<FunctionDefinition>> {
        self.definitions.get(&name.to_ascii_lowercase()).cloned().ok_or_else(|| {
            VeinError::not_found(format!("Function '{name}' not found"))
        })
    }

    /// Creates aggregate state through the registered vectorized kernel.
    pub fn create_aggregate_accumulator(
        &self,
        name: &str,
        argument_types: &[types::VeinDataType],
        expected_type: &types::VeinDataType,
    ) -> Result<Box<dyn AggregateAccumulator>> {
        let definition = self.definition(name)?;
        let FunctionImplementation::Aggregate(function) = definition.implementation() else {
            return Err(VeinError::planning(
                format!("function '{name}' has no aggregate runtime kernel"), None,
            ));
        };
        let inferred_type = function.return_type(argument_types)?;
        if &inferred_type != expected_type {
            return Err(VeinError::internal(format!(
                "aggregate '{name}' inferred {inferred_type:?}, planned output is {expected_type:?}"
            )));
        }
        function.create_accumulator(argument_types, expected_type)
    }

    /// Returns the registered aggregate kernel without creating query state.
    pub fn aggregate_function(&self, name: &str) -> Result<Arc<dyn AggregateFunction>> {
        let definition = self.definition(name)?;
        match definition.implementation() {
            FunctionImplementation::Aggregate(function) => Ok(Arc::clone(function)),
            _ => Err(VeinError::planning(
                format!("function '{name}' has no aggregate runtime kernel"), None,
            )),
        }
    }

    /// Executes a scalar function and validates the planned output contract.
    pub fn invoke(
        &self,
        name: &str,
        arguments: &[FunctionInput<'_>],
        row_count: usize,
        expected_type: &DataType,
    ) -> Result<FunctionOutput> {
        let definition = self.definition(name)?;
        let FunctionImplementation::Scalar(function) = definition.implementation() else {
            return Err(VeinError::planning(
                format!("function '{name}' has no scalar runtime implementation"), None,
            ));
        };
        if arguments.iter().any(|argument| argument.len() != row_count) {
            return Err(VeinError::internal("function arguments do not match the logical batch row count"));
        }
        let types = arguments.iter().map(|argument| argument.data_type().clone()).collect::<Vec<_>>();
        let inferred_type = function.return_type(&types)?;
        if &inferred_type != expected_type {
            return Err(VeinError::internal(format!(
                "function '{name}' inferred {inferred_type:?}, planned output is {expected_type:?}"
            )));
        }
        let output = function.invoke(arguments, row_count, expected_type)?;
        output.validate(row_count, expected_type, arguments)?;
        Ok(output)
    }
}

impl FunctionCatalog for FunctionRegistry {
    fn list(&self) -> Vec<(&str, &FunctionSignature)> {
        let mut entries = self.definitions.values().map(|definition|
            (definition.name(), definition.signature())).collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(name, _)| *name);
        entries
    }

    fn resolve(&self, name: &str) -> Result<FunctionSignature> {
        Ok(self.definition(name)?.signature().clone())
    }
}

#[cfg(test)]
mod tests;
