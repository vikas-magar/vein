use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::DataType;
use error::{Result, VeinError};
use types::VeinDataType;

use crate::test_support::DenseInput;
use crate::{
    FunctionCatalog, FunctionDefinition, FunctionInput, FunctionKind, FunctionOutput,
    FunctionRegistry, FunctionSignature, ScalarFunction, Volatility,
};

mod aggregate;

#[derive(Debug)]
struct Identity;

impl ScalarFunction for Identity {
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [DataType::Int64] => Ok(DataType::Int64),
            _ => Err(VeinError::planning("IDENTITY expects one Int64 argument", None)),
        }
    }

    fn invoke(
        &self,
        arguments: &[FunctionInput<'_>],
        _rows: usize,
        _return_type: &DataType,
    ) -> Result<FunctionOutput> {
        if arguments.len() != 1 {
            return Err(VeinError::internal("IDENTITY expects one argument"));
        }
        Ok(FunctionOutput::reuse_input(0))
    }
}

#[derive(Debug)]
struct InvalidLength;

impl ScalarFunction for InvalidLength {
    fn return_type(&self, _types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke(
        &self,
        _arguments: &[FunctionInput<'_>],
        _rows: usize,
        _return_type: &DataType,
    ) -> Result<FunctionOutput> {
        Ok(FunctionOutput::from_array(
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        ))
    }
}

fn signature() -> FunctionSignature {
    FunctionSignature {
        return_type: VeinDataType::Int64,
        volatility: Volatility::Immutable,
        arg_types: vec![VeinDataType::Int64],
        min_arg_count: 1,
        kind: FunctionKind::Scalar,
        bytecode_ref: None,
    }
}

fn registry_with_identity() -> FunctionRegistry {
    let mut registry = FunctionRegistry::new();
    registry
        .register(FunctionDefinition::scalar("identity", signature(), Identity))
        .expect("register identity");
    registry
}

#[test]
fn custom_function_lookup_is_case_insensitive_and_can_reuse_its_input() {
    let registry = registry_with_identity();
    assert_eq!(registry.resolve("IDENTITY").unwrap().kind, FunctionKind::Scalar);

    let inputs = [DenseInput::column(Arc::new(Int64Array::from(vec![4, 8])) as ArrayRef)];
    let arguments = [FunctionInput::new(&inputs[0], 2)];
    let output = registry
        .invoke("IDENTITY", &arguments, 2, &DataType::Int64)
        .expect("identity invocation");
    assert!(matches!(output, FunctionOutput::ReuseInput(0)));
}

#[test]
fn registry_rejects_duplicate_names_without_replacing_the_registered_function() {
    let mut registry = registry_with_identity();
    let error = registry
        .register(FunctionDefinition::scalar("IDENTITY", signature(), Identity))
        .expect_err("duplicate names are rejected case-insensitively");
    assert!(error.to_string().contains("already registered"));
    assert_eq!(registry.resolve("identity").unwrap().kind, FunctionKind::Scalar);
}

#[test]
fn registry_validates_plugin_output_shape() {
    let mut registry = FunctionRegistry::new();
    registry
        .register(FunctionDefinition::scalar(
            "invalid_length",
            signature(),
            InvalidLength,
        ))
        .expect("register test function");
    let inputs = [DenseInput::column(Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef)];
    let arguments = [FunctionInput::new(&inputs[0], 2)];

    let error = registry
        .invoke("invalid_length", &arguments, 2, &DataType::Int64)
        .expect_err("a wrong-sized plugin result must fail validation");
    assert!(error.to_string().contains("expected 2 values"));
}
