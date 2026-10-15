use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::DataType;
use error::{Result, VeinError};

use super::super::ScalarUDFImpl;

#[derive(Debug)]
pub(super) struct LowerFunction;

impl ScalarUDFImpl for LowerFunction {
    fn name(&self) -> &str {
        "lower"
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types != [DataType::Utf8] {
            return Err(VeinError::planning(
                "LOWER expects one UTF-8 argument",
                None,
            ));
        }
        Ok(DataType::Utf8)
    }

    fn invoke(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        let [input] = args else {
            return Err(VeinError::internal("LOWER expects one argument"));
        };
        let input = input
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| VeinError::internal("LOWER expects a UTF-8 array"))?;
        let output: StringArray = input
            .iter()
            .map(|value| value.map(str::to_lowercase))
            .collect();
        Ok(Arc::new(output))
    }
}
