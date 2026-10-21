use std::sync::Arc;

use arrow::array::{StringArray, new_null_array};
use arrow::datatypes::DataType;
use error::{Result, VeinError};

use crate::segments::invoke_segments;
use crate::{FunctionInput, FunctionOutput, ScalarFunction};

#[derive(Debug)]
pub(super) struct LowerFunction;

impl ScalarFunction for LowerFunction {
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types != [DataType::Utf8] {
            return Err(VeinError::planning(
                "LOWER expects one UTF-8 argument",
                None,
            ));
        }
        Ok(DataType::Utf8)
    }

    fn invoke(
        &self,
        args: &[FunctionInput<'_>],
        rows: usize,
        return_type: &DataType,
    ) -> Result<FunctionOutput> {
        invoke_segments(args, rows, return_type, |segments, segment_rows| {
            let [input] = segments else {
                return Err(VeinError::internal("LOWER expects one argument"));
            };
            let Some(array) = input.array() else {
                return Ok(new_null_array(return_type, segment_rows));
            };
            let input = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| VeinError::internal("LOWER expects a UTF-8 array"))?;
            let output: StringArray = input
                .iter()
                .map(|value| value.map(str::to_lowercase))
                .collect();
            Ok(Arc::new(output))
        })
    }
}
