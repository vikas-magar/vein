use std::ops::Range;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray, StringBuilder};
use arrow::datatypes::DataType;
use error::{Result, VeinError};

use super::super::ScalarUDFImpl;

#[derive(Debug)]
pub(super) struct SubstringFunction;

impl ScalarUDFImpl for SubstringFunction {
    fn name(&self) -> &str {
        "substring"
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let valid = matches!(
            arg_types,
            [DataType::Utf8, DataType::Int64] | [DataType::Utf8, DataType::Int64, DataType::Int64]
        );
        if !valid {
            return Err(VeinError::planning(
                "SUBSTRING expects (Utf8, Int64) or (Utf8, Int64, Int64)",
                None,
            ));
        }
        Ok(DataType::Utf8)
    }

    fn invoke(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        let (input, start, length) = match args {
            [input, start] => (input, start, None),
            [input, start, length] => (input, start, Some(length)),
            _ => {
                return Err(VeinError::internal(
                    "SUBSTRING expects two or three arguments",
                ));
            }
        };
        let input = input
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| VeinError::internal("SUBSTRING expects a UTF-8 array"))?;
        let start = start
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| VeinError::internal("SUBSTRING start must be an Int64 array"))?;
        let length = length
            .map(|array| {
                array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    VeinError::internal("SUBSTRING length must be an Int64 array")
                })
            })
            .transpose()?;

        if start.len() != input.len() || length.is_some_and(|array| array.len() != input.len()) {
            return Err(VeinError::internal(
                "SUBSTRING argument arrays have different lengths",
            ));
        }

        let mut output = StringBuilder::new();
        for row in 0..input.len() {
            if input.is_null(row)
                || start.is_null(row)
                || length.is_some_and(|array| array.is_null(row))
            {
                output.append_null();
                continue;
            }
            let length = length.map(|array| array.value(row));
            let value = input.value(row);
            let bytes = substring_byte_range(value, start.value(row), length);
            output.append_value(&value[bytes]);
        }
        Ok(Arc::new(output.finish()))
    }
}

fn substring_byte_range(value: &str, offset: i64, length: Option<i64>) -> Range<usize> {
    let character_count = i64::try_from(value.chars().count()).unwrap_or(i64::MAX);
    let mut length = length.unwrap_or(i64::MAX);
    if length == 0 {
        return 0..0;
    }

    let mut start = if offset > 0 {
        offset.saturating_sub(1)
    } else {
        offset
    };
    if offset == 0 {
        length = length.saturating_sub(1);
        if length <= 0 {
            return 0..0;
        }
    }

    let mut end;
    if length > 0 {
        end = start.saturating_add(length);
    } else {
        end = start;
        start = start.saturating_add(length);
    }
    if offset < 0 {
        start = start.saturating_add(character_count);
        end = end.saturating_add(character_count);
    }

    start = start.clamp(0, character_count);
    end = end.clamp(0, character_count);
    if start >= end {
        return 0..0;
    }
    character_byte_offset(value, start as usize)..character_byte_offset(value, end as usize)
}

fn character_byte_offset(value: &str, character: usize) -> usize {
    value
        .char_indices()
        .nth(character)
        .map_or(value.len(), |(byte_offset, _)| byte_offset)
}

#[cfg(test)]
#[path = "substring_tests.rs"]
mod tests;
