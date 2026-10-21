use std::ops::Range;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray, StringBuilder, new_null_array};
use arrow::datatypes::DataType;
use error::{Result, VeinError};

use crate::segments::invoke_segments;
use crate::{FunctionInput, FunctionInputSegment, FunctionOutput, ScalarFunction};

#[derive(Debug)]
pub(super) struct SubstringFunction;

impl ScalarFunction for SubstringFunction {
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

    fn invoke(
        &self,
        args: &[FunctionInput<'_>],
        rows: usize,
        return_type: &DataType,
    ) -> Result<FunctionOutput> {
        invoke_segments(args, rows, return_type, |segments, segment_rows| {
            substring_segment(segments, segment_rows, return_type)
        })
    }
}

fn substring_segment(
    args: &[FunctionInputSegment],
    rows: usize,
    return_type: &DataType,
) -> Result<ArrayRef> {
    let (input_segment, start_segment, length_segment) = match args {
        [input, start] => (input, start, None),
        [input, start, length] => (input, start, Some(length)),
        _ => {
            return Err(VeinError::internal(
                "SUBSTRING expects two or three arguments",
            ));
        }
    };
    let Some(input_array) = input_segment.array() else {
        return Ok(new_null_array(return_type, rows));
    };
    let Some(start_array) = start_segment.array() else {
        return Ok(new_null_array(return_type, rows));
    };
    let length_array = match length_segment {
        Some(segment) => match segment.array() {
            Some(array) => Some(array),
            None => return Ok(new_null_array(return_type, rows)),
        },
        None => None,
    };
    let input = input_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| VeinError::internal("SUBSTRING expects a UTF-8 array"))?;
    let start = start_array
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| VeinError::internal("SUBSTRING start must be an Int64 array"))?;
    let length = length_array
        .map(|array| {
            array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                VeinError::internal("SUBSTRING length must be an Int64 array")
            })
        })
        .transpose()?;

    if input.len() < required_len(input_segment.is_scalar(), rows)
        || start.len() < required_len(start_segment.is_scalar(), rows)
        || length.is_some_and(|array| {
        let segment = length_segment.expect("length segment exists");
        array.len() < required_len(segment.is_scalar(), rows)
    })
    {
        return Err(VeinError::internal(
            "SUBSTRING input segment is shorter than its logical rows",
        ));
    }

    let mut output = StringBuilder::new();
    for row in 0..rows {
        let input_row = segment_index(input_segment, row);
        let start_row = segment_index(start_segment, row);
        let length_row = length_segment.map(|segment| segment_index(segment, row));
        if input.is_null(input_row)
            || start.is_null(start_row)
            || length
            .zip(length_row)
            .is_some_and(|(array, index)| array.is_null(index))
        {
            output.append_null();
            continue;
        }
        let length = length.zip(length_row).map(|(array, index)| array.value(index));
        let value = input.value(input_row);
        let bytes = substring_byte_range(value, start.value(start_row), length);
        output.append_value(&value[bytes]);
    }
    Ok(Arc::new(output.finish()))
}

fn required_len(scalar: bool, rows: usize) -> usize {
    if scalar { 1 } else { rows }
}

fn segment_index(segment: &FunctionInputSegment, row: usize) -> usize {
    if segment.is_scalar() { 0 } else { row }
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
mod tests;
