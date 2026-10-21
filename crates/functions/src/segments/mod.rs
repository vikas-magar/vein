use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use error::{Result, VeinError};

use crate::{FunctionInput, FunctionInputSegment, FunctionOutput};

/// Applies a vector kernel to aligned source runs without gathering columns.
pub(crate) fn invoke_segments<F>(
    arguments: &[FunctionInput<'_>],
    row_count: usize,
    return_type: &DataType,
    mut evaluate: F,
) -> Result<FunctionOutput>
where
    F: FnMut(&[FunctionInputSegment], usize) -> Result<ArrayRef>,
{
    if row_count == 0 {
        return Ok(FunctionOutput::empty(return_type));
    }
    if arguments.is_empty() {
        return Ok(FunctionOutput::from_array(validate(evaluate(&[], row_count)?, row_count, return_type)?));
    }
    if arguments.iter().all(FunctionInput::is_scalar) {
        let segments = arguments.iter().map(|argument| argument.segment(0, 1)).collect::<Result<Vec<_>>>()?;
        return FunctionOutput::from_scalar(validate(evaluate(&segments, 1)?, 1, return_type)?);
    }

    let mut outputs = Vec::new();
    let mut offset = 0usize;
    while offset < row_count {
        let segments = arguments.iter()
            .map(|argument| argument.segment(offset, row_count - offset))
            .collect::<Result<Vec<_>>>()?;
        let length = segments.iter().filter(|segment| !segment.is_scalar())
            .map(FunctionInputSegment::len).min()
            .ok_or_else(|| VeinError::internal("function has no column input segment"))?;
        if length == 0 {
            return Err(VeinError::internal("function received an empty input segment"));
        }
        outputs.push(validate(evaluate(&segments, length)?, length, return_type)?);
        offset += length;
    }
    FunctionOutput::from_segments(outputs)
}

fn validate(array: ArrayRef, rows: usize, data_type: &DataType) -> Result<ArrayRef> {
    if array.len() != rows || array.data_type() != data_type {
        return Err(VeinError::internal(format!(
            "function segment returned {} values of type {:?}, expected {rows} values of type {data_type:?}",
            array.len(), array.data_type()
        )));
    }
    Ok(array)
}
