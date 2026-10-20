//! Shared zero-copy segment execution for scalar UDF implementations.

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use common::Result;
use error::VeinError;

use super::{FunctionInput, FunctionInputSegment, UdfOutput};

/// Runs a vector kernel over aligned source segments without gathering inputs.
pub(crate) fn invoke_segments<F>(
    arguments: &[FunctionInput<'_>],
    row_count: usize,
    return_type: &DataType,
    mut evaluate: F,
) -> Result<UdfOutput>
where
    F: FnMut(&[FunctionInputSegment], usize) -> Result<ArrayRef>,
{
    if row_count == 0 {
        return Ok(UdfOutput::empty(return_type));
    }
    if arguments.is_empty() {
        let output = validate(evaluate(&[], row_count)?, row_count, return_type)?;
        return Ok(UdfOutput::from_array(output));
    }
    if arguments.iter().all(FunctionInput::is_scalar) {
        let segments = arguments
            .iter()
            .map(|argument| argument.segment(0, 1))
            .collect::<Result<Vec<_>>>()?;
        let output = validate(evaluate(&segments, 1)?, 1, return_type)?;
        return UdfOutput::from_scalar(output);
    }

    let mut outputs = Vec::new();
    let mut offset = 0;
    while offset < row_count {
        let remaining = row_count - offset;
        let segments = arguments
            .iter()
            .map(|argument| argument.segment(offset, remaining))
            .collect::<Result<Vec<_>>>()?;
        let segment_rows = segments
            .iter()
            .filter(|segment| !segment.is_scalar())
            .map(FunctionInputSegment::len)
            .min()
            .ok_or_else(|| VeinError::internal("UDF segment has no column input"))?;
        if segment_rows == 0 {
            return Err(VeinError::internal("UDF produced an empty input segment"));
        }
        outputs.push(validate(evaluate(&segments, segment_rows)?, segment_rows, return_type)?);
        offset += segment_rows;
    }
    UdfOutput::from_segments(outputs)
}

fn validate(array: ArrayRef, rows: usize, data_type: &DataType) -> Result<ArrayRef> {
    if array.len() != rows || array.data_type() != data_type {
        return Err(VeinError::internal(format!(
            "UDF segment returned {} values of type {:?}, expected {rows} values of type {data_type:?}",
            array.len(),
            array.data_type()
        )));
    }
    Ok(array)
}
