//! Arrow kernels over source-backed expression value segments.

use arrow::array::{Array, ArrayRef, Datum, new_empty_array, new_null_array};
use common::Result;
use error::VeinError;
use functions::OutputSegment;

use crate::expression::value::{ExpressionDatum, ExpressionValue, SegmentedArray};

pub(crate) fn eval_mapped_unary_value(
    value: &ExpressionValue,
    row_count: usize,
    mut null_result: impl FnMut(usize) -> Result<ArrayRef>,
    mut kernel: impl FnMut(&dyn Array) -> Result<ArrayRef>,
) -> Result<ExpressionValue> {
    if !value.is_mapped() {
        let array = kernel(value.array()?.as_ref())?;
        return Ok(if value.is_scalar() { ExpressionValue::Scalar(array) }
                  else { ExpressionValue::Array(array) });
    }
    if row_count == 0 {
        let empty = new_empty_array(value.data_type());
        return Ok(ExpressionValue::Array(kernel(empty.as_ref())?));
    }

    let mut outputs = Vec::new();
    let mut offset = 0;
    while offset < row_count {
        let run = value.segment(offset, row_count - offset, row_count)?;
        if run.len() == 0 {
            return Err(VeinError::internal(
                "expression unary kernel produced an empty segment",
            ));
        }
        let output = if run.is_null_extended() {
            null_result(run.len())?
        } else {
            let input = run
                .datum()
                .ok_or_else(|| VeinError::internal("missing unary kernel input"))?;
            kernel(input.get().0)?
        };
        push_output(&mut outputs, output, run.len(), run.is_scalar())?;
        offset += run.len();
    }
    finish_outputs(outputs)
}

pub(crate) fn eval_mapped_binary_value(
    lhs: &ExpressionValue,
    rhs: &ExpressionValue,
    row_count: usize,
    mut kernel: impl FnMut(&dyn Datum, &dyn Datum) -> Result<ArrayRef>,
) -> Result<ExpressionValue> {
    if !lhs.is_mapped() && !rhs.is_mapped() {
        let left = lhs.dense_datum()?;
        let right = rhs.dense_datum()?;
        let array = kernel(&left, &right)?;
        return Ok(if lhs.is_scalar() && rhs.is_scalar() { ExpressionValue::Scalar(array) }
                  else { ExpressionValue::Array(array) });
    }
    if row_count == 0 {
        let left = new_empty_array(lhs.data_type());
        let right = new_empty_array(rhs.data_type());
        let left = ExpressionDatum::from_array(&left, false);
        let right = ExpressionDatum::from_array(&right, false);
        return Ok(ExpressionValue::Array(kernel(&left, &right)?));
    }

    let mut outputs = Vec::new();
    let mut offset = 0;
    let mut left = lhs.segment(offset, row_count, row_count)?;
    let mut right = rhs.segment(offset, row_count, row_count)?;
    while offset < row_count {
        let len = left.len().min(right.len());
        if len == 0 {
            return Err(VeinError::internal(
                "expression binary kernel produced an empty segment",
            ));
        }
        let left_run = left.take_prefix(len);
        let right_run = right.take_prefix(len);
        let left_null = left_run
            .is_null_extended()
            .then(|| new_null_array(lhs.data_type(), len));
        let right_null = right_run
            .is_null_extended()
            .then(|| new_null_array(rhs.data_type(), len));
        let left_input = left_null
            .as_ref()
            .map(|array| ExpressionDatum::from_array(array, false))
            .or_else(|| left_run.datum())
            .ok_or_else(|| VeinError::internal("missing left binary kernel input"))?;
        let right_input = right_null
            .as_ref()
            .map(|array| ExpressionDatum::from_array(array, false))
            .or_else(|| right_run.datum())
            .ok_or_else(|| VeinError::internal("missing right binary kernel input"))?;
        push_output(&mut outputs, kernel(&left_input, &right_input)?, len,
                    left_run.is_scalar() && right_run.is_scalar())?;
        offset += len;
        if offset < row_count {
            if left.len() == 0 {
                left = lhs.segment(offset, row_count - offset, row_count)?;
            }
            if right.len() == 0 {
                right = rhs.segment(offset, row_count - offset, row_count)?;
            }
        }
    }
    finish_outputs(outputs)
}

pub(crate) fn push_output(
    outputs: &mut Vec<OutputSegment>,
    output: ArrayRef,
    expected_len: usize,
    scalar: bool,
) -> Result<()> {
    if output.len() != if scalar { 1 } else { expected_len } {
        return Err(VeinError::internal(format!(
            "mapped expression kernel returned {} rows for a {expected_len}-row segment",
            output.len()
        )));
    }
    if outputs
        .first()
        .is_some_and(|first| first.data_type() != output.data_type())
    {
        return Err(VeinError::internal(
            "mapped expression kernel returned inconsistent segment types",
        ));
    }
    outputs.push(OutputSegment::new(output, expected_len, scalar)?);
    Ok(())
}

pub(crate) fn finish_outputs(mut outputs: Vec<OutputSegment>) -> Result<ExpressionValue> {
    match outputs.len() {
        0 => Err(VeinError::internal("mapped expression kernel produced no output")),
        1 if !outputs[0].is_scalar() => Ok(ExpressionValue::Array(
            outputs.pop().expect("one output was checked").into_array(),
        )),
        _ => Ok(ExpressionValue::Segments(SegmentedArray::from_runs(outputs)?)),
    }
}

#[cfg(test)]
#[path = "mapped_kernel/tests/mod.rs"]
mod tests;
