use std::sync::Arc;
use arrow::array::{Array, ArrayRef, Int64Array};
use super::*;
use crate::{FunctionInputRow, FunctionInputSource};

struct Runs(Vec<OutputSegment>);

impl FunctionInputSource for Runs {
    fn data_type(&self) -> &DataType { self.0[0].data_type() }
    fn is_scalar(&self) -> bool { false }
    fn segment(&self, start: usize, max: usize, _: usize) -> Result<FunctionInputSegment> {
        let mut offset = start;
        for run in &self.0 {
            if offset < run.len() {
                let len = max.min(run.len() - offset);
                let array = if run.is_scalar() { Arc::clone(run.array()) } else { run.array().slice(offset, len) };
                return FunctionInputSegment::new(Some(array), run.is_scalar(), len,
                                                 self.data_type().clone());
            }
            offset -= run.len();
        }
        Err(VeinError::internal("test source range is invalid"))
    }
    fn source_row(&self, row: usize, rows: usize) -> Result<Option<FunctionInputRow>> {
        self.segment(row, 1, rows)?.row(0)
    }
}

fn repeated(value: i64, rows: usize) -> OutputSegment {
    OutputSegment::new(Arc::new(Int64Array::from(vec![value])), rows, true).unwrap()
}

fn sum(segments: &[FunctionInputSegment], rows: usize) -> Result<ArrayRef> {
    assert!(segments.iter().all(|segment| segment.len() == rows));
    let values = (0..rows).map(|row| {
        segments.iter().map(|segment| {
            let value = segment.row(row).unwrap().unwrap();
            value.array().as_any().downcast_ref::<Int64Array>().unwrap().value(value.row_index())
        }).sum::<i64>()
    }).collect::<Vec<_>>();
    Ok(Arc::new(Int64Array::from(values)))
}

fn invoke(left: &Runs, right: &Runs) -> FunctionOutput {
    let arguments = [FunctionInput::new(left, 5), FunctionInput::new(right, 5)];
    let output = invoke_segments(&arguments, 5, &DataType::Int64, sum).unwrap();
    output.validate(5, &DataType::Int64, &arguments).unwrap();
    output
}

fn values(output: FunctionOutput) -> Vec<i64> {
    let FunctionOutput::Segments(runs) = output else { panic!("segmented output expected") };
    runs.iter().flat_map(|run| {
        let values = run.array().as_any().downcast_ref::<Int64Array>().unwrap();
        (0..run.len()).map(move |row| values.value(if run.is_scalar() { 0 } else { row }))
    }).collect()
}

#[test]
fn independently_repeated_arguments_align_at_each_logical_boundary() {
    let left = Runs(vec![repeated(7, 3), repeated(8, 2)]);
    let right = Runs(vec![repeated(10, 2), repeated(20, 3)]);
    let output = invoke(&left, &right);
    let FunctionOutput::Segments(runs) = &output else { panic!("runs expected") };
    assert_eq!(runs.iter().map(OutputSegment::len).collect::<Vec<_>>(), vec![2, 1, 2]);
    assert!(runs.iter().all(|run| run.is_scalar() && run.array().len() == 1));
    assert_eq!(values(output), vec![17, 17, 27, 28, 28]);
}

#[test]
fn short_scalar_run_limits_a_long_dense_argument() {
    let left = Runs(vec![repeated(10, 2), repeated(20, 3)]);
    let right = Runs(vec![OutputSegment::dense(Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])))]);
    assert_eq!(values(invoke(&left, &right)), vec![11, 12, 23, 24, 25]);
}

#[test]
fn rejects_malformed_dense_function_output_and_incomplete_scalar_run() {
    let input = Runs(vec![OutputSegment::dense(Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])))]);
    let arguments = [FunctionInput::new(&input, 5)];
    assert!(invoke_segments(&arguments, 5, &DataType::Int64,
                            |_, _| Ok(Arc::new(Int64Array::from(vec![1])))).is_err());
    let output = FunctionOutput::from_runs(vec![repeated(1, 3)]).unwrap();
    assert!(output.validate(5, &DataType::Int64, &arguments).is_err());
}

#[test]
fn empty_function_batch_never_invokes_the_kernel() {
    let output = invoke_segments(&[], 0, &DataType::Int64,
                                 |_, _| panic!("empty batches must not invoke kernels")).unwrap();
    output.validate(0, &DataType::Int64, &[]).unwrap();
}
