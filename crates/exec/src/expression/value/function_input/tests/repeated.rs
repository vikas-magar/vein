use super::*;
use crate::pipeline::{DataChunkView, ViewColumnRef, ViewInputRows};

#[test]
fn year_accepts_scalar_runs_inside_column_inputs_and_keeps_nulls() {
    let source = test_support::dense_view(vec![
        Arc::new(Date32Array::from(vec![Some(0), Some(19_723), None])) as ArrayRef,
    ]);
    let rows = [Some(0), Some(0), Some(1), Some(1), Some(2), Some(2), None];
    let view = DataChunkView::combine(&[ViewInputRows { view: &source, rows: &rows }],
                                      &[ViewColumnRef { input: 0, column: 0 }]).unwrap();
    let value = ExpressionValue::view_column(Arc::new(view), 0).unwrap();
    let adapter = ExpressionFunctionInputSource::new(&value);
    let arguments = [FunctionInput::new(&adapter, 7)];
    let output = FunctionRegistry::with_builtins()
        .invoke("year", &arguments, 7, &DataType::Int64).unwrap();
    output.validate(7, &DataType::Int64, &arguments).unwrap();
    let value = into_expression_value(output, &[]).unwrap();
    let actual = (0..7).map(|row| {
        value.source_row(row, 7).unwrap().and_then(|(array, index, _)| {
            let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            array.is_valid(index).then(|| array.value(index))
        })
    }).collect::<Vec<_>>();
    assert_eq!(actual, vec![Some(1970), Some(1970), Some(2024), Some(2024), None, None, None]);
    let run = value.segment(0, 7, 7).unwrap();
    assert!(run.is_scalar());
    assert_eq!(run.len(), 2);
}
