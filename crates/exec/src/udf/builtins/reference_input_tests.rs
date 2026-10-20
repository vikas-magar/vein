use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Date32Array, Int32Array};

use crate::expression::value::ExpressionValue;
use crate::pipeline::DataChunkView;
use crate::udf::{FunctionRegistry, FunctionInput, register_builtin_scalars};

#[test]
fn built_in_extract_consumes_reference_backed_date_input() {
    let source = Arc::new(Date32Array::from(vec![Some(0), Some(19_723), None])) as ArrayRef;
    let view = Arc::new(
        DataChunkView::from_dense_arrays(vec![Arc::clone(&source)])
            .expect("single date column view"),
    );
    let input = ExpressionValue::view_column(Arc::clone(&view), 0).expect("view-backed input");
    let mut functions = FunctionRegistry::new();
    register_builtin_scalars(&mut functions);

    let output = functions
        .invoke("year", &[FunctionInput::new(&input, view.num_rows())], view.num_rows())
        .expect("year UDF should read source slices")
        .into_expression_value();
    let years = output
        .array()
        .expect("dense output")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("year output type");

    assert_eq!(years.value(0), 1970);
    assert_eq!(years.value(1), 2024);
    assert!(years.is_null(2));
}
