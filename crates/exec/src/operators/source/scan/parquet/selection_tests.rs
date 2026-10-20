use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use super::handoff_view;

#[test]
fn scan_handoff_reorders_output_without_changing_arrow_array_identity() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("predicate", DataType::Int64, false),
        Field::new("output", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            Arc::new(Int64Array::from(vec![100, 200, 300, 400])),
        ],
    )
        .expect("test batch");
    let view = handoff_view(batch.clone(), &[1, 0]).expect("projected view");

    assert_eq!(view.num_columns(), 2);
    assert_eq!(view.num_rows(), 4);
    let (source, physical_row) = view.resolve(0, 0).expect("resolve output row").unwrap();
    assert!(Arc::ptr_eq(source, batch.column(1)));
    assert_eq!(physical_row, 0);
}
