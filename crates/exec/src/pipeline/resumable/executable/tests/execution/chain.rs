use super::{drain, fixture::pipeline, stage::Action::*};
use crate::pipeline::test_support::dense_view;
use arrow::array::{Array, ArrayRef, Int64Array};
use std::sync::Arc;

#[test]
fn a_deep_streaming_chain_retains_each_intermediate_view_through_suspension() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(3), None, Some(3)]));
    let pointer = array.to_data().buffers()[0].as_ptr();
    let mut scripts = vec![vec![Output(dense_view(vec![Arc::clone(&array)])), Finish]];
    for _ in 0..6 {
        scripts.push(vec![Need, RetryBatch, Block(1), Need, AcceptBatch, Work, Forward,
                          Need, AcceptEof, Work, Block(2), Finish]);
    }
    scripts.push(vec![Need, AcceptBatch, Need, AcceptEof, Finish]);
    let (mut executor, records) = pipeline(scripts);
    assert_eq!(drain(&mut executor, 1), 12);
    let sink = records.last().unwrap().lock().unwrap();
    let output = sink.received[0].materialize_column(0).unwrap();
    assert_eq!(output.to_data().buffers()[0].as_ptr(), pointer);
    assert_eq!(sink.batches, 1);
    drop(sink);
    for record in &records { assert_eq!(record.lock().unwrap().combined, 1); }
}

#[test]
fn sparse_repeated_row_mappings_survive_without_gathering_payloads() {
    use crate::pipeline::test_support::selected_view;
    use common::arrow::SelectionVector;
    let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(10), None, Some(30)]));
    let view = selected_view(vec![Arc::clone(&array)], SelectionVector::new_sparse(vec![2, 1, 0, 2]));
    let (mut executor, records) = pipeline(vec![
        vec![Output(view), Finish],
        vec![Need, RetryBatch, Block(1), Need, AcceptBatch, Forward, Need, AcceptEof, Finish],
        vec![Need, AcceptBatch, Need, AcceptEof, Finish],
    ]);
    drain(&mut executor, 1);
    let sink = records[2].lock().unwrap();
    let output = &sink.received[0];
    assert!(Arc::ptr_eq(output.source_arrays_for_column(0).unwrap().first().unwrap(), &array));
    let rows = (0..output.num_rows()).map(|row| {
        let (array, index) = output.resolve(0, row).unwrap().unwrap();
        let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
        if array.is_null(index) { None } else { Some(array.value(index)) }
    }).collect::<Vec<_>>();
    assert_eq!(rows, vec![Some(30), None, Some(10), Some(30)]);
}
