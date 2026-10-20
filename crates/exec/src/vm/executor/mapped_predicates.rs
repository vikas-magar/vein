use super::*;
use arrow::array::{Array, BooleanArray, StringArray};
use crate::pipeline::{ViewColumnRef, ViewInputRows};

#[test]
fn reordered_duplicate_view_rows_compare_without_gathering_source_values() {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40]));
    let labels: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d"]));
    let input = DataChunkView::from_chunk(DataChunk::new(
        vec![Arc::clone(&values), Arc::clone(&labels)],
        SelectionVector::new_sparse(vec![3, 1, 3]),
    )).unwrap();
    let program = MicroProgram {
        register_count: 4,
        instructions: vec![
            MicroOp::LoadConstant { dst: 2, value: LogicalLiteral::Int64(25) },
            MicroOp::CmpGt { dst: 3, src1: 0, src2: 2 },
            MicroOp::FilterMask { bool_reg: 3 },
            MicroOp::ProjectRegisters { out_regs: vec![0, 1] },
        ],
    };

    let (mut vm, output) = run(program, input);
    assert_eq!(vm.stats.input_materialization.gathered_columns, 0);
    vm.flush().unwrap();
    let output = output.lock().unwrap();
    assert_eq!(output[0].num_rows(), 2);
    for row in 0..2 {
        let (array, index) = output[0].resolve(0, row).unwrap().unwrap();
        assert_eq!(array.as_any().downcast_ref::<Int64Array>().unwrap().value(index), 40);
    }
    assert!(Arc::ptr_eq(output[0].source_arrays_for_column(0).unwrap().first().unwrap(), &values));
    assert!(Arc::ptr_eq(output[0].source_arrays_for_column(1).unwrap().first().unwrap(), &labels));
}

#[test]
fn null_extended_view_rows_produce_null_predicates_without_gathering() {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![40, 20]));
    let source = DataChunkView::from_chunk(DataChunk::from_vectors(vec![Arc::clone(&values)])).unwrap();
    let rows = [Some(0), None, Some(1)];
    let input = DataChunkView::combine(
        &[ViewInputRows { view: &source, rows: &rows }],
        &[ViewColumnRef { input: 0, column: 0 }],
    ).unwrap();
    let program = MicroProgram {
        register_count: 3,
        instructions: vec![
            MicroOp::LoadConstant { dst: 1, value: LogicalLiteral::Int64(25) },
            MicroOp::CmpGt { dst: 2, src1: 0, src2: 1 },
            MicroOp::ProjectRegisters { out_regs: vec![0, 2] },
        ],
    };

    let (mut vm, output) = run(program, input);
    assert_eq!(vm.stats.input_materialization.gathered_columns, 0);
    vm.flush().unwrap();
    let output = output.lock().unwrap();
    let result = output[0].resolve(1, 0).unwrap().unwrap();
    let booleans = result.0.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(booleans.value(result.1));
    let null = output[0].resolve(1, 1).unwrap().unwrap();
    assert!(null.0.is_null(null.1));
    let last = output[0].resolve(1, 2).unwrap().unwrap();
    assert!(!last.0.as_any().downcast_ref::<BooleanArray>().unwrap().value(last.1));
    assert!(Arc::ptr_eq(output[0].source_arrays_for_column(0).unwrap().first().unwrap(), &values));
}

#[test]
fn mismatched_view_run_boundaries_are_consumed_once_without_gathering() {
    let left_values: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40]));
    let right_values: ArrayRef = Arc::new(Int64Array::from(vec![15, 15, 25, 35]));
    let left = DataChunkView::from_chunk(DataChunk::from_vectors(vec![Arc::clone(&left_values)])).unwrap();
    let right = DataChunkView::from_chunk(DataChunk::from_vectors(vec![Arc::clone(&right_values)])).unwrap();
    let left_rows = [Some(0), Some(1), Some(2), Some(3)];
    let right_rows = [Some(3), Some(2), Some(1), Some(0)];
    let input = DataChunkView::combine(
        &[
            ViewInputRows { view: &left, rows: &left_rows },
            ViewInputRows { view: &right, rows: &right_rows },
        ],
        &[
            ViewColumnRef { input: 0, column: 0 },
            ViewColumnRef { input: 1, column: 0 },
        ],
    ).unwrap();
    let program = MicroProgram {
        register_count: 3,
        instructions: vec![
            MicroOp::CmpGt { dst: 2, src1: 0, src2: 1 },
            MicroOp::ProjectRegisters { out_regs: vec![2] },
        ],
    };

    let (mut vm, output) = run(program, input);
    assert_eq!(vm.stats.input_materialization.gathered_columns, 0);
    vm.flush().unwrap();
    let output = output.lock().unwrap();
    let actual = (0..4)
        .map(|row| {
            let (array, index) = output[0].resolve(0, row).unwrap().unwrap();
            array.as_any().downcast_ref::<BooleanArray>().unwrap().value(index)
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, vec![false, false, true, true]);
}

#[test]
fn mapped_like_uses_slices_and_retains_source_array() {
    let values: ArrayRef = Arc::new(StringArray::from(vec![
        "GOLD BRASS", "WOOD", "SILVER BRASS",
    ]));
    let input = DataChunkView::from_chunk(DataChunk::new(
        vec![Arc::clone(&values)],
        SelectionVector::new_sparse(vec![2, 1, 0]),
    )).unwrap();
    let program = MicroProgram {
        register_count: 3,
        instructions: vec![
            MicroOp::LoadConstant { dst: 1, value: LogicalLiteral::String("%BRASS".into()) },
            MicroOp::Like { dst: 2, src: 0, pattern: 1, negated: false },
            MicroOp::ProjectRegisters { out_regs: vec![0, 2] },
        ],
    };

    let (mut vm, output) = run(program, input);
    assert_eq!(vm.stats.input_materialization.gathered_columns, 0);
    vm.flush().unwrap();
    let output = output.lock().unwrap();
    assert!(Arc::ptr_eq(output[0].source_arrays_for_column(0).unwrap().first().unwrap(), &values));
    let actual = (0..3)
        .map(|row| {
            let (array, index) = output[0].resolve(1, row).unwrap().unwrap();
            array.as_any().downcast_ref::<BooleanArray>().unwrap().value(index)
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, vec![true, false, true]);
}

#[test]
fn null_tests_handle_outer_join_rows_without_materializing_inputs() {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(7), None]));
    let source = DataChunkView::from_chunk(DataChunk::from_vectors(vec![Arc::clone(&values)])).unwrap();
    let rows = [Some(0), None, Some(1)];
    let input = DataChunkView::combine(
        &[ViewInputRows { view: &source, rows: &rows }],
        &[ViewColumnRef { input: 0, column: 0 }],
    ).unwrap();
    let program = MicroProgram {
        register_count: 3,
        instructions: vec![
            MicroOp::IsNull { dst: 1, src: 0 },
            MicroOp::IsNotNull { dst: 2, src: 0 },
            MicroOp::ProjectRegisters { out_regs: vec![0, 1, 2] },
        ],
    };

    let (mut vm, output) = run(program, input);
    assert_eq!(vm.stats.input_materialization.gathered_columns, 0);
    vm.flush().unwrap();
    let output = output.lock().unwrap();
    for (column, expected) in [(1, vec![false, true, true]), (2, vec![true, false, false])] {
        let values = (0..3).map(|row| {
            let (array, index) = output[0].resolve(column, row).unwrap().unwrap();
            array.as_any().downcast_ref::<BooleanArray>().unwrap().value(index)
        }).collect::<Vec<_>>();
        assert_eq!(values, expected);
    }
    assert!(Arc::ptr_eq(output[0].source_arrays_for_column(0).unwrap().first().unwrap(), &values));
}

#[test]
fn mapped_not_preserves_outer_and_source_nulls_without_gathering() {
    let values: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), Some(false), None]));
    let source = DataChunkView::from_chunk(DataChunk::from_vectors(vec![Arc::clone(&values)])).unwrap();
    let rows = [Some(0), None, Some(1), Some(2)];
    let input = DataChunkView::combine(
        &[ViewInputRows { view: &source, rows: &rows }],
        &[ViewColumnRef { input: 0, column: 0 }],
    ).unwrap();
    let program = MicroProgram {
        register_count: 2,
        instructions: vec![
            MicroOp::Not { dst: 1, src: 0 },
            MicroOp::ProjectRegisters { out_regs: vec![0, 1] },
        ],
    };

    let (mut vm, output) = run(program, input);
    assert_eq!(vm.stats.input_materialization.gathered_columns, 0);
    vm.flush().unwrap();
    let output = output.lock().unwrap();
    let result = (0..4).map(|row| {
        let (array, index) = output[0].resolve(1, row).unwrap().unwrap();
        let values = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        (!values.is_null(index)).then(|| values.value(index))
    }).collect::<Vec<_>>();
    assert_eq!(result, vec![Some(false), None, Some(true), None]);
    assert!(Arc::ptr_eq(output[0].source_arrays_for_column(0).unwrap().first().unwrap(), &values));
}
