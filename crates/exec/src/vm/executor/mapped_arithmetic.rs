use super::*;
use arrow::array::{Array, Float64Array};
use crate::pipeline::{ViewColumnRef, ViewInputRows};
use crate::vm::register::RegValue;

fn int_at(view: &DataChunkView, column: usize, row: usize) -> Option<i64> {
    let (array, index) = view.resolve(column, row).unwrap().unwrap();
    let values = array.as_any().downcast_ref::<Int64Array>().unwrap();
    (!values.is_null(index)).then(|| values.value(index))
}

fn float_at(view: &DataChunkView, column: usize, row: usize) -> Option<f64> {
    let (array, index) = view.resolve(column, row).unwrap().unwrap();
    let values = array.as_any().downcast_ref::<Float64Array>().unwrap();
    (!values.is_null(index)).then(|| values.value(index))
}

fn segmented_ints(value: &RegValue, row_count: usize) -> Vec<i64> {
    let RegValue::Segments(segments) = value else {
        panic!("expected segmented register, got {value:?}");
    };
    let mut values = Vec::with_capacity(row_count);
    let mut offset = 0;
    while offset < row_count {
        let (array, len) = segments.segment(offset, row_count - offset, row_count).unwrap();
        let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
        values.extend(array.values().iter().copied());
        offset += len;
    }
    values
}

#[test]
fn mapped_arithmetic_and_cast_keep_input_views_and_scalar_broadcast() {
    let left: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40]));
    let right: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4]));
    let input = DataChunkView::from_chunk(
        DataChunk::new(
            vec![Arc::clone(&left), Arc::clone(&right)],
            SelectionVector::new_sparse(vec![3, 1, 0]),
        ),
    )
        .unwrap();
    let program = MicroProgram {
        register_count: 11,
        instructions: vec![
            MicroOp::Add { dst: 2, src1: 0, src2: 1 },
            MicroOp::Sub { dst: 3, src1: 0, src2: 1 },
            MicroOp::Mul { dst: 4, src1: 0, src2: 1 },
            MicroOp::Div { dst: 5, src1: 0, src2: 1 },
            MicroOp::Mod { dst: 6, src1: 0, src2: 1 },
            MicroOp::Neg { dst: 7, src: 1 },
            MicroOp::Cast { dst: 8, src: 0, target: types::VeinDataType::Float64 },
            MicroOp::LoadConstant { dst: 9, value: LogicalLiteral::Int64(2) },
            MicroOp::Add { dst: 10, src1: 0, src2: 9 },
            MicroOp::ProjectRegisters { out_regs: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 10] },
        ],
    };

    let (mut vm, output) = run(program, input);
    assert_eq!(vm.stats.input_materialization.gathered_columns, 0);
    vm.flush().unwrap();
    let output = output.lock().unwrap();
    let view = &output[0];
    assert_eq!((0..3).map(|row| int_at(view, 2, row)).collect::<Vec<_>>(), vec![Some(44), Some(22), Some(11)]);
    assert_eq!((0..3).map(|row| int_at(view, 3, row)).collect::<Vec<_>>(), vec![Some(36), Some(18), Some(9)]);
    assert_eq!((0..3).map(|row| int_at(view, 4, row)).collect::<Vec<_>>(), vec![Some(160), Some(40), Some(10)]);
    assert_eq!((0..3).map(|row| int_at(view, 5, row)).collect::<Vec<_>>(), vec![Some(10), Some(10), Some(10)]);
    assert_eq!((0..3).map(|row| int_at(view, 6, row)).collect::<Vec<_>>(), vec![Some(0), Some(0), Some(0)]);
    assert_eq!((0..3).map(|row| int_at(view, 7, row)).collect::<Vec<_>>(), vec![Some(-4), Some(-2), Some(-1)]);
    assert_eq!((0..3).map(|row| float_at(view, 8, row)).collect::<Vec<_>>(), vec![Some(40.0), Some(20.0), Some(10.0)]);
    assert_eq!((0..3).map(|row| int_at(view, 9, row)).collect::<Vec<_>>(), vec![Some(42), Some(22), Some(12)]);
    assert!(Arc::ptr_eq(view.source_arrays_for_column(0).unwrap().first().unwrap(), &left));
    assert!(Arc::ptr_eq(view.source_arrays_for_column(1).unwrap().first().unwrap(), &right));
}

#[test]
fn mapped_arithmetic_and_cast_preserve_outer_join_nulls_without_gathering() {
    let left: ArrayRef = Arc::new(Int64Array::from(vec![5, 6]));
    let right: ArrayRef = Arc::new(Int64Array::from(vec![2, 4]));
    let source = DataChunkView::from_chunk(DataChunk::from_vectors(vec![
        Arc::clone(&left), Arc::clone(&right),
    ]))
        .unwrap();
    let rows = [Some(1), None, Some(0)];
    let input = DataChunkView::combine(
        &[ViewInputRows { view: &source, rows: &rows }],
        &[
            ViewColumnRef { input: 0, column: 0 },
            ViewColumnRef { input: 0, column: 1 },
        ],
    )
        .unwrap();
    let program = MicroProgram {
        register_count: 4,
        instructions: vec![
            MicroOp::Add { dst: 2, src1: 0, src2: 1 },
            MicroOp::Cast { dst: 3, src: 0, target: types::VeinDataType::Float64 },
            MicroOp::ProjectRegisters { out_regs: vec![0, 1, 2, 3] },
        ],
    };

    let (mut vm, output) = run(program, input);
    assert_eq!(vm.stats.input_materialization.gathered_columns, 0);
    vm.flush().unwrap();
    let output = output.lock().unwrap();
    assert_eq!((0..3).map(|row| int_at(&output[0], 2, row)).collect::<Vec<_>>(), vec![Some(10), None, Some(7)]);
    assert_eq!((0..3).map(|row| float_at(&output[0], 3, row)).collect::<Vec<_>>(), vec![Some(6.0), None, Some(5.0)]);
    assert!(Arc::ptr_eq(output[0].source_arrays_for_column(0).unwrap().first().unwrap(), &left));
    assert!(Arc::ptr_eq(output[0].source_arrays_for_column(1).unwrap().first().unwrap(), &right));
}

#[test]
fn chained_mapped_kernels_consume_computed_segments_without_concatenation() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40]));
    let input = DataChunkView::from_chunk(DataChunk::new(
        vec![Arc::clone(&source)],
        SelectionVector::new_sparse(vec![3, 1, 0]),
    ))
        .unwrap();
    let program = MicroProgram {
        register_count: 5,
        instructions: vec![
            MicroOp::LoadConstant { dst: 1, value: LogicalLiteral::Int64(2) },
            MicroOp::Add { dst: 2, src1: 0, src2: 1 },
            MicroOp::Mul { dst: 3, src1: 2, src2: 1 },
            MicroOp::ProjectRegisters { out_regs: vec![2, 3] },
        ],
    };

    let (mut vm, output) = run(program, input);
    let RegValue::Segments(add_segments) = vm.registers().get(2).unwrap() else {
        panic!("first mapped kernel should retain its output segments");
    };
    let RegValue::Segments(mul_segments) = vm.registers().get(3).unwrap() else {
        panic!("downstream mapped kernel should consume and retain segments");
    };
    assert_eq!(add_segments.segment_count(), 3);
    assert_eq!(mul_segments.segment_count(), 3);
    assert!(!add_segments.is_materialized());
    assert!(!mul_segments.is_materialized());
    assert_eq!(segmented_ints(vm.registers().get(2).unwrap(), 3), vec![42, 22, 12]);
    assert_eq!(segmented_ints(vm.registers().get(3).unwrap(), 3), vec![84, 44, 24]);
    assert!(!add_segments.is_materialized());
    assert!(!mul_segments.is_materialized());
    assert_eq!(vm.registers().get(0).unwrap().view_column_index(), Some(0));

    let retained_arrays = mul_segments.source_arrays();
    vm.flush().unwrap();
    let output = output.lock().unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(
        (0..3).map(|row| int_at(&output[0], 1, row)).collect::<Vec<_>>(),
        vec![Some(84), Some(44), Some(24)]
    );
    let projected_arrays = output[0].source_arrays_for_column(1).unwrap();
    assert_eq!(projected_arrays.len(), retained_arrays.len());
    for (projected, retained) in projected_arrays.iter().zip(retained_arrays.iter()) {
        assert!(Arc::ptr_eq(projected, retained));
    }
    let RegValue::Segments(segments) = vm.registers().get(3).unwrap() else {
        panic!("projection must preserve the computed register");
    };
    assert!(!segments.is_materialized());
}

#[test]
fn projected_scalar_remains_broadcast_without_materializing_input_rows() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
    let input = DataChunkView::from_chunk(DataChunk::from_vectors(vec![source])).unwrap();
    let program = MicroProgram {
        register_count: 2,
        instructions: vec![
            MicroOp::LoadConstant { dst: 1, value: LogicalLiteral::Int64(7) },
            MicroOp::ProjectRegisters { out_regs: vec![1] },
        ],
    };

    let (mut vm, output) = run(program, input);
    vm.flush().unwrap();
    let output = output.lock().unwrap();
    assert_eq!(output[0].num_rows(), 3);
    assert_eq!(
        (0..3).map(|row| int_at(&output[0], 0, row)).collect::<Vec<_>>(),
        vec![Some(7), Some(7), Some(7)]
    );
    assert_eq!(output[0].source_arrays_for_column(0).unwrap().len(), 1);
}
