use super::*;
use arrow::array::{Array, BooleanArray};
use crate::pipeline::{ViewColumnRef, ViewInputRows};

fn bool_at(view: &DataChunkView, column: usize, row: usize) -> Option<bool> {
    let (array, index) = view.resolve(column, row).unwrap().unwrap();
    let values = array.as_any().downcast_ref::<BooleanArray>().unwrap();
    (!values.is_null(index)).then(|| values.value(index))
}

#[test]
fn mapped_in_and_not_in_keep_source_columns_and_sql_null_semantics() {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None, Some(4)]));
    let input = DataChunkView::from_chunk(DataChunk::new(
        vec![Arc::clone(&values)],
        SelectionVector::new_sparse(vec![3, 1, 0, 2]),
    ))
        .unwrap();
    let program = MicroProgram {
        register_count: 7,
        instructions: vec![
            MicroOp::LoadConstant { dst: 1, value: LogicalLiteral::Int64(2) },
            MicroOp::LoadConstant {
                dst: 2,
                value: LogicalLiteral::Null(types::VeinDataType::Int64),
            },
            MicroOp::InList { dst: 3, value: 0, candidates: vec![1, 1, 2], negated: false },
            MicroOp::InList { dst: 4, value: 0, candidates: vec![1, 1, 2], negated: true },
            MicroOp::InList { dst: 5, value: 0, candidates: vec![], negated: false },
            MicroOp::InList { dst: 6, value: 0, candidates: vec![], negated: true },
            MicroOp::ProjectRegisters { out_regs: vec![0, 3, 4, 5, 6] },
        ],
    };

    let (mut vm, output) = run(program, input);
    assert_eq!(vm.stats.input_materialization.gathered_columns, 0);
    for register in [3, 4] {
        let RegValue::Segments(result) = vm.registers().get(register).unwrap() else {
            panic!("mapped IN output should retain its Arrow segments");
        };
        assert!(!result.is_materialized());
    }
    vm.flush().unwrap();
    let output = output.lock().unwrap();
    assert_eq!(
        (0..4).map(|row| bool_at(&output[0], 1, row)).collect::<Vec<_>>(),
        vec![None, Some(true), None, None]
    );
    assert_eq!(
        (0..4).map(|row| bool_at(&output[0], 2, row)).collect::<Vec<_>>(),
        vec![None, Some(false), None, None]
    );
    assert_eq!(
        (0..4).map(|row| bool_at(&output[0], 3, row)).collect::<Vec<_>>(),
        vec![Some(false); 4]
    );
    assert_eq!(
        (0..4).map(|row| bool_at(&output[0], 4, row)).collect::<Vec<_>>(),
        vec![Some(true); 4]
    );
    assert!(Arc::ptr_eq(
        output[0].source_arrays_for_column(0).unwrap().first().unwrap(),
        &values
    ));
}

#[test]
fn mapped_in_handles_outer_join_null_extension_without_gathering() {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![2, 1]));
    let source = DataChunkView::from_chunk(DataChunk::from_vectors(vec![Arc::clone(&values)])).unwrap();
    let rows = [Some(0), None, Some(1)];
    let input = DataChunkView::combine(
        &[ViewInputRows { view: &source, rows: &rows }],
        &[ViewColumnRef { input: 0, column: 0 }],
    )
        .unwrap();
    let program = MicroProgram {
        register_count: 3,
        instructions: vec![
            MicroOp::LoadConstant { dst: 1, value: LogicalLiteral::Int64(2) },
            MicroOp::InList { dst: 2, value: 0, candidates: vec![1], negated: false },
            MicroOp::ProjectRegisters { out_regs: vec![0, 2] },
        ],
    };

    let (mut vm, output) = run(program, input);
    assert_eq!(vm.stats.input_materialization.gathered_columns, 0);
    vm.flush().unwrap();
    let output = output.lock().unwrap();
    assert_eq!(
        (0..3).map(|row| bool_at(&output[0], 1, row)).collect::<Vec<_>>(),
        vec![Some(true), None, Some(false)]
    );
    assert!(Arc::ptr_eq(
        output[0].source_arrays_for_column(0).unwrap().first().unwrap(),
        &values
    ));
}

#[test]
fn mapped_in_supports_view_backed_candidates_without_gathering() {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let candidates: ArrayRef = Arc::new(Int64Array::from(vec![1, 3, 0]));
    let input = DataChunkView::from_chunk(DataChunk::from_vectors(vec![
        Arc::clone(&values),
        Arc::clone(&candidates),
    ]))
        .unwrap();
    let program = MicroProgram {
        register_count: 3,
        instructions: vec![
            MicroOp::InList { dst: 2, value: 0, candidates: vec![1], negated: false },
            MicroOp::ProjectRegisters { out_regs: vec![0, 1, 2] },
        ],
    };

    let (mut vm, output) = run(program, input);
    assert_eq!(vm.stats.input_materialization.gathered_columns, 0);
    vm.flush().unwrap();
    let output = output.lock().unwrap();
    assert_eq!(
        (0..3).map(|row| bool_at(&output[0], 2, row)).collect::<Vec<_>>(),
        vec![Some(true), Some(false), Some(false)]
    );
    assert!(Arc::ptr_eq(
        output[0].source_arrays_for_column(0).unwrap().first().unwrap(),
        &values
    ));
    assert!(Arc::ptr_eq(
        output[0].source_arrays_for_column(1).unwrap().first().unwrap(),
        &candidates
    ));
}
