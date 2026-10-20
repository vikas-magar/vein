use super::*;
use arrow::array::{Array, BooleanArray};
use crate::pipeline::{ViewColumnRef, ViewInputRows};

#[test]
fn mapped_and_or_preserve_sql_null_rules_without_gathering() {
    let left_values: ArrayRef = Arc::new(BooleanArray::from(vec![Some(false), Some(true)]));
    let right_values: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), Some(false)]));
    let left = DataChunkView::from_chunk(DataChunk::from_vectors(vec![Arc::clone(&left_values)])).unwrap();
    let right = DataChunkView::from_chunk(DataChunk::from_vectors(vec![Arc::clone(&right_values)])).unwrap();
    let left_rows = [Some(0), None, Some(1)];
    let right_rows = [Some(0), Some(1), Some(0)];
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
        register_count: 4,
        instructions: vec![
            MicroOp::And { dst: 2, src1: 0, src2: 1 },
            MicroOp::Or { dst: 3, src1: 0, src2: 1 },
            MicroOp::ProjectRegisters { out_regs: vec![0, 1, 2, 3] },
        ],
    };

    let (mut vm, output) = run(program, input);
    assert_eq!(vm.stats.input_materialization.gathered_columns, 0);
    vm.flush().unwrap();
    let output = output.lock().unwrap();
    for (column, expected) in [(2, vec![Some(false), Some(false), Some(true)]), (3, vec![Some(true), None, Some(true)])] {
        let actual = (0..3).map(|row| {
            let (array, index) = output[0].resolve(column, row).unwrap().unwrap();
            let values = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            (!values.is_null(index)).then(|| values.value(index))
        }).collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }
    assert!(Arc::ptr_eq(output[0].source_arrays_for_column(0).unwrap().first().unwrap(), &left_values));
    assert!(Arc::ptr_eq(output[0].source_arrays_for_column(1).unwrap().first().unwrap(), &right_values));
}
