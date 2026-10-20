use super::*;

use arrow::array::BooleanArray;

#[test]
fn case_vm_keeps_mapped_conditions_and_branches_without_gathering_inputs() {
    let condition_a: ArrayRef = Arc::new(BooleanArray::from(vec![
        Some(false), Some(true), None, Some(true), Some(false),
    ]));
    let condition_b: ArrayRef =
        Arc::new(BooleanArray::from(vec![false, true, true, false, true]));
    let first_values: ArrayRef =
        Arc::new(StringArray::from(vec!["A0", "A1", "A2", "A3", "A4"]));
    let second_values: ArrayRef =
        Arc::new(StringArray::from(vec!["B0", "B1", "B2", "B3", "B4"]));
    let input = DataChunkView::from_chunk(DataChunk::new(
        vec![
            condition_a,
            condition_b,
            Arc::clone(&first_values),
            Arc::clone(&second_values),
        ],
        SelectionVector::new_sparse(vec![4, 0, 2, 3, 1]),
    ))
        .unwrap();
    let program = MicroProgram {
        register_count: 6,
        instructions: vec![
            MicroOp::LoadConstant {
                dst: 4,
                value: LogicalLiteral::String("else".into()),
            },
            MicroOp::Case {
                dst: 5,
                when_regs: vec![0, 1],
                then_regs: vec![2, 3],
                else_reg: Some(4),
                return_type: types::VeinDataType::Utf8,
            },
            MicroOp::ProjectRegisters { out_regs: vec![5] },
        ],
    };

    let (mut vm, output) = run(program, input);
    assert_eq!(vm.stats.input_materialization.gathered_columns, 0);
    for register in 0..4 {
        assert!(vm.registers().get(register).unwrap().is_mapped());
    }
    let case = vm.registers().get(5).unwrap();
    assert!(case.is_mapped(), "CASE result should remain source-backed");
    let (else_array, else_row, _) = vm.registers().get(4).unwrap().source_row(0, 5).unwrap().unwrap();
    assert_eq!(else_row, 0);
    for (row, expected_array, expected_row) in [
        (0, &second_values, 4),
        (1, else_array, 0),
        (2, &second_values, 2),
        (3, &first_values, 3),
        (4, &first_values, 1),
    ] {
        let (source, source_row, _) = case.source_row(row, 5).unwrap().unwrap();
        assert!(Arc::ptr_eq(source, expected_array));
        assert_eq!(source_row, expected_row);
    }
    vm.flush().unwrap();

    let output = output.lock().unwrap();
    let values_array = output[0].materialize_column(0).unwrap();
    let values = values_array
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("expected Utf8 materialization, got {:?}", values_array.data_type()))
        .clone();
    assert_eq!(
        values,
        StringArray::from(vec!["B4", "else", "B2", "A3", "A1"])
    );
}
