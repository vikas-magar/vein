use super::*;
use arrow::array::{ArrayRef, Int64Array, StringArray};
use common::arrow::SelectionVector;
use common::protocol::{MicroOp, MicroProgram};
use std::sync::{Arc, Mutex};
use types::LogicalLiteral;

#[path = "mapped_predicates.rs"]
mod mapped_predicates;
#[path = "mapped_boolean.rs"]
mod mapped_boolean;
#[path = "mapped_in_list.rs"]
mod mapped_in_list;
#[path = "mapped_arithmetic.rs"]
mod mapped_arithmetic;
#[path = "view_tests/mapped_case.rs"]
mod mapped_case;

#[derive(Clone)]
struct Capture(Arc<Mutex<Vec<DataChunkView>>>);

impl PipelineNode<DataChunkView> for Capture {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.0.lock().unwrap().push(view);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

fn run(program: MicroProgram, input: DataChunkView) -> (VectorVmExecutor<DataChunkView>, Arc<Mutex<Vec<DataChunkView>>>) {
    let output = Arc::new(Mutex::new(Vec::new()));
    let mut vm = VectorVmExecutor::new_view(
        Arc::new(program),
        Arc::new(crate::udf::FunctionRegistry::new()),
        Box::new(Capture(Arc::clone(&output))),
    );
    vm.push(input).unwrap();
    (vm, output)
}

#[test]
fn view_projection_reuses_source_array_without_register_materialization() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40]));
    let input = DataChunkView::from_chunk(DataChunk::new(
        vec![source.clone()],
        SelectionVector::new_sparse(vec![3, 1]),
    ))
        .unwrap();
    let program = MicroProgram {
        register_count: 1,
        instructions: vec![MicroOp::ProjectRegisters { out_regs: vec![0] }],
    };

    let (mut vm, output) = run(program, input);
    assert_eq!(vm.stats.input_materialization.gathered_columns, 0);
    vm.flush().unwrap();
    let output = output.lock().unwrap();
    assert_eq!(output[0].num_rows(), 2);
    assert!(Arc::ptr_eq(
        output[0].source_arrays_for_column(0).unwrap().first().unwrap(),
        &source
    ));
}

#[test]
fn copy_register_preserves_view_lineage_until_projection() {
    let source: arrow::array::ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let input = DataChunkView::from_chunk(DataChunk::from_vectors(vec![Arc::clone(&source)])).unwrap();
    let program = MicroProgram {
        register_count: 2,
        instructions: vec![
            MicroOp::CopyRegister { dst: 1, src: 0 },
            MicroOp::ProjectRegisters { out_regs: vec![1] },
        ],
    };

    let (mut vm, output) = run(program, input);
    assert_eq!(vm.stats.input_materialization.gathered_columns, 0);
    vm.flush().unwrap();
    assert!(Arc::ptr_eq(
        output.lock().unwrap()[0].source_arrays_for_column(0).unwrap().first().unwrap(),
        &source
    ));
}

#[test]
fn mapped_comparison_kernel_does_not_gather_source_columns() {
    let values: arrow::array::ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40]));
    let labels: arrow::array::ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d"]));
    let input = DataChunkView::from_chunk(DataChunk::new(
        vec![Arc::clone(&values), Arc::clone(&labels)],
        SelectionVector::new_sparse(vec![3, 1]),
    ))
        .unwrap();
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
    let RegValue::Segments(predicate) = vm.registers().get(3).unwrap() else {
        panic!("mapped comparison should retain its segmented predicate output");
    };
    assert!(!predicate.is_materialized());
    vm.flush().unwrap();
    let output = output.lock().unwrap();
    assert_eq!(output[0].num_rows(), 1);
    assert!(Arc::ptr_eq(output[0].source_arrays_for_column(0).unwrap().first().unwrap(), &values));
    assert!(Arc::ptr_eq(output[0].source_arrays_for_column(1).unwrap().first().unwrap(), &labels));
}
