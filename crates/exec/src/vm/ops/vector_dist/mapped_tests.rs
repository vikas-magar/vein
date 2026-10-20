use super::*;
use arrow::array::{ArrayRef, Float32Array, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use common::arrow::SelectionVector;
use common::protocol::{MicroOp, MicroProgram};
use crate::pipeline::{DataChunk, DataChunkView, PipelineNode};
use crate::udf::FunctionRegistry;
use crate::vm::register::{RegValue, RegisterFile};
use crate::vm::VectorVmExecutor;
use common::Result;
use std::sync::Mutex;

fn list(rows: Vec<Vec<f32>>) -> ArrayRef {
    let mut values = Vec::new();
    let mut lengths = Vec::new();
    for row in rows {
        lengths.push(row.len());
        values.extend(row);
    }
    Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Float32, false)),
        OffsetBuffer::from_lengths(lengths),
        Arc::new(Float32Array::from(values)),
        None,
    ))
}

fn nullable_list(values: Vec<Option<f32>>) -> ArrayRef {
    Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Float32, true)),
        OffsetBuffer::from_lengths([values.len()]),
        Arc::new(Float32Array::from(values)),
        None,
    ))
}

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

#[test]
fn mapped_distance_reads_sparse_rows_without_materializing_input_columns() {
    let left = list(vec![vec![1.0, 0.0], vec![3.0, 4.0], vec![2.0, 2.0]]);
    let right = list(vec![vec![0.0, 1.0], vec![3.0, 4.0], vec![1.0, 1.0]]);
    let view = DataChunkView::from_chunk(DataChunk::from_vectors(vec![left, right]))
        .unwrap()
        .with_selection(SelectionVector::new_sparse(vec![1, 0]))
        .unwrap();
    let mut registers = RegisterFile::new(2);
    registers
        .mount_view_columns(2, Arc::new(view), [0, 1])
        .unwrap();

    let result = eval_mapped_vector_distance(
        registers.get(0).unwrap(),
        registers.get(1).unwrap(),
        2,
        VectorDistance::L2,
    )
        .unwrap();
    let values = result.as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(values.value(0), 0.0);
    assert!((values.value(1) - std::f32::consts::SQRT_2).abs() < 1e-5);
    assert!(matches!(registers.get(0).unwrap(), RegValue::ViewColumn(_)));
    assert!(matches!(registers.get(1).unwrap(), RegValue::ViewColumn(_)));
}

#[test]
fn mapped_distance_propagates_null_extended_rows_and_null_elements() {
    let left = list(vec![vec![1.0, 0.0], vec![1.0, 1.0]]);
    let right = list(vec![vec![0.0, 1.0], vec![1.0, 1.0]]);
    let source = DataChunkView::from_chunk(DataChunk::from_vectors(vec![left, right])).unwrap();
    let rows = [Some(0), None];
    let view = DataChunkView::combine(
        &[crate::pipeline::ViewInputRows { view: &source, rows: &rows }],
        &[
            crate::pipeline::ViewColumnRef { input: 0, column: 0 },
            crate::pipeline::ViewColumnRef { input: 0, column: 1 },
        ],
    )
        .unwrap();
    let mut registers = RegisterFile::new(2);
    registers
        .mount_view_columns(2, Arc::new(view), [0, 1])
        .unwrap();
    let result = eval_mapped_vector_distance(
        registers.get(0).unwrap(),
        registers.get(1).unwrap(),
        2,
        VectorDistance::Cosine,
    )
        .unwrap();
    let values = result.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(values.value(0), 1.0);
    assert!(values.is_null(1));

    let nullable = DataChunkView::from_chunk(DataChunk::from_vectors(vec![
        nullable_list(vec![Some(1.0), None]),
        nullable_list(vec![Some(0.0), Some(1.0)]),
    ]))
        .unwrap();
    let mut registers = RegisterFile::new(2);
    registers
        .mount_view_columns(1, Arc::new(nullable), [0, 1])
        .unwrap();
    let result = eval_mapped_vector_distance(
        registers.get(0).unwrap(),
        registers.get(1).unwrap(),
        1,
        VectorDistance::Cosine,
    )
        .unwrap();
    assert!(result.as_any().downcast_ref::<Float64Array>().unwrap().is_null(0));
}

#[test]
fn vector_distance_micro_op_keeps_view_inputs_mapped() {
    let left = list(vec![vec![1.0, 0.0], vec![3.0, 4.0]]);
    let right = list(vec![vec![0.0, 1.0], vec![3.0, 4.0]]);
    let input = DataChunkView::from_chunk(DataChunk::new(
        vec![Arc::clone(&left), Arc::clone(&right)],
        SelectionVector::new_sparse(vec![1, 0]),
    ))
        .unwrap();
    let program = MicroProgram {
        register_count: 3,
        instructions: vec![
            MicroOp::VectorL2Distance {
                dst: 2,
                src1: 0,
                src2: 1,
            },
            MicroOp::ProjectRegisters {
                out_regs: vec![0, 1, 2],
            },
        ],
    };
    let output = Arc::new(Mutex::new(Vec::new()));
    let mut vm = VectorVmExecutor::new_view(
        Arc::new(program),
        Arc::new(FunctionRegistry::new()),
        Box::new(Capture(Arc::clone(&output))),
    );

    vm.push(input).unwrap();

    assert!(matches!(vm.registers().get(0).unwrap(), RegValue::ViewColumn(_)));
    assert!(matches!(vm.registers().get(1).unwrap(), RegValue::ViewColumn(_)));
    let output = output.lock().unwrap();
    assert!(Arc::ptr_eq(
        output[0].source_arrays_for_column(0).unwrap().first().unwrap(),
        &left
    ));
    assert!(Arc::ptr_eq(
        output[0].source_arrays_for_column(1).unwrap().first().unwrap(),
        &right
    ));
    let distance = output[0].materialize_column(2).unwrap();
    let distance = distance.as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(distance.value(0), 0.0);
    assert!((distance.value(1) - std::f32::consts::SQRT_2).abs() < 1e-5);
}
