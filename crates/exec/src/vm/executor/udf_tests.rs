use arrow::array::{Array, ArrayRef, Int64Array, Int64Builder};
use arrow::datatypes::DataType;
use common::arrow::SelectionVector;
use common::protocol::{MicroOp, MicroProgram};
use common::Result;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::pipeline::{
    DataChunk, DataChunkView, PipelineNode, RowMapping, SourceRow, ViewColumn,
};
use crate::udf::{FunctionRegistry, MappedUdfInput, ScalarUDFImpl};

use super::VectorVmExecutor;

#[path = "udf_tests/zero_copy_boundary.rs"]
mod zero_copy_boundary;

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

#[derive(Debug)]
struct IncrementMapped(Arc<AtomicUsize>);

impl ScalarUDFImpl for IncrementMapped {
    fn name(&self) -> &str {
        "increment_mapped"
    }

    fn return_type(&self, _arg_types: &[DataType]) -> error::Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke(&self, _args: &[ArrayRef]) -> error::Result<ArrayRef> {
        Err(error::VeinError::internal("dense UDF path was not expected"))
    }

    fn invoke_mapped(
        &self,
        args: &[MappedUdfInput<'_>],
        row_count: usize,
    ) -> error::Result<Option<ArrayRef>> {
        self.0.fetch_add(1, Ordering::Relaxed);
        let mut output = Int64Builder::with_capacity(row_count);
        for row in 0..row_count {
            match args[0].source_row(row)? {
                Some(source) => {
                    let values = source
                        .array()
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("mapped input type is Int64");
                    let index = source.row_index();
                    output.append_option((!values.is_null(index)).then(|| values.value(index) + 1));
                }
                None => output.append_null(),
            }
        }
        Ok(Some(Arc::new(output.finish())))
    }
}

#[derive(Debug)]
struct IncrementDense(Arc<AtomicUsize>);

impl ScalarUDFImpl for IncrementDense {
    fn name(&self) -> &str {
        "increment_dense"
    }

    fn return_type(&self, _arg_types: &[DataType]) -> error::Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke(&self, args: &[ArrayRef]) -> error::Result<ArrayRef> {
        self.0.fetch_add(1, Ordering::Relaxed);
        let values = args[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("dense input type is Int64");
        Ok(Arc::new(values.iter().map(|value| value.map(|value| value + 1)).collect::<Int64Array>()))
    }
}

fn program(name: &str) -> Arc<MicroProgram> {
    Arc::new(MicroProgram {
        register_count: 2,
        instructions: vec![
            MicroOp::CallUdf {
                name: name.to_owned(),
                arg_regs: vec![0],
                dst: 1,
            },
            MicroOp::ProjectRegisters { out_regs: vec![1] },
        ],
    })
}

fn execute(
    input: DataChunkView,
    udf: Arc<dyn ScalarUDFImpl>,
    name: &str,
) -> (VectorVmExecutor<DataChunkView>, Arc<Mutex<Vec<DataChunkView>>>) {
    let output = Arc::new(Mutex::new(Vec::new()));
    let mut registry = FunctionRegistry::new();
    registry.register_scalar(udf);
    let mut vm = VectorVmExecutor::new_view(
        program(name),
        Arc::new(registry),
        Box::new(Capture(Arc::clone(&output))),
    );
    vm.push(input).expect("UDF batch should execute");
    (vm, output)
}

fn output_values(output: &Mutex<Vec<DataChunkView>>) -> Vec<Option<i64>> {
    let views = output.lock().unwrap();
    let array = views[0]
        .materialize_column(0)
        .expect("UDF output should materialize");
    let values = array
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("UDF output type is Int64");
    values.iter().collect()
}

#[test]
fn mapped_udf_resolves_sparse_rows_without_gathering_input() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40]));
    let input = DataChunkView::from_chunk(DataChunk::new(
        vec![Arc::clone(&source)],
        SelectionVector::new_sparse(vec![3, 1]),
    ))
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let (mut vm, output) = execute(input, Arc::new(IncrementMapped(Arc::clone(&calls))), "increment_mapped");

    assert_eq!(vm.stats.input_materialization.gathered_columns, 0);
    assert_eq!(vm.stats.mapped_udf_batches, 1);
    assert_eq!(vm.stats.dense_udf_fallback_batches, 0);
    vm.flush().unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(output_values(&output), vec![Some(41), Some(21)]);
    assert_eq!(vm.stats.input_materialization.gathered_columns, 0);
}

#[test]
fn mapped_udf_observes_outer_join_null_extension() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![10, 20]));
    let lengths: Arc<[usize]> = Arc::from(vec![source.len()]);
    let mapping = Arc::new(
        RowMapping::new(
            lengths,
            [Some(SourceRow::new(0, 1).unwrap()), None],
        )
            .unwrap(),
    );
    let column = ViewColumn::new(vec![source], DataType::Int64, mapping).unwrap();
    let input = DataChunkView::new(2, vec![column], SelectionVector::new_dense(2)).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let (mut vm, output) = execute(input, Arc::new(IncrementMapped(calls)), "increment_mapped");

    vm.flush().unwrap();
    assert_eq!(output_values(&output), vec![Some(21), None]);
    assert_eq!(vm.stats.input_materialization.gathered_columns, 0);
}
