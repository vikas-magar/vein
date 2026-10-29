use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use common::arrow::SelectionVector;
use common::protocol::{
    BinaryExprOp, ExprKind, ExpressionProgram, ExpressionStep, PhysicalExpr,
};
use types::{LogicalLiteral, VeinDataType};

use crate::expression::eval::ExpressionKernelRegistry;
use crate::expression::executor::ExpressionExecutor;
use crate::pipeline::{DataChunkView, PipelineNode};
use functions::FunctionRegistry;

#[derive(Clone)]
struct Capture(Arc<Mutex<Vec<DataChunkView>>>);

#[async_trait::async_trait]
impl PipelineNode for Capture {
    async fn push(&mut self, batch: DataChunkView) -> common::Result<()> {
        self.0.lock().unwrap().push(batch);
        Ok(())
    }

    async fn flush(&mut self) -> common::Result<()> {
        Ok(())
    }
}

fn column(index: u32) -> PhysicalExpr {
    PhysicalExpr { kind: ExprKind::Column { index }, return_type: VeinDataType::Int64 }
}

fn literal(value: i64) -> PhysicalExpr {
    PhysicalExpr {
        kind: ExprKind::Literal(LogicalLiteral::Int64(value)),
        return_type: VeinDataType::Int64,
    }
}

fn input() -> (DataChunkView, ArrayRef) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let source: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let batch = RecordBatch::try_new(schema, vec![Arc::clone(&source)]).unwrap();
    (crate::pipeline::arrow_adapter::from_record_batch(batch).unwrap(), source)
}

async fn run(program: ExpressionProgram, batch: DataChunkView) -> Vec<DataChunkView> {
    let output = Arc::new(Mutex::new(Vec::new()));
    let next = Capture(Arc::clone(&output));
    let mut executor = ExpressionExecutor::new(
        Arc::new(program),
        Arc::new(ExpressionKernelRegistry::with_builtins()),
        Arc::new(FunctionRegistry::new()),
        Box::new(next),
    );
    executor.push(batch).await.unwrap();
    std::mem::take(&mut *output.lock().unwrap())
}

#[tokio::test]
async fn direct_projection_keeps_source_arrow_buffer_and_computes_only_new_result() {
    let (batch, source) = input();
    let computed = PhysicalExpr {
        kind: ExprKind::Binary {
            op: BinaryExprOp::Add,
            left: Box::new(column(0)),
            right: Box::new(literal(10)),
        },
        return_type: VeinDataType::Int64,
    };
    let output = run(
        ExpressionProgram {
            steps: vec![ExpressionStep::Projection(vec![column(0), computed])],
        },
        batch,
    );
    let output = output.await;
    let [view] = output.as_slice() else { panic!("expected one projected view") };

    assert!(Arc::ptr_eq(
        &source,
        &view.source_arrays_for_column(0).unwrap()[0],
    ));
    let result = view.materialize_column(1).unwrap();
    assert_eq!(result.as_any().downcast_ref::<Int64Array>().unwrap().values(), &[11, 12, 13]);
}

#[tokio::test]
async fn filter_updates_only_selection_and_projection_preserves_source_mapping() {
    let (batch, source) = input();
    let predicate = PhysicalExpr {
        kind: ExprKind::Binary {
            op: BinaryExprOp::Greater,
            left: Box::new(column(0)),
            right: Box::new(literal(1)),
        },
        return_type: VeinDataType::Boolean,
    };
    let output = run(
        ExpressionProgram {
            steps: vec![
                ExpressionStep::Filter(predicate),
                ExpressionStep::Projection(vec![column(0)]),
            ],
        },
        batch,
    );
    let output = output.await;
    let [view] = output.as_slice() else { panic!("expected one filtered view") };

    assert_eq!(view.num_rows(), 2);
    assert!(Arc::ptr_eq(&source, &view.source_arrays_for_column(0).unwrap()[0]));
    assert_eq!(view.selection().contiguous_range(), Some(0..2));
}

#[tokio::test]
async fn all_true_filter_does_not_allocate_a_sparse_selection() {
    let (mut batch, _) = input();
    batch = batch.with_selection(SelectionVector::new_dense(3)).unwrap();
    let predicate = PhysicalExpr {
        kind: ExprKind::Binary {
            op: BinaryExprOp::Greater,
            left: Box::new(column(0)),
            right: Box::new(literal(0)),
        },
        return_type: VeinDataType::Boolean,
    };
    let output = run(
        ExpressionProgram { steps: vec![ExpressionStep::Filter(predicate)] },
        batch,
    );
    let output = output.await;
    let [view] = output.as_slice() else { panic!("expected one filtered view") };
    assert_eq!(view.selection().contiguous_range(), Some(0..3));
}
