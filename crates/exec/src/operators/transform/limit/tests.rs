use super::*;
use arrow::array::Int64Array;
use arrow::datatypes::DataType;
use common::arrow::SelectionVector;
use crate::pipeline::{DataChunkView, RowMapping, SourceRow, ViewColumn};
use std::sync::{Arc, Mutex};

struct TestCollector(Arc<Mutex<Vec<u32>>>);
impl PipelineNode<DataChunkView> for TestCollector {
    fn push(&mut self, chunk: DataChunkView) -> Result<()> {
        let mut out = self.0.lock().unwrap();
        for i in 0..chunk.selection().size() {
            out.push(chunk.selection().get_index(i) as u32);
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

#[test]
fn test_limit_single_chunk() {
    let collected = Arc::new(Mutex::new(Vec::new()));
    let sink = Box::new(TestCollector(collected.clone()));
    let mut limit = LimitTransform::new(2, 3, sink);

    let array = Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50, 60, 70])) as arrow::array::ArrayRef;
    let chunk = DataChunkView::from_arrays(vec![array], 7, SelectionVector::new_dense(7))
        .expect("view from arrays");

    limit.push(chunk).unwrap();

    let out = collected.lock().unwrap().clone();
    // Offset 2, count 3: indices [2, 3, 4]
    assert_eq!(out, vec![2, 3, 4]);
}

#[test]
fn test_limit_spanning_multiple_chunks() {
    let collected = Arc::new(Mutex::new(Vec::new()));
    let sink = Box::new(TestCollector(collected.clone()));
    // Offset 3, Count 4 across chunks of size 3
    let mut limit = LimitTransform::new(3, 4, sink);

    // Chunk 1: indices [0, 1, 2] (all skipped because offset is 3)
    let c1 = DataChunkView::from_arrays(
        vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as _],
        3,
        SelectionVector::new_dense(3),
    )
    .expect("view from arrays");
    limit.push(c1).unwrap();
    assert!(collected.lock().unwrap().is_empty());

    // Chunk 2: indices [0, 1, 2] (all 3 taken)
    let c2 = DataChunkView::from_arrays(
        vec![Arc::new(Int64Array::from(vec![4, 5, 6])) as _],
        3,
        SelectionVector::new_dense(3),
    )
    .expect("view from arrays");
    limit.push(c2).unwrap();
    assert_eq!(collected.lock().unwrap().len(), 3);

    // Chunk 3: indices [0, 1, 2] (only 1 taken to reach count = 4)
    let c3 = DataChunkView::from_arrays(
        vec![Arc::new(Int64Array::from(vec![7, 8, 9])) as _],
        3,
        SelectionVector::new_dense(3),
    )
    .expect("view from arrays");
    limit.push(c3).unwrap();
    assert_eq!(collected.lock().unwrap().len(), 4);
}

struct ViewCollector(Arc<Mutex<Option<DataChunkView>>>, arrow::array::ArrayRef);

impl PipelineNode<DataChunkView> for ViewCollector {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        assert_eq!(view.num_rows(), 1);
        let (array, physical_row) = view.resolve(0, 0)?.expect("selected value");
        assert!(Arc::ptr_eq(array, &self.1));
        assert_eq!(physical_row, 1);
        *self.0.lock().unwrap() = Some(view);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

#[test]
fn limit_preserves_reference_backed_view_and_slices_only_selection() {
    let source = Arc::new(Int64Array::from(vec![10, 20, 30, 40])) as arrow::array::ArrayRef;
    let mapping = Arc::new(
        RowMapping::new(
            Arc::from(vec![4]),
            [
                Some(SourceRow::new(0, 0).unwrap()),
                Some(SourceRow::new(0, 1).unwrap()),
                Some(SourceRow::new(0, 2).unwrap()),
                Some(SourceRow::new(0, 3).unwrap()),
            ],
        )
            .expect("valid row mapping"),
    );
    let view = DataChunkView::new(
        4,
        vec![ViewColumn::new(vec![Arc::clone(&source)], DataType::Int64, mapping)
            .expect("valid reference-backed column")],
        SelectionVector::new_sparse(vec![3, 1, 2]),
    )
        .expect("valid reference-backed view");
    let output = Arc::new(Mutex::new(None::<DataChunkView>));
    let sink = Box::new(ViewCollector(Arc::clone(&output), Arc::clone(&source)));
    let mut limit = LimitTransform::new(1, 1, sink);

    limit
        .push(view)
        .expect("limit should forward selected view rows");

    let output = output.lock().unwrap();
    let Some(view) = output.as_ref() else {
        panic!("limit output should remain a view");
    };
    assert_eq!(view.num_rows(), 1);
    assert_eq!(view.num_columns(), 1);
}
