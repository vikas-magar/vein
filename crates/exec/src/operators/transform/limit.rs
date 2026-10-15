//! Limit transform operator: truncates and offsets in-flight rows.

use crate::pipeline::{DataChunk, PipelineBatch, PipelineNode, Transform};
use common::Result;

/// A synchronous [`Transform`] that limits the number of rows produced by a pipeline.
pub struct LimitTransform<B = DataChunk> {
    offset: u64,
    count: u64,
    rows_seen: u64,
    rows_emitted: u64,
    next: Box<dyn PipelineNode<B>>,
}

impl<B: PipelineBatch> LimitTransform<B> {
    /// Create a new [`LimitTransform`] with the given offset and count.
    pub fn new(offset: u64, count: u64, next: Box<dyn PipelineNode<B>>) -> Self {
        Self {
            offset,
            count,
            rows_seen: 0,
            rows_emitted: 0,
            next,
        }
    }
}

impl<B: PipelineBatch> PipelineNode<B> for LimitTransform<B> {
    fn push(&mut self, chunk: B) -> Result<()> {
        if self.rows_emitted >= self.count {
            // Maximum rows already reached; drop further pushes
            return Ok(());
        }

        let chunk_rows = chunk.num_rows();
        if chunk_rows == 0 {
            return Ok(());
        }

        // Check how many rows of this chunk fall before `offset`
        if self.rows_seen + (chunk_rows as u64) <= self.offset {
            // Whole chunk is before offset
            self.rows_seen += chunk_rows as u64;
            return Ok(());
        }

        let start_in_chunk = if self.rows_seen < self.offset {
            (self.offset - self.rows_seen) as usize
        } else {
            0
        };

        let remaining_to_emit = (self.count - self.rows_emitted) as usize;
        let available = chunk_rows - start_in_chunk;
        let take_count = available.min(remaining_to_emit);

        self.rows_seen += chunk_rows as u64;
        self.rows_emitted += take_count as u64;

        if take_count == 0 {
            return Ok(());
        }

        let limited = chunk.slice_rows(start_in_chunk, take_count)?;
        self.next.push(limited)
    }

    fn flush(&mut self) -> Result<()> {
        self.next.flush()
    }

    /// Once the window is full, nothing further down can matter.
    fn finished(&self) -> bool {
        self.rows_emitted >= self.count
    }
}

impl<B: PipelineBatch> Transform<B> for LimitTransform<B> {}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::DataType;
    use common::arrow::SelectionVector;
    use crate::pipeline::{DataChunkView, RowMapping, SourceRow, ViewColumn};
    use std::sync::{Arc, Mutex};

    struct TestCollector(Arc<Mutex<Vec<u32>>>);
    impl PipelineNode<DataChunk> for TestCollector {
        fn push(&mut self, chunk: DataChunk) -> Result<()> {
            let mut out = self.0.lock().unwrap();
            for i in 0..chunk.selection.size() {
                out.push(chunk.selection.get_index(i) as u32);
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
        let chunk = DataChunk::new(vec![array], SelectionVector::new_dense(7));

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
        let c1 = DataChunk::new(
            vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as _],
            SelectionVector::new_dense(3),
        );
        limit.push(c1).unwrap();
        assert!(collected.lock().unwrap().is_empty());

        // Chunk 2: indices [0, 1, 2] (all 3 taken)
        let c2 = DataChunk::new(
            vec![Arc::new(Int64Array::from(vec![4, 5, 6])) as _],
            SelectionVector::new_dense(3),
        );
        limit.push(c2).unwrap();
        assert_eq!(collected.lock().unwrap().len(), 3);

        // Chunk 3: indices [0, 1, 2] (only 1 taken to reach count = 4)
        let c3 = DataChunk::new(
            vec![Arc::new(Int64Array::from(vec![7, 8, 9])) as _],
            SelectionVector::new_dense(3),
        );
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
        let mut limit = LimitTransform::<DataChunkView>::new(1, 1, sink);

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
}
