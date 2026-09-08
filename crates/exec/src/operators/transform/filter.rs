use crate::daemon::pipeline::{DataChunk, PipelineNode, Transform};
use crate::expr::PhysicalExpr;
use arrow::array::Array;
use error::{Result, VeinError};

/// A synchronous Transform that evaluates a boolean predicate.
pub struct FilterExecutor {
    predicate: Box<dyn PhysicalExpr>,
    next: Box<dyn PipelineNode<DataChunk>>,
}

impl FilterExecutor {
    /// Creates a new FilterExecutor.
    pub fn new(
        predicate: Box<dyn PhysicalExpr>,
        next: Box<dyn PipelineNode<DataChunk>>,
    ) -> Self {
        Self { predicate, next }
    }
}

impl PipelineNode<DataChunk> for FilterExecutor {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        let predicate_array = self.predicate.evaluate(&chunk)?;
        let boolean_array = predicate_array
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .ok_or_else(|| VeinError::internal("Predicate did not return a BooleanArray"))?;

        let mut filtered = chunk;
        filtered.selection = filtered.selection.filter(|row_idx| {
            boolean_array.is_valid(row_idx) && boolean_array.value(row_idx)
        });

        self.next.push(filtered)
    }

    fn flush(&mut self) -> Result<()> {
        self.next.flush()
    }
}

impl Transform<DataChunk> for FilterExecutor {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::{BinaryExpr, BinaryOperator, ColumnExpr, LiteralExpr, LiteralValue};
    use arrow::array::Int64Array;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_filter_stage() {
        // Build Predicate: a > 15
        let col_a = Box::new(ColumnExpr::new("a", 0));
        let lit_15 = Box::new(LiteralExpr::new(LiteralValue::Int64(15)));
        let predicate = Box::new(BinaryExpr::new(col_a, BinaryOperator::Gt, lit_15));

        // Mock sink to collect results
        struct MockSink;
        impl PipelineNode<DataChunk> for MockSink {
            fn push(&mut self, _batch: DataChunk) -> Result<()> {
                Ok(())
            }
            fn flush(&mut self) -> Result<()> {
                Ok(())
            }
        }

        let sink = Box::new(MockSink);

        // Execute Pipeline
        let mut filter = FilterExecutor::new(predicate, sink);
        let array = Arc::new(Int64Array::from(vec![10, 20, 30])) as arrow::array::ArrayRef;
        let chunk = DataChunk::from_vectors(vec![array]);
        filter.push(chunk).unwrap();
    }

    #[test]
    fn test_chained_filters_preserve_selection() {
        use std::sync::Mutex;

        let col_a = Box::new(ColumnExpr::new("a", 0));
        let lit_10 = Box::new(LiteralExpr::new(LiteralValue::Int64(10)));
        let pred_gt_10 = Box::new(BinaryExpr::new(col_a.clone(), BinaryOperator::Gt, lit_10));

        let lit_30 = Box::new(LiteralExpr::new(LiteralValue::Int64(30)));
        let pred_lt_30 = Box::new(BinaryExpr::new(col_a, BinaryOperator::Lt, lit_30));

        struct InspectSink(Arc<Mutex<Vec<u32>>>);
        impl PipelineNode<DataChunk> for InspectSink {
            fn push(&mut self, batch: DataChunk) -> Result<()> {
                let mut out = self.0.lock().unwrap();
                for i in 0..batch.selection.size() {
                    out.push(batch.selection.get_index(i) as u32);
                }
                Ok(())
            }
            fn flush(&mut self) -> Result<()> {
                Ok(())
            }
        }

        let captured = Arc::new(Mutex::new(Vec::new()));
        let sink = Box::new(InspectSink(captured.clone()));

        // Chain: Filter(a < 30) -> Sink
        let filter2 = Box::new(FilterExecutor::new(pred_lt_30, sink));
        // Chain: Filter(a > 10) -> Filter(a < 30) -> Sink
        let mut filter1 = FilterExecutor::new(pred_gt_10, filter2);

        // Input data: a = [5, 15, 25, 35, 45] (indices 0, 1, 2, 3, 4)
        let array = Arc::new(Int64Array::from(vec![5, 15, 25, 35, 45])) as arrow::array::ArrayRef;
        let chunk = DataChunk::new(vec![array], common::arrow::SelectionVector::new_dense(5));

        filter1.push(chunk).unwrap();

        let results = captured.lock().unwrap().clone();
        // Only 15 (idx 1) and 25 (idx 2) satisfy BOTH a > 10 and a < 30!
        assert_eq!(results, vec![1, 2]);
    }
}
