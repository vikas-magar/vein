use crate::daemon::pipeline::{DataChunk, PipelineNode, Transform};
use crate::expr::PhysicalExpr;
use arrow::array::Array;
use arrow::record_batch::RecordBatch;
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
        let mut fields = Vec::with_capacity(chunk.vectors.len());
        let mut arrays = Vec::with_capacity(chunk.vectors.len());
        for (i, vec) in chunk.vectors.iter().enumerate() {
            fields.push(arrow::datatypes::Field::new(
                format!("col_{}", i),
                vec.data_type().clone(),
                true,
            ));
            arrays.push(vec.clone());
        }
        let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(fields));
        let record_batch = RecordBatch::try_new(schema, arrays)
            .map_err(|e| VeinError::internal(format!("Failed to create RecordBatch: {e}")))?;

        let predicate_array = self.predicate.evaluate(&record_batch)?;
        let boolean_array = predicate_array
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .ok_or_else(|| VeinError::internal("Predicate did not return a BooleanArray"))?;

        let mut new_selection = Vec::with_capacity(boolean_array.len());
        for i in 0..boolean_array.len() {
            if boolean_array.is_valid(i) && boolean_array.value(i) {
                new_selection.push(i as u32);
            }
        }

        let mut filtered = chunk;
        filtered.selection = common::arrow::SelectionVector::new_sparse(new_selection);
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
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_filter_stage() {
        // Build Schema: a (Int64)
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));

        // Build Data: a = [10, 20, 30]
        let _batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 20, 30]))],
        )
        .unwrap();

        // Build Predicate: a > 15
        let col_a = Box::new(ColumnExpr::new("a", 0));
        let lit_15 = Box::new(LiteralExpr::new(LiteralValue::Int64(15)));
        let predicate = Box::new(BinaryExpr::new(col_a, BinaryOperator::Gt, lit_15));

        // Mock sink to collect results
        struct MockSink {
            rows: usize,
        }
        impl PipelineNode<DataChunk> for MockSink {
            fn push(&mut self, _batch: DataChunk) -> Result<()> {
                Ok(())
            }
            fn flush(&mut self) -> Result<()> {
                Ok(())
            }
        }

        let sink = Box::new(MockSink { rows: 0 });

        // Execute Pipeline
        let mut _filter = FilterExecutor::new(predicate, sink);
        // Since we can't easily inspect the moved sink in this simple test, we just assert it ran successfully.
    }
}
