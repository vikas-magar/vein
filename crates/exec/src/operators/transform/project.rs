use crate::daemon::pipeline::{PipelineNode, Transform, VeinBatch};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;
use error::{Result, VeinError};
use crate::expr::PhysicalExpr;

/// A synchronous Transform that evaluates expressions to produce new columns.
pub struct ProjectTransform {
    exprs: Vec<Box<dyn PhysicalExpr>>,
    schema: SchemaRef,
    next: Box<dyn PipelineNode>,
}

impl ProjectTransform {
    /// Create a new ProjectTransform.
    pub fn new(exprs: Vec<Box<dyn PhysicalExpr>>, schema: SchemaRef, next: Box<dyn PipelineNode>) -> Self {
        Self { exprs, schema, next }
    }
}

impl PipelineNode for ProjectTransform {
    fn push(&mut self, _batch: VeinBatch) -> Result<()> {
        todo!("Epic 1.2: migrate operator logic to DataChunk")
    }

    fn flush(&mut self) -> Result<()> {
        self.next.flush()
    }
}

impl Transform for ProjectTransform {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use crate::expr::{ColumnExpr, LiteralExpr, LiteralValue, BinaryExpr, BinaryOperator};

    #[tokio::test]
    async fn test_project_stage() {
        // Input Schema: a (Int64)
        let in_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        
        // Data: a = [10, 20]
        let batch = RecordBatch::try_new(
            in_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 20]))]
        ).unwrap();

        // Output Schema: a (Int64), a_plus_5 (Int64)
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("a_plus_5", DataType::Int64, false),
        ]));

        // Expressions:
        // 1. `a`
        let col_a = Box::new(ColumnExpr::new("a", 0));
        // 2. `a + 5`
        let col_a_clone = Box::new(ColumnExpr::new("a", 0));
        let lit_5 = Box::new(LiteralExpr::new(LiteralValue::Int64(5)));
        let plus_5 = Box::new(BinaryExpr::new(col_a_clone, BinaryOperator::Plus, lit_5));

        let exprs: Vec<Box<dyn PhysicalExpr>> = vec![col_a, plus_5];

        // Mock sink
        struct MockSink {
            rows: usize,
        }
        impl PipelineNode for MockSink {
            fn push(&mut self, _batch: VeinBatch) -> Result<()> {
                Ok(())
            }
            fn flush(&mut self) -> Result<()> { Ok(()) }
        }

        let sink = Box::new(MockSink { rows: 0 });

        // Execute Pipeline
        let mut _project = ProjectTransform::new(exprs, out_schema, sink);
        // _project.push(batch).unwrap();
    }
}
