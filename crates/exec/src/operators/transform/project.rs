use crate::daemon::pipeline::{DataChunk, PipelineNode, Transform};
use crate::expr::PhysicalExpr;
use arrow::datatypes::SchemaRef;
use error::Result;

/// A synchronous Transform that evaluates expressions to produce new columns.
pub struct ProjectExecutor {
    exprs: Vec<Box<dyn PhysicalExpr>>,
    schema: SchemaRef,
    next: Box<dyn PipelineNode<DataChunk>>,
}

impl ProjectExecutor {
    /// Create a new ProjectExecutor.
    pub fn new(
        exprs: Vec<Box<dyn PhysicalExpr>>,
        schema: SchemaRef,
        next: Box<dyn PipelineNode<DataChunk>>,
    ) -> Self {
        Self {
            exprs,
            schema,
            next,
        }
    }

    /// Returns a reference to the output schema.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

impl PipelineNode<DataChunk> for ProjectExecutor {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        let mut new_vectors = Vec::with_capacity(self.exprs.len());
        for expr in &self.exprs {
            new_vectors.push(expr.evaluate(&chunk)?);
        }

        let new_chunk = DataChunk {
            vectors: new_vectors,
            selection: chunk.selection,
        };

        self.next.push(new_chunk)
    }

    fn flush(&mut self) -> Result<()> {
        self.next.flush()
    }
}

impl Transform<DataChunk> for ProjectExecutor {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::{BinaryExpr, BinaryOperator, ColumnExpr, LiteralExpr, LiteralValue};
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_project_stage() {
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
        let mut project = ProjectExecutor::new(exprs, out_schema, sink);
        let array = Arc::new(Int64Array::from(vec![10, 20])) as arrow::array::ArrayRef;
        let chunk = DataChunk::from_vectors(vec![array]);
        project.push(chunk).unwrap();
    }
}
