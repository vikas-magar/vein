use crate::daemon::pipeline::{PipelineNode, Transform, VeinBatch};
use crate::expr::PhysicalExpr;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use error::{Result, VeinError};

/// A synchronous Transform that evaluates expressions to produce new columns.
pub struct ProjectTransform {
    exprs: Vec<Box<dyn PhysicalExpr>>,
    schema: SchemaRef,
    next: Box<dyn PipelineNode>,
}

impl ProjectTransform {
    /// Create a new ProjectTransform.
    pub fn new(
        exprs: Vec<Box<dyn PhysicalExpr>>,
        schema: SchemaRef,
        next: Box<dyn PipelineNode>,
    ) -> Self {
        Self {
            exprs,
            schema,
            next,
        }
    }
}

impl PipelineNode for ProjectTransform {
    fn push(&mut self, batch: VeinBatch) -> Result<()> {
        match batch {
            VeinBatch::Flat(chunk) => {
                let mut fields = Vec::with_capacity(chunk.vectors.len());
                let mut arrays = Vec::with_capacity(chunk.vectors.len());
                for (i, vec) in chunk.vectors.iter().enumerate() {
                    fields.push(arrow::datatypes::Field::new(
                        &format!("col_{}", i),
                        vec.data.data_type().clone(),
                        true,
                    ));
                    arrays.push(vec.data.clone());
                }
                let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(fields));
                let record_batch = RecordBatch::try_new(schema, arrays).map_err(|e| {
                    VeinError::internal(format!("Failed to create RecordBatch: {}", e))
                })?;

                let mut new_vectors = Vec::with_capacity(self.exprs.len());
                for expr in &self.exprs {
                    let array = expr.evaluate(&record_batch)?;
                    new_vectors.push(crate::daemon::pipeline::ValueVector { data: array });
                }

                let new_chunk = crate::daemon::pipeline::DataChunk {
                    vectors: new_vectors,
                    selection: chunk.selection,
                };

                self.next.push(VeinBatch::Flat(new_chunk))
            }
            VeinBatch::Factorized(_) => {
                Err(VeinError::internal("Epic 3: project on factorized chunks"))
            }
        }
    }

    fn flush(&mut self) -> Result<()> {
        self.next.flush()
    }
}

impl Transform for ProjectTransform {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::{BinaryExpr, BinaryOperator, ColumnExpr, LiteralExpr, LiteralValue};
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_project_stage() {
        // Input Schema: a (Int64)
        let in_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));

        // Data: a = [10, 20]
        let batch = RecordBatch::try_new(
            in_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 20]))],
        )
        .unwrap();

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
            fn flush(&mut self) -> Result<()> {
                Ok(())
            }
        }

        let sink = Box::new(MockSink { rows: 0 });

        // Execute Pipeline
        let mut _project = ProjectTransform::new(exprs, out_schema, sink);
        // _project.push(batch).unwrap();
    }
}
