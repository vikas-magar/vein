use crate::daemon::pipeline::{DataChunk, PipelineNode, Source, ValueVector, VeinBatch};
use arrow::record_batch::RecordBatch;
use common::Result;
use common::arrow::SelectionVector;

/// An in-memory data source for testing.
pub struct Memory {
    batch: Option<RecordBatch>,
}

impl Memory {
    /// Create a memory source from a single RecordBatch.
    pub fn new(batch: RecordBatch) -> Self {
        Self { batch: Some(batch) }
    }

    /// Create a dummy Memory source for quick testing.
    pub fn dummy() -> Self {
        let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("dummy", arrow::datatypes::DataType::Int64, false),
        ]));
        let array = std::sync::Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3, 4, 5]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        Self::new(batch)
    }
}

#[async_trait::async_trait(?Send)]
impl Source for Memory {
    async fn execute(&mut self, next: &mut dyn PipelineNode) -> Result<()> {
        if let Some(batch) = self.batch.take() {
            let num_rows = batch.num_rows();
            let vectors = batch
                .columns()
                .iter()
                .map(|col| ValueVector { data: col.clone() })
                .collect();
            let chunk = DataChunk {
                vectors,
                selection: SelectionVector::new_dense(num_rows),
            };
            next.push(VeinBatch::Flat(chunk))?;
        }
        next.flush()?;
        Ok(())
    }
}
