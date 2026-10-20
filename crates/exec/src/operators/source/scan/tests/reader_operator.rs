use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use common::Result;

use crate::pipeline::{DataChunkView, PipelineNode, Source};
use crate::operators::source::scan::{ParquetScanReader, parquet::ParquetScanRequest};

use super::support::{Int64Collector, scan_over};

struct EmittingReader(Arc<AtomicUsize>);

#[async_trait::async_trait]
impl ParquetScanReader for EmittingReader {
    type Batch = DataChunkView;

    async fn execute(
        &self,
        _request: ParquetScanRequest<'_>,
        next: &mut dyn PipelineNode<Self::Batch>,
    ) -> Result<()> {
        self.0.fetch_add(1, Ordering::Relaxed);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![41, 42]))],
        )
        .expect("valid test batch");
        next.push(DataChunkView::from_record_batch(batch)?)
    }
}

#[tokio::test]
async fn scan_source_dispatches_to_injected_reader_and_accepts_view_batches() {
    let directory = tempfile::tempdir().expect("temporary scan directory");
    let calls = Arc::new(AtomicUsize::new(0));
    let mut source = scan_over(directory.path(), &[])
        .with_parquet_reader(Arc::new(EmittingReader(Arc::clone(&calls))));
    let mut sink = Int64Collector::default();

    source.execute(&mut sink).await.expect("injected reader should run");

    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(sink.0, vec![41, 42]);
}
