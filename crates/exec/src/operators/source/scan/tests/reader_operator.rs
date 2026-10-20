use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use common::Result;
use common::arrow::SelectionVector;

use crate::pipeline::{DataChunkView, PipelineNode, Source};
use crate::operators::source::scan::{ParquetScanReader, parquet::ParquetScanRequest};

use super::support::{Int64Collector, scan_over};

struct EmittingReader(Arc<AtomicUsize>);

#[async_trait::async_trait]
impl ParquetScanReader for EmittingReader {
    async fn execute(
        &self,
        _request: ParquetScanRequest<'_>,
        next: &mut dyn PipelineNode,
    ) -> Result<()> {
        self.0.fetch_add(1, Ordering::Relaxed);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let values = Arc::new(Int64Array::from(vec![41, 42]));
        let view = DataChunkView::from_arrays(
            vec![values],
            2,
            SelectionVector::new_dense(2),
        )?
        .with_schema(schema)?;
        next.push(view)
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
