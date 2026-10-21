use std::ops::Range;
use std::sync::{Arc, Mutex};

use arrow::array::{AsArray, Int32Array};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt};
use futures::TryStreamExt;
use parquet::arrow::arrow_reader::{
    ArrowReaderOptions, RowSelection, RowSelector,
};
use parquet::arrow::async_reader::{
    AsyncFileReader, ParquetRecordBatchStreamBuilder, RowGroupSelection,
};
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::WriterProperties;

#[derive(Clone)]
struct RecordingReader {
    bytes: Bytes,
    ranges: Arc<Mutex<Vec<Range<usize>>>>,
}

impl AsyncFileReader for RecordingReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let range = range.start as usize..range.end as usize;
        self.ranges.lock().unwrap().push(range.clone());
        futures::future::ready(Ok(self.bytes.slice(range))).boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        let metadata_reader = ParquetMetaDataReader::new().with_arrow_reader_options(options);
        let metadata = metadata_reader
            .parse_and_finish(&self.bytes)
            .map(Arc::new);
        futures::future::ready(metadata).boxed()
    }
}

#[tokio::test]
async fn reader_does_not_request_skipped_data_page_bytes() {
    let schema = Arc::new(Schema::new(vec![Field::new("value", arrow::datatypes::DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from_iter_values(0..12))],
    )
        .unwrap();
    let properties = WriterProperties::builder()
        .set_write_batch_size(2)
        .set_data_page_row_count_limit(2)
        .build();
    let mut file = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut file, schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let reader = RecordingReader {
        bytes: Bytes::from(file),
        ranges: Arc::default(),
    };
    let requested_ranges = reader.ranges.clone();
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
        .await
        .unwrap();
    let metadata = builder.metadata();
    let row_count = metadata.row_group_num_rows(0).unwrap();
    let locations = metadata
        .page_index_for_row_group(0)
        .page_locations(0)
        .expect("test writer should produce an offset index")
        .clone();
    assert!(locations.len() > 1, "test data must span multiple pages");

    let skipped = &locations[0];
    let skipped_start = usize::try_from(skipped.offset).unwrap();
    let skipped_end = skipped_start + usize::try_from(skipped.compressed_page_size).unwrap();
    let skipped_rows = usize::try_from(locations[1].first_row_index).unwrap();
    let selection = RowSelection::from(vec![
        RowSelector::skip(skipped_rows),
        RowSelector::select(row_count - skipped_rows),
    ]);
    let row_group = RowGroupSelection::new(0, Some(selection)).with_page_locations(0, locations);

    requested_ranges.lock().unwrap().clear();
    let stream = builder
        .with_row_group_selections(vec![row_group])
        .build()
        .unwrap();
    let batches = stream.try_collect::<Vec<_>>().await.unwrap();
    let values = batches
        .iter()
        .flat_map(|batch| batch.column(0).as_primitive::<arrow::datatypes::Int32Type>().values())
        .copied()
        .collect::<Vec<_>>();
    assert_eq!(values, (skipped_rows as i32..row_count as i32).collect::<Vec<_>>());

    let requests = requested_ranges.lock().unwrap();
    assert!(requests.iter().all(|range| {
        range.end <= skipped_start || range.start >= skipped_end
    }), "skipped page bytes were requested: {requests:?}");
}
