use super::*;

#[derive(Clone)]
pub(super) struct RecordingReader {
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
pub(super) async fn indexed_file() -> (
    ParquetRecordBatchStreamBuilder<RecordingReader>,
    Arc<Mutex<Vec<Range<usize>>>>,
    Vec<parquet::file::page_index::offset_index::PageLocation>,
    usize,
) {
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

    (builder, requested_ranges, locations, row_count)
}
