use super::*;
use arrow::array::{Array, BooleanArray};
use arrow::buffer::BooleanBuffer;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter, RowSelectionPolicy};
use parquet::arrow::push_decoder::ParquetPushDecoderBuilder;
use parquet::DecodeResult;

#[test]
fn late_filter_columns_decode_only_survivors_with_async_predicate_cache() {
    let bytes = fixture();
    let metadata = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).unwrap().metadata().clone();
    let descriptor = metadata.file_metadata().schema_descr();
    let first = ArrowPredicateFn::new(ProjectionMask::roots(descriptor, [0]), |batch| {
        let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        Ok(BooleanArray::from_iter(ids.iter().map(|id| id.map(|id| id % 8 == 1))))
    });
    let late = ArrowPredicateFn::new(ProjectionMask::roots(descriptor, [1]), |batch| {
        let labels = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        Ok(BooleanArray::from_iter((0..labels.len()).map(|row| Some(labels.is_valid(row)))))
    });
    let metrics = ArrowReaderMetrics::enabled();
    let mut decoder = ParquetPushDecoderBuilder::try_new_decoder(metadata).unwrap()
        .with_batch_size(64).with_metrics(metrics.clone())
        .with_row_selection_policy(RowSelectionPolicy::Selectors)
        .with_row_filter(RowFilter::new(vec![Box::new(first), Box::new(late)]))
        .build().unwrap();
    let mut ids = Vec::new();
    loop {
        match decoder.try_decode().unwrap() {
            DecodeResult::NeedsData(ranges) => {
                let data = ranges.iter().map(|range| bytes.slice(range.start as usize..range.end as usize)).collect();
                decoder.push_ranges(ranges, data).unwrap();
            }
            DecodeResult::Data(batch) => {
                let values = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                ids.extend(values.iter().map(Option::unwrap));
            }
            DecodeResult::Finished => break,
        }
    }
    let expected = (0_i64..4096).filter(|id| id % 13 != 0 && id % 8 == 1 && id % 7 != 0).collect::<Vec<_>>();
    assert_eq!(ids, expected);
    let columns = metrics.column_metrics();
    assert_eq!(columns[0].rows_read, 4096, "the first predicate reads the full input");
    assert!(columns[1].rows_read < 4096, "late cache population must not expand sparse matches to full batches");
    assert!(columns[1].rows_skipped > 0);
}

#[test]
fn auto_selection_avoids_full_decode_for_fragmented_sparse_matches() {
    let bytes = fixture();
    for mask_backed in [false, true] {
        let mask = BooleanBuffer::from_iter((0..4096).map(|row| row % 8 == 1));
        let selection = if mask_backed {
            RowSelection::from_boolean_buffer(mask)
        } else {
            let boolean = BooleanArray::new(mask, None);
            RowSelection::from_filters(&[boolean])
        };
        let metrics = ArrowReaderMetrics::enabled();
        let actual = read(bytes.clone(), metrics.clone(), Some(selection.clone()));
        let expected = read(bytes.clone(), ArrowReaderMetrics::disabled(), Some(selection));
        assert_eq!(actual, expected);
        assert_eq!(actual.iter().map(RecordBatch::num_rows).sum::<usize>(), 512);
        for column in metrics.column_metrics() {
            assert_eq!(column.rows_read, 512);
            assert!(column.rows_skipped > 3000);
        }
    }
}

#[test]
fn auto_selection_retains_dense_batch_decoding() {
    let selection = RowSelection::from_boolean_buffer(
        BooleanBuffer::from_iter((0..4096).map(|row| row % 4 != 0)));
    let metrics = ArrowReaderMetrics::enabled();
    let actual = read(fixture(), metrics.clone(), Some(selection));
    assert_eq!(actual.iter().map(RecordBatch::num_rows).sum::<usize>(), 3072);
    assert!(metrics.column_metrics().iter().all(|column| column.rows_read > 3072));
}
