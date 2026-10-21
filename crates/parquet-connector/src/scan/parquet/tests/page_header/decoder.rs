use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow::array::Int64Array;
use parquet::DecodeResult;
use parquet::arrow::push_decoder::ParquetPushDecoderBuilder;

type PageLocations =
HashMap<usize, HashMap<usize, Vec<parquet::file::page_index::offset_index::PageLocation>>>;

pub(super) fn verify_selected_page_reads(
    path: &Path,
    metadata: &parquet::file::metadata::ParquetMetaData,
    selection: parquet::arrow::arrow_reader::RowGroupSelection,
    page_locations_by_group: &PageLocations,
    selected_payload_bytes: u64,
    skipped_payload_bytes: u64,
    target: i64,
) -> parquet::arrow::arrow_reader::RowSelection {
    assert!(selection.page_locations(0).is_some());
    assert!(selection.page_locations(1).is_none());
    let row_selection = selection
        .selection()
        .cloned()
        .expect("page-header bounds should create a row selection");
    let locations = page_locations_by_group
        .get(&0)
        .and_then(|columns| columns.get(&0))
        .expect("header scan returns byte locations for the filtered column");
    let mut decoder =
        ParquetPushDecoderBuilder::try_new_decoder(Arc::new(metadata.clone()))
            .expect("create push decoder")
            .with_row_group_selections(vec![selection])
            .build()
            .expect("build push decoder with header-derived page locations");
    let file_bytes = bytes::Bytes::from(std::fs::read(path).expect("read test Parquet bytes"));
    let (value_start, value_len) = metadata.row_group(0).column(0).byte_range();
    let value_end = value_start + value_len;
    let (payload_start, payload_len) = metadata.row_group(0).column(1).byte_range();
    let payload_end = payload_start + payload_len;
    let mut requested = Vec::new();
    let mut matches = Vec::new();
    loop {
        match decoder.try_decode().expect("decode selected page ranges") {
            DecodeResult::NeedsData(ranges) => {
                requested.extend(ranges.iter().cloned());
                let data = ranges
                    .iter()
                    .map(|range| file_bytes.slice(range.start as usize..range.end as usize))
                    .collect();
                decoder
                    .push_ranges(ranges, data)
                    .expect("provide requested Parquet ranges");
            }
            DecodeResult::Data(batch) => {
                let values = batch.column(0).as_any().downcast_ref::<Int64Array>()
                    .expect("push-decoder output is Int64");
                let payload = batch.column(1).as_any().downcast_ref::<Int64Array>()
                    .expect("push-decoder payload is Int64");
                matches.extend((0..values.len()).filter_map(|row| {
                    (values.value(row) == target)
                        .then_some((values.value(row), payload.value(row)))
                }));
            }
            DecodeResult::Finished => break,
        }
    }
    let value_bytes = requested_bytes(&requested, value_start, value_end);
    let payload_bytes = requested_bytes(&requested, payload_start, payload_end);
    assert_eq!(matches, vec![(target, target * 2)]);
    assert!(!requested.is_empty());
    assert!(requested.iter().all(|range| {
        (range.start >= value_start && range.end <= value_end)
            || (range.start >= payload_start && range.end <= payload_end)
    }));
    let dictionary_prefix = u64::try_from(locations[0].offset)
        .expect("validated first data page offset")
        .saturating_sub(value_start);
    assert_eq!(value_bytes, dictionary_prefix.saturating_add(selected_payload_bytes));
    assert_eq!(payload_bytes, payload_len);
    let total_page_bytes = locations.iter()
        .map(|page| u64::try_from(page.compressed_page_size).unwrap_or(0))
        .sum::<u64>();
    assert!(selected_payload_bytes < total_page_bytes);
    assert_eq!(selected_payload_bytes + skipped_payload_bytes, total_page_bytes);
    row_selection
}

fn requested_bytes(
    ranges: &[std::ops::Range<u64>],
    chunk_start: u64,
    chunk_end: u64,
) -> u64 {
    ranges.iter().fold(0_u64, |total, range| {
        total.saturating_add(
            range.end.min(chunk_end).saturating_sub(range.start.max(chunk_start)),
        )
    })
}
