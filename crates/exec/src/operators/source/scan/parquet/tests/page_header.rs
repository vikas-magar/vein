use super::*;

#[tokio::test]
async fn page_header_statistics_prune_flat_data_pages_without_dropping_matches() {
    let root = tempfile::tempdir().expect("temporary Parquet root");
    let relative_path = "page_headers.parquet";
    let path = root.path().join(relative_path);
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, false),
        Field::new("payload", DataType::Int64, false),
    ]));
    let properties = WriterProperties::builder()
        .set_data_page_size_limit(256)
        .set_write_batch_size(32)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_write_page_header_statistics(true)
        .build();
    let file = std::fs::File::create(&path).expect("create Parquet file");
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(properties))
        .expect("create Parquet writer");
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from_iter_values(0_i64..8192)),
            Arc::new(Int64Array::from_iter_values((0_i64..8192).map(|value| value * 2))),
        ],
    )
        .expect("valid sorted values");
    writer.write(&batch).expect("write values");
    writer.close().expect("finish Parquet file");

    let file_size = std::fs::metadata(&path)
        .expect("read Parquet file size")
        .len();
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Skip)
        .parse_and_finish(&std::fs::File::open(&path).expect("open Parquet footer"))
        .expect("parse footer without page indexes");
    assert_eq!(metadata.num_row_groups(), 1);
    assert!(metadata.page_index().is_none());

    let root_path = root.path().to_string_lossy().into_owned();
    let operator = Operator::new(opendal::services::Fs::default().root(&root_path))
        .expect("create local filesystem operator");
    let bytes_read = Arc::new(AtomicU64::new(0));
    let page_headers = read_page_header_statistics(
        &operator,
        relative_path,
        file_size,
        Arc::clone(&bytes_read),
        Arc::new(IoStats::default()),
        schema.as_ref(),
        &metadata,
        &["value".to_string()],
        &[0],
    )
        .await;

    assert_eq!(page_headers.metrics.failed_chunks, 0);
    assert_eq!(page_headers.metrics.unsupported_row_mapping_chunks, 0);
    assert!(page_headers.metrics.pages_scanned > 1);
    assert_eq!(
        page_headers.metrics.pages_with_statistics,
        page_headers.metrics.pages_scanned,
        "the fixture explicitly writes statistics into every page header"
    );
    assert_eq!(
        page_headers.metrics.pages_with_complete_bounds,
        page_headers.metrics.pages_scanned,
        "the fixture uses exact Int64 min/max bounds"
    );
    assert!(page_headers.metrics.header_bytes > 0);
    assert!(page_headers.metrics.probe_bytes >= page_headers.metrics.header_bytes);
    assert!(bytes_read.load(Ordering::Relaxed) >= page_headers.metrics.probe_bytes);

    let pages = page_headers
        .pages
        .get(&0)
        .and_then(|columns| columns.get("value"))
        .expect("complete flat column page mapping");
    assert_eq!(pages.len(), page_headers.metrics.pages_scanned as usize);
    assert_eq!(pages.first().expect("first data page").first_row, 0);
    let page_locations = page_headers
        .page_locations
        .get(&0)
        .and_then(|columns| columns.get(&0))
        .expect("header scan returns byte locations for the flat column");
    assert_eq!(page_locations.len(), pages.len());
    assert_eq!(page_locations.first().expect("first data page").first_row_index, 0);
    assert_eq!(
        pages.iter().map(|page| page.row_count).sum::<usize>(),
        8192,
        "page row spans must cover the row group exactly"
    );

    let target = 4097_i64;
    let predicate = catalog::StatisticsPredicate::Comparison {
        column: "value".to_string(),
        comparison: catalog::StatisticsComparison::Equal,
        literal: types::LogicalLiteral::Int64(target),
    };
    let without_header_statistics =
        crate::parquet_statistics::prune_data_pages_with_headers(
            schema.as_ref(),
            &metadata,
            std::slice::from_ref(&predicate),
            &[0],
            &crate::parquet_statistics::PageHeaderStatistics::new(),
        );
    assert_eq!(without_header_statistics.rows_skipped, 0);
    assert!(without_header_statistics.selections[0].selection().is_none());

    let pruning = crate::parquet_statistics::prune_data_pages_with_headers(
        schema.as_ref(),
        &metadata,
        std::slice::from_ref(&predicate),
        &[0],
        &page_headers.pages,
    );
    assert!(pruning.row_ranges_skipped > 0);
    assert!(pruning.rows_skipped > 0);
    let row_group_selection = attach_header_page_locations(
        pruning.selections,
        &page_headers.page_locations,
    )
        .into_iter()
        .next()
        .expect("one row group survives the page-header pruning");
    let selection = row_group_selection
        .selection()
        .cloned()
        .expect("page-header bounds should create a row selection");
    let payload_metrics = page_payload_pruning_metrics(
        std::slice::from_ref(&row_group_selection),
        &page_headers.page_locations,
    );
    assert!(payload_metrics.data_pages_skipped > 0);
    assert!(payload_metrics.skipped_payload_bytes > 0);
    assert!(row_group_selection.page_locations(0).is_some());
    assert!(row_group_selection.page_locations(1).is_none());

    // Exercise Vein's actual push-decoder path with no OffsetIndex. The
    // custom locations must restrict storage requests to the dictionary
    // prefix and data pages selected by the page-header statistics.
    let mut decoder = ParquetPushDecoderBuilder::try_new_decoder(Arc::new(metadata.clone()))
        .expect("create push decoder")
        .with_row_group_selections(vec![row_group_selection])
        .build()
        .expect("build push decoder with header-derived page locations");
    let file_bytes = bytes::Bytes::from(std::fs::read(&path).expect("read test Parquet bytes"));
    let value_chunk = metadata.row_group(0).column(0);
    let (value_chunk_start, value_chunk_len) = value_chunk.byte_range();
    let value_chunk_end = value_chunk_start + value_chunk_len;
    let payload_chunk = metadata.row_group(0).column(1);
    let (payload_chunk_start, payload_chunk_len) = payload_chunk.byte_range();
    let payload_chunk_end = payload_chunk_start + payload_chunk_len;
    let mut requested_ranges = Vec::new();
    let mut push_matches = Vec::new();
    loop {
        match decoder.try_decode().expect("decode selected page ranges") {
            DecodeResult::NeedsData(ranges) => {
                requested_ranges.extend(ranges.iter().cloned());
                let data = ranges
                    .iter()
                    .map(|range| {
                        file_bytes.slice(range.start as usize..range.end as usize)
                    })
                    .collect();
                decoder
                    .push_ranges(ranges, data)
                    .expect("provide requested Parquet ranges");
            }
            DecodeResult::Data(batch) => {
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("push-decoder output is Int64");
                let payload = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("push-decoder payload is Int64");
                push_matches.extend((0..values.len()).filter_map(|row| {
                    (values.value(row) == target)
                        .then_some((values.value(row), payload.value(row)))
                }));
            }
            DecodeResult::Finished => break,
        }
    }
    let bytes_requested_in = |chunk_start: u64, chunk_end: u64| {
        requested_ranges.iter().fold(0_u64, |total, range| {
            let start = range.start.max(chunk_start);
            let end = range.end.min(chunk_end);
            total.saturating_add(end.saturating_sub(start))
        })
    };
    let value_bytes_requested = bytes_requested_in(value_chunk_start, value_chunk_end);
    let payload_bytes_requested = bytes_requested_in(payload_chunk_start, payload_chunk_end);
    assert_eq!(push_matches, vec![(target, target * 2)]);
    assert!(!requested_ranges.is_empty());
    assert!(requested_ranges.iter().all(|range| {
        (range.start >= value_chunk_start && range.end <= value_chunk_end)
            || (range.start >= payload_chunk_start && range.end <= payload_chunk_end)
    }));
    let dictionary_prefix_bytes = u64::try_from(page_locations[0].offset)
        .expect("validated first data page offset")
        .saturating_sub(value_chunk_start);
    assert_eq!(
        value_bytes_requested,
        dictionary_prefix_bytes.saturating_add(payload_metrics.selected_payload_bytes),
        "the filtered column should read its required dictionary and selected data pages only"
    );
    assert_eq!(
        payload_bytes_requested, payload_chunk_len,
        "a projected column without page locations must fall back to its complete chunk"
    );
    let total_page_payload_bytes = page_locations
        .iter()
        .map(|page| u64::try_from(page.compressed_page_size).unwrap_or(0))
        .sum::<u64>();
    assert!(payload_metrics.selected_payload_bytes < total_page_payload_bytes);
    assert_eq!(
        payload_metrics.selected_payload_bytes + payload_metrics.skipped_payload_bytes,
        total_page_payload_bytes,
        "selected and skipped data-page bytes must account for the complete page map"
    );

    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(&path).expect("reopen Parquet file"),
    )
        .expect("build selected reader")
        .with_row_selection(selection)
        .build()
        .expect("apply page-derived row selection");
    let matches = reader
        .map(|batch| {
            let batch = batch.expect("read selected rows");
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 output");
            (0..values.len())
                .map(|row| values.value(row))
                .filter(|value| *value == target)
                .collect::<Vec<_>>()
        })
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(matches, vec![target]);
}
