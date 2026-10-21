use super::*;

#[test]
fn inexact_row_group_bounds_are_not_used_for_planning() {
    let values = Int64Array::from(vec![Some(3)]);
    let exact = BooleanArray::from(vec![Some(false)]);
    assert_eq!(
        exact_scalar_at(Some(&values), Some(&exact), 0, &DataType::Int64),
        None
    );
}

#[test]
fn exact_footer_bounds_skip_row_groups_and_reader_returns_same_matching_rows() {
    let root = tempfile::tempdir().expect("temporary root");
    let path = root.path().join("ordered.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, false)]));
    let values = Int64Array::from(vec![1, 2, 10, 11, 20, 21]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(values)])
        .expect("valid input batch");
    let file = std::fs::File::create(&path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("create writer");
    for offset in (0..batch.num_rows()).step_by(2) {
        writer
            .write(&batch.slice(offset, 2))
            .expect("write row group");
        writer.flush().expect("finish row group");
    }
    writer.close().expect("finish parquet file");

    let file = std::fs::File::open(&path).expect("open parquet file");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("read footer");
    let predicate = [
        StatisticsPredicate::Comparison {
            column: "value".into(),
            comparison: StatisticsComparison::GreaterOrEqual,
            literal: LogicalLiteral::Int64(10),
        },
        StatisticsPredicate::Comparison {
            column: "value".into(),
            comparison: StatisticsComparison::Less,
            literal: LogicalLiteral::Int64(20),
        },
    ];
    let pruning = prune_row_groups(builder.schema(), builder.metadata(), &predicate);

    assert_eq!(pruning.selected, vec![1]);
    assert_eq!(pruning.skipped_rows, 4);
    assert!(pruning.estimated_compressed_bytes_skipped > 0);

    let reader = builder
        .with_row_groups(pruning.selected)
        .build()
        .expect("build reader over selected groups");
    let selected_rows = reader
        .flat_map(|batch| {
            let batch = batch.expect("read selected row group");
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 output");
            (0..values.len()).map(|row| values.value(row)).collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(selected_rows, vec![10, 11]);
}

#[test]
fn page_indexes_select_only_possible_rows_and_missing_indexes_fail_open() {
    let root = tempfile::tempdir().expect("temporary root");
    let path = root.path().join("page_index.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, false)]));
    let values = Int64Array::from_iter_values(0..1024);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(values)])
        .expect("valid input batch");
    let file = std::fs::File::create(&path).expect("create Parquet file");
    let properties = WriterProperties::builder()
        .set_write_batch_size(64)
        .set_data_page_row_count_limit(64)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(properties))
        .expect("create indexed writer");
    writer.write(&batch).expect("write test rows");
    writer.close().expect("finish indexed file");

    let file = std::fs::File::open(&path).expect("open Parquet file");
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .parse_and_finish(&file)
        .expect("read page indexes");
    assert!(metadata.page_index().is_some(), "writer should emit page indexes");

    let predicates = [
        StatisticsPredicate::Comparison {
            column: "value".into(),
            comparison: StatisticsComparison::GreaterOrEqual,
            literal: LogicalLiteral::Int64(900),
        },
        StatisticsPredicate::Comparison {
            column: "value".into(),
            comparison: StatisticsComparison::Less,
            literal: LogicalLiteral::Int64(1000),
        },
    ];
    let pruning = prune_data_pages(&schema, &metadata, &predicates, &[0]);
    assert!(pruning.indexed_columns > 0);
    assert!(pruning.page_bounds_complete > 0);
    assert!(pruning.page_index_complete_columns[&0].contains("value"));
    assert!(page_header_requests_with_audit(&predicates, &[0], true, &pruning, false).is_empty());
    assert!(
        pruning.page_bounds_complete + pruning.page_bounds_incomplete > 0,
        "available indexes must account for each page's bound quality"
    );
    assert!(pruning.row_ranges_considered > 1);
    assert!(pruning.row_ranges_skipped > 0);
    assert!(pruning.rows_skipped > 0);
    assert_eq!(pruning.row_groups_skipped, 0);
    let selection = pruning.selections[0]
        .selection()
        .cloned()
        .expect("at least some pages should be skipped");

    // Applying the generated selection before the row predicate must retain
    // every true row even when page boundaries do not align with constants.
    let file = std::fs::File::open(&path).expect("reopen Parquet file");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("build ordinary reader")
        .with_row_selection(selection)
        .build()
        .expect("apply page-derived row selection");
    let selected_rows = reader
        .flat_map(|batch| {
            let batch = batch.expect("read selected page ranges");
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 value output");
            (0..values.len())
                .map(|row| values.value(row))
                .filter(|value| (900..1000).contains(value))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(selected_rows, (900..1000).collect::<Vec<_>>());

    let file = std::fs::File::open(&path).expect("reopen Parquet file");
    let metadata_without_indexes = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Skip)
        .parse_and_finish(&file)
        .expect("read footer without page indexes");
    let fallback = prune_data_pages(&schema, &metadata_without_indexes, &predicates, &[0]);
    assert_eq!(fallback.rows_skipped, 0);
    assert_eq!(fallback.page_bounds_complete, 0);
    assert_eq!(fallback.page_bounds_incomplete, 0);
    assert_eq!(fallback.selections.len(), 1);
    assert!(fallback.selections[0].selection().is_none());
}
