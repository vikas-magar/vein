use super::*;

#[test]
fn bloom_boolean_logic_is_conservative_for_and_or_and_unknown_terms() {
    let filters = HashMap::from([("value".to_string(), int64_bloom_filter("value", [7]))]);

    let mut probes = 0;
    assert!(!predicate_may_match_bloom(
        &StatisticsPredicate::And(vec![equality("value", 100), StatisticsPredicate::Unknown]),
        &filters,
        &mut probes,
    ));
    assert_eq!(probes, 1);

    let mut probes = 0;
    assert!(predicate_may_match_bloom(
        &StatisticsPredicate::Or(vec![equality("value", 100), StatisticsPredicate::Unknown]),
        &filters,
        &mut probes,
    ));
    assert_eq!(probes, 1);

    let mut probes = 0;
    assert!(predicate_may_match_bloom(
        &equality("missing_filter", 100),
        &filters,
        &mut probes,
    ));
    assert_eq!(probes, 0, "a missing filter must fail open");
}

#[test]
fn bloom_candidates_require_equality_predicates_and_report_distinct_columns() {
    let range_predicates = [StatisticsPredicate::And(vec![
        StatisticsPredicate::Comparison {
            column: "key".into(),
            comparison: StatisticsComparison::GreaterOrEqual,
            literal: LogicalLiteral::Int64(10),
        },
        StatisticsPredicate::Comparison {
            column: "key".into(),
            comparison: StatisticsComparison::LessOrEqual,
            literal: LogicalLiteral::Int64(20),
        },
    ])];
    assert!(equality_predicate_columns(&range_predicates).is_empty());

    let equality_predicates = [equality("z_key", 1), equality("a_key", 2), equality("z_key", 3)];
    assert_eq!(
        equality_predicate_columns(&equality_predicates),
        vec!["a_key", "z_key"]
    );
}

#[test]
fn parquet_bloom_filter_prunes_row_group_without_losing_matching_rows() {
    let root = tempfile::tempdir().expect("temporary root");
    let path = root.path().join("bloom_filter.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, false)]));
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(4096))
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
        .set_bloom_filter_enabled(true)
        .build();
    let file = std::fs::File::create(&path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(properties))
        .expect("create Bloom-enabled writer");

    let first_group = Int64Array::from_iter_values(0..4096);
    let first_batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(first_group)])
        .expect("valid first group");
    writer.write(&first_batch).expect("write first row group");
    writer.flush().expect("finish first row group");

    let second_group = Int64Array::from_iter_values(1_000_000..1_000_128);
    let second_batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(second_group)])
        .expect("valid second group");
    writer.write(&second_batch).expect("write second row group");
    writer.flush().expect("finish second row group");
    writer.close().expect("finish parquet file");

    let file = std::fs::File::open(&path).expect("open parquet file");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("read parquet footer");
    assert_eq!(builder.metadata().num_row_groups(), 2);
    let columns = bloom_filter_columns(
        builder.schema().as_ref(),
        builder.metadata(),
        &[equality("value", 1_000_000)],
    );
    assert_eq!(columns.len(), 1);
    let column = &columns[0];
    let first_filter = builder
        .get_row_group_column_bloom_filter(0, column.parquet_column_index)
        .expect("read first Bloom filter")
        .expect("first row group has a Bloom filter");
    let second_filter = builder
        .get_row_group_column_bloom_filter(1, column.parquet_column_index)
        .expect("read second Bloom filter")
        .expect("second row group has a Bloom filter");

    // Use an actual value from row group 1 that its filter confirms and
    // row group 0's filter rejects. This avoids relying on one guessed
    // non-member not colliding with a Bloom false positive.
    let matching_value = (1_000_000..1_000_128)
        .find(|value| second_filter.check(value) && !first_filter.check(value))
        .expect("the disjoint row groups should yield a Bloom-negative probe");
    let filters = HashMap::from([
        (
            0,
            HashMap::from([(
                column.name.clone(),
                ParquetBloomFilter::new(column.clone(), first_filter),
            )]),
        ),
        (
            1,
            HashMap::from([(
                column.name.clone(),
                ParquetBloomFilter::new(column.clone(), second_filter),
            )]),
        ),
    ]);
    let predicate = equality("value", matching_value);
    let pruning = prune_row_groups_with_bloom(
        builder.metadata(),
        std::slice::from_ref(&predicate),
        vec![
            RowGroupSelection::new(0, None),
            RowGroupSelection::new(1, None),
        ],
        &filters,
    );

    assert_eq!(pruning.selections.len(), 1);
    assert_eq!(pruning.selections[0].row_group_index(), 1);
    assert_eq!(pruning.row_groups_skipped, 1);
    assert_eq!(pruning.rows_skipped, 4096);
    assert!(pruning.probes >= 2);

    let reader = builder
        .with_row_groups(
            pruning
                .selections
                .iter()
                .map(RowGroupSelection::row_group_index)
                .collect(),
        )
        .build()
        .expect("build reader for Bloom-selected row groups");
    let values = reader
        .flat_map(|batch| {
            let batch = batch.expect("read selected row group");
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 output");
            (0..values.len()).map(|row| values.value(row)).collect::<Vec<_>>()
        })
        .filter(|value| *value == matching_value)
        .collect::<Vec<_>>();
    assert_eq!(values, vec![matching_value]);
}
