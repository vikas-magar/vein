use super::*;

#[test]
fn unsigned_bounds_preserve_logical_types_nulls_and_extremes() {
    let cases: Vec<(arrow::array::ArrayRef, u64)> = vec![
        (
            Arc::new(UInt8Array::from(vec![Some(0), Some(u8::MAX), None])),
            u64::from(u8::MAX),
        ),
        (
            Arc::new(UInt16Array::from(vec![Some(0), Some(u16::MAX), None])),
            u64::from(u16::MAX),
        ),
        (
            Arc::new(UInt32Array::from(vec![Some(0), Some(u32::MAX), None])),
            u64::from(u32::MAX),
        ),
        (
            Arc::new(UInt64Array::from(vec![Some(0), Some(u64::MAX), None])),
            u64::MAX,
        ),
    ];
    let exact = BooleanArray::from(vec![true; 3]);

    for (values, maximum) in cases {
        let data_type = values.data_type();
        assert_eq!(
            exact_scalar_at(Some(values.as_ref()), Some(&exact), 0, data_type),
            Some(LogicalLiteral::UInt64(0)),
            "{data_type:?} minimum must retain unsigned semantics"
        );
        let maximum_bound = exact_scalar_at(Some(values.as_ref()), Some(&exact), 1, data_type);
        assert_eq!(maximum_bound, Some(LogicalLiteral::UInt64(maximum)));
        assert_eq!(
            exact_scalar_at(Some(values.as_ref()), Some(&exact), 2, data_type),
            None,
            "a null statistic is never a value bound"
        );
        let (bytes, physical_type) = if data_type == &DataType::UInt64 {
            (maximum.to_le_bytes().to_vec(), ParquetType::INT64)
        } else {
            ((maximum as u32).to_le_bytes().to_vec(), ParquetType::INT32)
        };
        assert_eq!(
            page_statistic_literal(&bytes, data_type, physical_type),
            maximum_bound,
            "footer arrays and page headers must use the same literal type"
        );
    }
}

#[test]
fn unsigned_footer_and_page_bounds_prune_across_signed_boundary() {
    let root = tempfile::tempdir().expect("temporary root");
    let path = root.path().join("unsigned_bounds.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::UInt64, false)]));
    let high = (i64::MAX as u64) + 1;
    let values = UInt64Array::from(vec![0, 1, high, u64::MAX]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(values)])
        .expect("unsigned input batch");
    let properties = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_write_batch_size(1)
        .set_data_page_row_count_limit(1)
        .build();
    let file = std::fs::File::create(&path).expect("create unsigned Parquet file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(properties))
        .expect("create unsigned writer");
    for offset in [0, 2] {
        writer.write(&batch.slice(offset, 2)).expect("write unsigned row group");
        writer.flush().expect("finish unsigned row group");
    }
    writer.close().expect("finish unsigned file");

    let file = std::fs::File::open(&path).expect("open unsigned Parquet file");
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .parse_and_finish(&file)
        .expect("read unsigned page indexes");
    let columns = row_group_column_statistics(&schema, &metadata, &HashSet::from(["value"]));
    assert_eq!(columns[0]["value"].min_value, Some(LogicalLiteral::UInt64(0)));
    assert_eq!(columns[0]["value"].max_value, Some(LogicalLiteral::UInt64(1)));
    assert_eq!(columns[1]["value"].min_value, Some(LogicalLiteral::UInt64(high)));
    assert_eq!(columns[1]["value"].max_value, Some(LogicalLiteral::UInt64(u64::MAX)));
    assert!(columns.iter().all(|columns| columns["value"].min_max_complete == Some(true)));
    let file_bytes = file.metadata().expect("unsigned file metadata").len();
    let summary = table_statistics(file_units(0, file_bytes, &schema, &metadata), &schema);
    assert_eq!(
        summary.column_statistics["value"].min_value,
        Some(LogicalLiteral::UInt64(0))
    );
    assert_eq!(
        summary.column_statistics["value"].max_value,
        Some(LogicalLiteral::UInt64(u64::MAX))
    );

    let predicate = [StatisticsPredicate::Comparison {
        column: "value".into(),
        comparison: StatisticsComparison::Equal,
        literal: LogicalLiteral::UInt64(u64::MAX),
    }];
    let footer_pruning = prune_row_groups(&schema, &metadata, &predicate);
    assert_eq!(footer_pruning.selected, vec![1]);
    assert_eq!(footer_pruning.skipped_rows, 2);
    let page_pruning = prune_data_pages(&schema, &metadata, &predicate, &[0, 1]);
    assert_eq!(page_pruning.rows_skipped, 3);
    assert_eq!(page_pruning.selections.len(), 1);
    assert_eq!(page_pruning.selections[0].row_group_index(), 1);
    let selection = page_pruning.selections[0]
        .selection()
        .cloned()
        .expect("the lower page in the unsigned row group must be skipped");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("build unsigned reader")
        .with_row_groups(vec![1])
        .with_row_selection(selection)
        .build()
        .expect("read matching unsigned page");
    let mut selected_values = Vec::new();
    for batch in reader {
        let batch = batch.expect("unsigned output batch");
        let values = batch.column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("unsigned output values");
        selected_values.extend(values.values().iter().copied());
    }
    assert_eq!(selected_values, vec![u64::MAX]);
}
