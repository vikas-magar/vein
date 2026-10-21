use super::*;

#[tokio::test]
async fn analyzes_external_parquet_tables_and_rejects_stale_sidecars() {
    let root = tempfile::tempdir().expect("temporary data root");
    let table_dir = root.path().join("external_events");
    std::fs::create_dir_all(&table_dir).expect("create external table directory");
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
    ]));
    let path = table_dir.join("part.parquet");
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(1),
                Some(2),
                Some(2),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some("A"),
                Some("A"),
                Some("B"),
                Some("A"),
                None,
            ])),
        ],
    )
    .expect("valid external batch");
    let file = File::create(&path).expect("create external Parquet file");
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)
        .expect("create external Parquet writer");
    writer.write(&initial).expect("write external batch");
    writer.close().expect("close external Parquet writer");

    let engine = LocalQueryEngine::new(root.path());
    let config = StatisticsConfig {
        scan_batch_size: 2,
        most_common_capacity: 4,
        histogram_sample_size: 8,
        histogram_bucket_count: 2,
        multi_column_sets: vec![vec!["label".to_owned(), "key".to_owned()]],
        ..StatisticsConfig::default()
    };
    let analyzed = engine
        .analyze_table_with_config("external_events", config.clone())
        .await
        .expect("analyze external Parquet table");
    assert_eq!(analyzed.statistics.num_rows, Some(5));
    assert_eq!(
        analyzed.statistics.column_groups[0].columns,
        vec!["key", "label"]
    );
    assert_eq!(analyzed.statistics.column_groups[0].distinct_count, Some(4));
    assert_eq!(
        analyzed.statistics.column_statistics["key"]
            .histogram
            .as_ref()
            .unwrap()
            .population_count,
        4
    );

    let catalog = parquet_catalog(root.path());
    let loaded = CatalogProvider::get_table(&catalog, "external_events")
        .await
        .expect("load analyzed external table");
    assert_eq!(
        loaded.statistics.as_ref().unwrap().column_groups[0].distinct_count,
        Some(4)
    );
    assert!(
        loaded.statistics.as_ref().unwrap().column_statistics["key"]
            .most_common_values
            .is_some()
    );

    std::fs::remove_file(&path).expect("replace source file");
    let replacement = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![
                Some(7),
                Some(7),
                Some(7),
                Some(8),
                Some(9),
                Some(10),
            ])),
            Arc::new(StringArray::from(vec![
                Some("X"),
                Some("X"),
                Some("X"),
                Some("Y"),
                Some("Z"),
                Some("W"),
            ])),
        ],
    )
    .expect("valid replacement batch");
    let file = File::create(&path).expect("recreate external Parquet file");
    let mut writer =
        ArrowWriter::try_new(file, schema, None).expect("create replacement Parquet writer");
    writer.write(&replacement).expect("write replacement batch");
    writer.close().expect("close replacement Parquet writer");

    let changed = CatalogProvider::get_table(&catalog, "external_events")
        .await
        .expect("load changed external table");
    let changed_stats = changed.statistics.as_ref().unwrap();
    assert_eq!(changed_stats.num_rows, Some(6));
    assert_eq!(changed_stats.column_statistics["key"].distinct_count, None);
    assert!(
        changed_stats.column_statistics["key"]
            .most_common_values
            .is_none()
    );

    let refreshed = engine
        .analyze_table_with_config("external_events", config)
        .await
        .expect("reanalyze changed external table");
    assert_eq!(refreshed.statistics.num_rows, Some(6));
    assert_eq!(
        refreshed.statistics.column_statistics["key"]
            .most_common_values
            .as_ref()
            .unwrap()
            .analyzed_count,
        6
    );
}
