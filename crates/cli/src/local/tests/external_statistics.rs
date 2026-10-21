use super::*;
use common::statistics::StatisticsConfig;

#[tokio::test]
async fn analyzes_parquet_tables_and_publishes_synopses() {
    let root = tempfile::tempdir().expect("temporary data root");
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
    ]));
    let input = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(2), Some(2), None])),
            Arc::new(StringArray::from(vec![
                Some("A"),
                Some("A"),
                Some("B"),
                Some("A"),
                None,
            ])),
        ],
    )
        .expect("valid analysis input");
    write_parquet_table(
        root.path(),
        "analyzed_events",
        "create",
        Arc::clone(&schema),
        &input,
    );

    let engine = test_engine(root.path());
    let config = StatisticsConfig {
        scan_batch_size: 2,
        most_common_capacity: 4,
        histogram_sample_size: 8,
        histogram_bucket_count: 2,
        multi_column_sets: vec![vec!["label".to_owned(), "key".to_owned()]],
        ..StatisticsConfig::default()
    };
    let analyzed = engine
        .analyze_table_with_config("analyzed_events", config)
        .await
        .expect("analyze Parquet table");

    assert_eq!(analyzed.statistics.num_rows, Some(5));
    assert_eq!(analyzed.statistics.column_groups[0].columns, vec!["key", "label"]);
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
    let loaded = CatalogProvider::get_table(&catalog, "analyzed_events")
        .await
        .expect("load published analysis");
    let statistics = loaded.statistics.as_ref().expect("published statistics");
    assert_eq!(statistics.num_rows, Some(5));
    assert_eq!(statistics.column_groups[0].distinct_count, Some(4));
    assert!(statistics.column_statistics["key"].most_common_values.is_some());
}
