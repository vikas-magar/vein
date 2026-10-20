use super::*;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(1), None])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("a"), None])),
        ],
    )
        .expect("test record batch")
}

#[test]
fn collects_mergeable_table_ndv_and_null_counts_from_borrowed_batches() {
    let batch = batch();
    let input_array = Arc::clone(batch.column(0));
    let input_string_array = Arc::clone(batch.column(1));
    let retained_references = Arc::strong_count(&input_array);
    let retained_string_references = Arc::strong_count(&input_string_array);
    let input_values = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().as_ptr();
    let mut collector = StatisticsCollector::new(
        batch.schema(),
        StatisticsConfig { distinct_precision: 10, ..StatisticsConfig::default() },
    )
        .expect("collector");
    collector.update_input(&batch).expect("collect input batch");
    let statistics = collector.finish();

    assert_eq!(statistics.num_rows, Some(4));
    assert_eq!(statistics.column_statistics["key"].null_count, Some(1));
    assert_eq!(statistics.column_statistics["key"].distinct_count, Some(2));
    assert_eq!(statistics.column_statistics["key"].distinct_count_relative_error, Some(1.04 / 32.0));
    assert_eq!(statistics.column_statistics["label"].null_count, Some(1));
    assert_eq!(statistics.column_statistics["label"].distinct_count, Some(2));
    assert_eq!(batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().as_ptr(), input_values);
    assert_eq!(Arc::strong_count(&input_array), retained_references);
    assert_eq!(Arc::strong_count(&input_string_array), retained_string_references);
}

#[test]
fn collector_merge_and_restore_preserve_table_wide_ndv() {
    let batch = batch();
    let schema = batch.schema();
    let config = StatisticsConfig { distinct_precision: 10, ..StatisticsConfig::default() };
    let mut left = StatisticsCollector::new(Arc::clone(&schema), config.clone()).unwrap();
    let mut right = StatisticsCollector::new(Arc::clone(&schema), config.clone()).unwrap();
    left.update_input(&batch.slice(0, 2)).unwrap();
    right.update_input(&batch.slice(2, 2)).unwrap();
    left.merge(&right).unwrap();

    let merged = left.finish();
    assert_eq!(merged.num_rows, Some(4));
    assert_eq!(merged.column_statistics["key"].distinct_count, Some(2));
    assert_eq!(merged.column_statistics["key"].null_count, Some(1));

    let mut restored = StatisticsCollector::restore(
        Arc::clone(&schema),
        config,
        &merged,
    )
        .expect("restore serialized mergeable state");
    restored.update_input(&batch.slice(0, 1)).unwrap();
    let appended = restored.finish();
    assert_eq!(appended.num_rows, Some(5));
    assert_eq!(appended.column_statistics["key"].distinct_count, Some(2));
    assert_eq!(appended.column_statistics["key"].min_value, Some(LogicalLiteral::Int64(1)));
    assert_eq!(appended.column_statistics["key"].max_value, Some(LogicalLiteral::Int64(2)));
    assert_eq!(appended.column_statistics["key"].numeric.as_ref().unwrap().finite_count, 4);
    assert_eq!(appended.column_statistics["label"].byte_length.as_ref().unwrap().value_count, 4);
}

#[test]
fn collects_exact_bounds_numeric_moments_and_utf8_lengths() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("measure", DataType::Float64, true),
        Field::new("label", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float64Array::from(vec![
                Some(3.0), Some(f64::NAN), Some(f64::INFINITY),
                Some(f64::NEG_INFINITY), None, Some(-2.0), Some(9.0),
            ])),
            Arc::new(StringArray::from(vec![Some("a"), Some("é"), Some("medium"), None, Some(""), None, None])),
        ],
    ).unwrap();
    let mut collector = StatisticsCollector::new(schema, StatisticsConfig::default()).unwrap();
    collector.update_input(&batch).unwrap();
    let stats = collector.finish();

    let measure = &stats.column_statistics["measure"];
    assert_eq!(measure.null_count, Some(1));
    assert_eq!(measure.nan_count, Some(1));
    assert_eq!(measure.min_value, Some(LogicalLiteral::Float64(f64::NEG_INFINITY)));
    assert_eq!(measure.max_value, Some(LogicalLiteral::Float64(f64::INFINITY)));
    assert_eq!(measure.min_max_complete, Some(true));
    let numeric = measure.numeric.as_ref().unwrap();
    assert_eq!(numeric.finite_count, 3);
    assert_eq!(numeric.non_finite_count, 3);
    assert_eq!(numeric.sum, Some(10.0));
    assert!((numeric.mean.unwrap() - (10.0 / 3.0)).abs() < 1e-12);
    assert!((numeric.m2.unwrap() - (182.0 / 3.0)).abs() < 1e-12);

    let label = &stats.column_statistics["label"];
    assert_eq!(label.min_value, Some(LogicalLiteral::String("".to_owned())));
    assert_eq!(label.max_value, Some(LogicalLiteral::String("é".to_owned())));
    assert_eq!(label.min_max_complete, Some(true));
    assert_eq!(label.byte_length, Some(ByteLengthStatistics {
        value_count: 4,
        total_bytes: Some(9),
        min_bytes: Some(0),
        max_bytes: Some(6),
        contains_non_ascii: Some(true),
    }));
}

#[test]
fn uint64_bounds_preserve_values_above_signed_range() {
    let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::UInt64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt64Array::from(vec![1, i64::MAX as u64 + 1, u64::MAX]))],
    ).unwrap();
    let mut collector = StatisticsCollector::new(schema, StatisticsConfig::default()).unwrap();
    collector.update_input(&batch).unwrap();
    let summary = collector.finish().column_statistics.remove("key").unwrap();
    assert_eq!(summary.min_value, Some(LogicalLiteral::UInt64(1)));
    assert_eq!(summary.max_value, Some(LogicalLiteral::UInt64(u64::MAX)));
    assert_eq!(summary.min_max_complete, Some(true));
}

#[test]
fn oversized_string_bounds_become_unknown_without_losing_length_stats() {
    let schema = Arc::new(Schema::new(vec![Field::new("label", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["z", "a much longer string"]))],
    ).unwrap();
    let config = StatisticsConfig { max_bound_value_bytes: 3, ..StatisticsConfig::default() };
    let mut collector = StatisticsCollector::new(schema, config).unwrap();
    collector.update_input(&batch).unwrap();
    let summary = collector.finish().column_statistics.remove("label").unwrap();
    assert_eq!(summary.min_max_complete, Some(false));
    assert_eq!(summary.min_value, None);
    assert_eq!(summary.max_value, None);
    assert_eq!(summary.byte_length.as_ref().unwrap().max_bytes, Some(20));
    assert_eq!(summary.distinct_count, Some(2));
}

#[test]
fn empty_numeric_batches_keep_appendable_moments_and_empty_bounds() {
    let schema = Arc::new(Schema::new(vec![Field::new("measure", DataType::Int64, true)]));
    let empty = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(Vec::<Option<i64>>::new()))],
    ).unwrap();
    let one = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![Some(7)]))],
    ).unwrap();
    let mut collector = StatisticsCollector::new(schema.clone(), StatisticsConfig::default()).unwrap();
    collector.update_input(&empty).unwrap();
    let empty_stats = collector.finish();
    assert_eq!(empty_stats.column_statistics["measure"].min_max_complete, Some(true));
    assert_eq!(empty_stats.column_statistics["measure"].min_value, None);
    assert_eq!(empty_stats.column_statistics["measure"].numeric.as_ref().unwrap().mean, None);

    let mut restored = StatisticsCollector::restore(schema, StatisticsConfig::default(), &empty_stats).unwrap();
    restored.update_input(&one).unwrap();
    let stats = restored.finish();
    assert_eq!(stats.column_statistics["measure"].numeric.as_ref().unwrap().mean, Some(7.0));
    assert_eq!(stats.column_statistics["measure"].min_value, Some(LogicalLiteral::Int64(7)));
    assert_eq!(stats.column_statistics["measure"].max_value, Some(LogicalLiteral::Int64(7)));
}

#[test]
fn compatible_sketches_merge_and_round_trip_through_catalog_state() {
    let mut left = HyperLogLog::new(10).expect("sketch");
    let mut right = HyperLogLog::new(10).expect("sketch");
    for value in 0..1_000_u64 {
        left.update_hash(stable_hash(5, &value.to_le_bytes()));
    }
    for value in 1_000..2_000_u64 {
        right.update_hash(stable_hash(5, &value.to_le_bytes()));
    }
    left.merge(&right).expect("merge sketches");
    assert!((1_850..=2_150).contains(&left.estimate().unwrap()));
    let restored = HyperLogLog::from_serialized(&left.to_serialized()).expect("restore");
    assert_eq!(left, restored);
}

#[test]
fn unsupported_arrow_types_keep_counts_but_do_not_publish_a_false_ndv() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "nested",
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        true,
    )]));
    let batch = RecordBatch::new_empty(schema.clone());
    let mut collector = StatisticsCollector::new(schema, StatisticsConfig::default()).unwrap();
    collector.update_input(&batch).unwrap();
    let stats = collector.finish();
    assert_eq!(stats.num_rows, Some(0));
    assert_eq!(stats.column_statistics["nested"].null_count, Some(0));
    assert_eq!(stats.column_statistics["nested"].distinct_count, None);
    assert!(stats.column_statistics["nested"].distinct_sketch.is_none());
}

#[test]
fn malformed_and_incompatible_hll_states_are_rejected() {
    let mut state = HyperLogLog::new(10).unwrap().to_serialized();
    state.payload.pop();
    assert!(HyperLogLog::from_serialized(&state).is_err());

    let mut low = HyperLogLog::new(10).unwrap();
    let high = HyperLogLog::new(11).unwrap();
    assert!(low.merge(&high).is_err());
}

#[test]
fn bounded_most_common_synopsis_tracks_heavy_hitters_and_nulls_are_excluded() {
    let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, true)]));
    let mut values = vec![Some(1); 100];
    values.extend((2..102).map(Some));
    values.push(None);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(values))],
    ).expect("valid heavy-hitter batch");
    let config = StatisticsConfig {
        most_common_capacity: 2,
        histogram_sample_size: 7,
        histogram_bucket_count: 3,
        ..StatisticsConfig::default()
    };
    let mut collector = StatisticsCollector::new(schema, config).expect("collector");
    collector.update_input(&batch).expect("collect values");
    let summary = collector.finish().column_statistics.remove("key").unwrap();
    let most_common = summary.most_common_values.expect("frequency synopsis");

    assert_eq!(most_common.capacity, 2);
    assert_eq!(most_common.analyzed_count, 200);
    assert!(most_common.error_bound > 0);
    assert!(most_common.candidates.len() <= 2);
    let heavy = most_common.candidates.iter()
        .find(|candidate| candidate.value == LogicalLiteral::Int64(1))
        .expect("frequent key remains a candidate");
    assert!(heavy.lower_count > 0 && heavy.lower_count <= 100);

    let histogram = summary.histogram.expect("histogram synopsis");
    assert_eq!(histogram.population_count, 200);
    assert_eq!(histogram.sample_count, 7);
    assert_eq!(histogram.samples.len(), 7);
    assert_eq!(histogram.buckets.len(), 3);
    assert_eq!(histogram.buckets.iter().map(|bucket| bucket.estimated_count).sum::<u64>(), 200);
    assert!(histogram.rank_error_bound.is_some_and(|error| error > 0.0 && error < 1.0));
}

#[test]
fn mcv_histogram_and_joint_ndv_merge_restore_and_serialize() {
    let batch = batch();
    let schema = batch.schema();
    let config = StatisticsConfig {
        most_common_capacity: 4,
        histogram_sample_size: 8,
        histogram_bucket_count: 2,
        multi_column_sets: vec![vec!["label".to_owned(), "key".to_owned()]],
        ..StatisticsConfig::default()
    };
    let mut left = StatisticsCollector::new(Arc::clone(&schema), config.clone()).unwrap();
    let mut right = StatisticsCollector::new(Arc::clone(&schema), config.clone()).unwrap();
    left.update_input(&batch.slice(0, 2)).unwrap();
    right.update_input(&batch.slice(2, 2)).unwrap();
    left.merge(&right).unwrap();

    let merged = left.finish();
    let encoded = serde_json::to_vec(&merged).expect("serialize statistics");
    let merged: TableStatistics = serde_json::from_slice(&encoded).expect("restore statistics model");
    assert!(merged.column_statistics["key"].is_consistent_with_rows(4));
    let key = &merged.column_statistics["key"];
    assert_eq!(key.most_common_values.as_ref().unwrap().analyzed_count, 3);
    assert_eq!(key.histogram.as_ref().unwrap().population_count, 3);
    assert_eq!(merged.column_groups.len(), 1);
    assert_eq!(merged.column_groups[0].columns, vec!["key", "label"]);
    assert_eq!(merged.column_groups[0].distinct_count, Some(3));

    let mut restored = StatisticsCollector::restore(Arc::clone(&schema), config, &merged)
        .expect("restore mergeable statistics");
    restored.update_input(&batch.slice(0, 1)).unwrap();
    let appended = restored.finish();
    assert_eq!(appended.column_statistics["key"].most_common_values.as_ref().unwrap().analyzed_count, 4);
    assert_eq!(appended.column_statistics["key"].histogram.as_ref().unwrap().population_count, 4);
    assert_eq!(appended.column_groups[0].distinct_count, Some(3));

    let mut malformed = appended.column_statistics["key"].clone();
    malformed.histogram.as_mut().unwrap().buckets[0].estimated_count += 1;
    assert!(!malformed.is_consistent_with_rows(5));
}
