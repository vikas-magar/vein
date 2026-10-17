use super::*;
use super::bloom::predicate_may_match_bloom;
use arrow::array::{
    BooleanArray, Int64Array, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use catalog::{ColumnStatistics, StatisticsComparison, StatisticsPredicate};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReaderBuilder, RowGroupSelection};
use parquet::arrow::ArrowWriter;
use parquet::basic::Type as ParquetType;
use parquet::bloom_filter::Sbbf;
use parquet::file::metadata::{ColumnChunkMetaData, PageIndexPolicy, ParquetMetaDataReader};
use parquet::file::properties::WriterProperties;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use types::LogicalLiteral;

fn file_unit(rows: u64, bytes: u64) -> DataUnitStatistics {
    let mut unit = DataUnitStatistics::new(StatisticsUnitKind::File, 0);
    unit.num_rows = Some(rows);
    unit.physical_bytes = Some(bytes);
    unit
}

fn row_group(
    ordinal: usize,
    rows: u64,
    column: ColumnStatistics,
) -> DataUnitStatistics {
    let mut unit = DataUnitStatistics::new(StatisticsUnitKind::RowGroup, ordinal);
    unit.parent = Some(0);
    unit.num_rows = Some(rows);
    unit.compressed_bytes = Some(10);
    unit.uncompressed_bytes = Some(20);
    unit.column_statistics.insert("value".into(), column);
    unit
}

#[test]
fn pruning_diagnostics_separate_proven_pruning_from_retained_reasons() {
    let predicate = equality("value", 5);
    let mut diagnostics = PredicatePruningDiagnostics::for_predicates(&[
        predicate.clone(),
        StatisticsPredicate::Unknown,
    ]);

    let complete_overlap = row_group(
        0,
        4,
        ColumnStatistics {
            null_count: Some(0),
            min_value: Some(LogicalLiteral::Int64(1)),
            max_value: Some(LogicalLiteral::Int64(9)),
            min_max_complete: Some(true),
            ..ColumnStatistics::default()
        },
    );
    diagnostics.record(0, &predicate, &complete_overlap, true);

    let missing = DataUnitStatistics::new(StatisticsUnitKind::RowGroup, 1);
    diagnostics.record(0, &predicate, &missing, true);
    diagnostics.record_missing_metadata(0);

    let incomplete = row_group(
        2,
        4,
        ColumnStatistics {
            min_value: Some(LogicalLiteral::Int64(1)),
            max_value: Some(LogicalLiteral::Int64(9)),
            min_max_complete: Some(false),
            ..ColumnStatistics::default()
        },
    );
    diagnostics.record(0, &predicate, &incomplete, true);

    let disjoint = row_group(
        3,
        4,
        ColumnStatistics {
            null_count: Some(0),
            min_value: Some(LogicalLiteral::Int64(10)),
            max_value: Some(LogicalLiteral::Int64(20)),
            min_max_complete: Some(true),
            ..ColumnStatistics::default()
        },
    );
    diagnostics.record(0, &predicate, &disjoint, false);
    diagnostics.record(1, &StatisticsPredicate::Unknown, &missing, true);

    let predicate_diagnostic = &diagnostics.predicates[0];
    assert_eq!(predicate_diagnostic.units_considered, 5);
    assert_eq!(predicate_diagnostic.units_pruned, 1);
    assert_eq!(predicate_diagnostic.retained_missing_statistics, 2);
    assert_eq!(predicate_diagnostic.retained_incomplete_bounds, 1);
    assert_eq!(predicate_diagnostic.retained_bounds_may_match, 1);
    assert_eq!(diagnostics.predicates[1].retained_unsupported, 1);
}

#[test]
fn pruning_diagnostic_labels_are_bounded() {
    let predicate = StatisticsPredicate::Comparison {
        column: "value".into(),
        comparison: StatisticsComparison::Equal,
        literal: LogicalLiteral::String("x".repeat(512)),
    };

    let diagnostics = PredicatePruningDiagnostics::for_predicates(&[predicate]);

    assert!(diagnostics.predicates[0].predicate.chars().count() <= 163);
    assert!(diagnostics.predicates[0].predicate.ends_with("..."));
}

#[test]
fn table_rollup_preserves_scope_and_only_sums_mergeable_statistics() {
    let first = ColumnStatistics {
        null_count: Some(0),
        distinct_count: Some(2),
        nan_count: Some(0),
        min_value: Some(LogicalLiteral::Int64(1)),
        max_value: Some(LogicalLiteral::Int64(4)),
        ..ColumnStatistics::default()
    };
    let second = ColumnStatistics {
        null_count: Some(1),
        distinct_count: Some(2),
        nan_count: Some(0),
        min_value: Some(LogicalLiteral::Int64(6)),
        max_value: Some(LogicalLiteral::Int64(9)),
        ..ColumnStatistics::default()
    };
    let stats = table_statistics(
        vec![file_unit(4, 100), row_group(0, 2, first), row_group(1, 2, second)],
        &Schema::new(vec![Field::new("value", DataType::Int64, true)]),
    );

    assert_eq!(stats.num_rows, Some(4));
    assert_eq!(stats.total_bytes, Some(100));
    assert_eq!(stats.file_count, Some(1));
    assert_eq!(stats.row_group_count, Some(2));
    assert_eq!(stats.compressed_bytes, Some(20));
    assert_eq!(stats.uncompressed_bytes, Some(40));
    let column = &stats.column_statistics["value"];
    assert_eq!(column.null_count, Some(1));
    assert_eq!(column.distinct_count, None, "local NDVs must never be added");
    assert_eq!(column.min_value, Some(LogicalLiteral::Int64(1)));
    assert_eq!(column.max_value, Some(LogicalLiteral::Int64(9)));
    assert_eq!(stats.data_units.len(), 3);
    assert_eq!(stats.data_units[1].parent, Some(0));
}

#[test]
fn table_bounds_and_null_counts_stay_unknown_when_group_stats_are_partial() {
    let first = ColumnStatistics {
        null_count: Some(0),
        min_value: Some(LogicalLiteral::Int64(1)),
        max_value: Some(LogicalLiteral::Int64(4)),
        ..ColumnStatistics::default()
    };
    let second = ColumnStatistics {
        null_count: None,
        min_value: None,
        max_value: Some(LogicalLiteral::Int64(9)),
        ..ColumnStatistics::default()
    };
    let stats = table_statistics(
        vec![file_unit(4, 100), row_group(0, 2, first), row_group(1, 2, second)],
        &Schema::new(vec![Field::new("value", DataType::Int64, true)]),
    );
    let column = &stats.column_statistics["value"];
    assert_eq!(column.null_count, None);
    assert_eq!(
        column.min_value,
        None,
        "an unknown group minimum prevents a table minimum"
    );
    assert_eq!(column.max_value, Some(LogicalLiteral::Int64(9)));
}

#[test]
fn checked_rollups_reject_overflow_and_empty_inputs() {
    assert_eq!(complete_sum([Some(u64::MAX), Some(1)]), None);
    assert_eq!(complete_sum([Some(1), None]), None);
    assert_eq!(complete_sum([]), None);
}

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

fn int64_bloom_filter(name: &str, values: impl IntoIterator<Item=i64>) -> ParquetBloomFilter {
    let mut filter = Sbbf::new_with_num_of_bytes(4096);
    for value in values {
        filter.insert(&value);
    }
    ParquetBloomFilter::new(
        BloomFilterColumn {
            name: name.to_string(),
            parquet_column_index: 0,
            arrow_type: DataType::Int64,
            parquet_type: ParquetType::INT64,
            type_length: -1,
        },
        filter,
    )
}

fn dictionary_equality(column: &str, literal: LogicalLiteral) -> StatisticsPredicate {
    StatisticsPredicate::Comparison {
        column: column.to_string(),
        comparison: StatisticsComparison::Equal,
        literal,
    }
}

fn write_dictionary_row_groups(path: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("shipmode", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
    ]));
    let properties = WriterProperties::builder()
        .set_dictionary_enabled(true)
        .set_max_row_group_row_count(Some(2))
        .build();
    let file = std::fs::File::create(path).expect("create dictionary Parquet file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(properties))
        .expect("create dictionary writer");
    for (modes, ids) in [
        (vec!["MAIL", "RAIL"], vec![10, 20]),
        (vec!["SHIP", "AIR"], vec![30, 40]),
    ] {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(modes)),
                Arc::new(Int64Array::from(ids)),
            ],
        )
            .expect("valid dictionary test batch");
        writer.write(&batch).expect("write dictionary test rows");
        writer.flush().expect("finish dictionary row group");
    }
    writer.close().expect("finish dictionary Parquet file");
}

fn dictionary_page_bytes(
    file_bytes: &Bytes,
    file_length: u64,
    column: &ColumnChunkMetaData,
) -> Bytes {
    let range = dictionary_page_range(column, file_length)
        .expect("dictionary page range must be available");
    let start = usize::try_from(range.start).expect("range start fits usize");
    let end = usize::try_from(range.end).expect("range end fits usize");
    file_bytes.slice(start..end)
}

#[test]
fn dictionary_predicates_extract_equality_and_safe_in_lists_only() {
    let equality = dictionary_equality("shipmode", LogicalLiteral::String("MAIL".into()));
    let in_list = StatisticsPredicate::Or(vec![
        dictionary_equality("shipmode", LogicalLiteral::String("MAIL".into())),
        dictionary_equality("shipmode", LogicalLiteral::String("SHIP".into())),
    ]);
    let mixed_or = StatisticsPredicate::Or(vec![
        equality.clone(),
        StatisticsPredicate::Unknown,
    ]);
    let other_column = StatisticsPredicate::Or(vec![
        equality.clone(),
        dictionary_equality("other", LogicalLiteral::String("SHIP".into())),
    ]);
    let predicates = dictionary_predicates(&[
        StatisticsPredicate::And(vec![equality, in_list]),
        mixed_or,
        other_column,
    ]);

    assert_eq!(predicates.len(), 2);
    assert_eq!(predicates[0].index, 0);
    assert_eq!(predicates[0].candidates, vec![LogicalLiteral::String("MAIL".into())]);
    assert_eq!(predicates[1].index, 1);
    assert_eq!(predicates[1].candidates.len(), 2);
}

#[test]
fn dictionary_pages_prune_absent_values_and_keep_present_values() {
    let root = tempfile::tempdir().expect("temporary root");
    let path = root.path().join("dictionary_filter.parquet");
    write_dictionary_row_groups(&path);

    let bytes = Bytes::from(std::fs::read(&path).expect("read test Parquet bytes"));
    let file_length = bytes.len() as u64;
    let file = std::fs::File::open(&path).expect("open dictionary Parquet file");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("read Parquet footer");
    assert_eq!(builder.metadata().num_row_groups(), 2);

    let predicate = dictionary_equality("shipmode", LogicalLiteral::String("SHIP".into()));
    let probes = dictionary_predicates(std::slice::from_ref(&predicate));
    let columns = bloom_filter_columns(
        builder.schema().as_ref(),
        builder.metadata(),
        std::slice::from_ref(&predicate),
    );
    let column = columns
        .iter()
        .find(|column| column.name == "shipmode")
        .expect("resolve dictionary column");

    let first_chunk = builder.metadata().row_group(0).column(0);
    let second_chunk = builder.metadata().row_group(1).column(0);
    assert!(is_fully_dictionary_encoded(first_chunk));
    assert!(is_fully_dictionary_encoded(second_chunk));
    let first_matches = dictionary_page_predicate_matches(
        dictionary_page_bytes(&bytes, file_length, first_chunk),
        file_length,
        2,
        first_chunk,
        column,
        &probes,
    );
    let second_matches = dictionary_page_predicate_matches(
        dictionary_page_bytes(&bytes, file_length, second_chunk),
        file_length,
        2,
        second_chunk,
        column,
        &probes,
    );
    assert_eq!(first_matches, vec![Some(false)]);
    assert_eq!(second_matches, vec![Some(true)]);

    let id_predicate = dictionary_equality("id", LogicalLiteral::Int64(30));
    let id_probes = dictionary_predicates(std::slice::from_ref(&id_predicate));
    let id_column = bloom_filter_columns(
        builder.schema().as_ref(),
        builder.metadata(),
        std::slice::from_ref(&id_predicate),
    )
        .into_iter()
        .next()
        .expect("resolve Int64 dictionary column");
    let first_id_chunk = builder.metadata().row_group(0).column(1);
    let second_id_chunk = builder.metadata().row_group(1).column(1);
    assert_eq!(
        dictionary_page_predicate_matches(
            dictionary_page_bytes(&bytes, file_length, first_id_chunk),
            file_length,
            2,
            first_id_chunk,
            &id_column,
            &id_probes,
        ),
        vec![Some(false)]
    );
    assert_eq!(
        dictionary_page_predicate_matches(
            dictionary_page_bytes(&bytes, file_length, second_id_chunk),
            file_length,
            2,
            second_id_chunk,
            &id_column,
            &id_probes,
        ),
        vec![Some(true)]
    );

    let incompatible_predicate =
        dictionary_equality("shipmode", LogicalLiteral::Int64(30));
    let incompatible_probes =
        dictionary_predicates(std::slice::from_ref(&incompatible_predicate));
    assert_eq!(
        dictionary_page_predicate_matches(
            dictionary_page_bytes(&bytes, file_length, first_chunk),
            file_length,
            2,
            first_chunk,
            column,
            &incompatible_probes,
        ),
        vec![None],
        "a type mismatch must retain the row group"
    );

    let membership = HashMap::from([
        (0, first_matches),
        (1, second_matches),
    ]);
    let pruning = prune_row_groups_with_dictionary(
        builder.metadata(),
        vec![
            RowGroupSelection::new(0, None),
            RowGroupSelection::new(1, None),
        ],
        &membership,
    );
    assert_eq!(pruning.row_groups_skipped, 1);
    assert_eq!(pruning.rows_skipped, 2);
    assert_eq!(pruning.probes, 2);
    assert_eq!(pruning.selections.len(), 1);
    assert_eq!(pruning.selections[0].row_group_index(), 1);
}

#[test]
fn dictionary_page_pruning_fails_open_for_chunks_without_dictionary_encoding() {
    let root = tempfile::tempdir().expect("temporary root");
    let path = root.path().join("plain_column.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("shipmode", DataType::Utf8, false)]));
    let properties = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .build();
    let file = std::fs::File::create(&path).expect("create plain Parquet file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(properties))
        .expect("create plain writer");
    writer
        .write(
            &RecordBatch::try_new(
                schema,
                vec![Arc::new(StringArray::from(vec!["MAIL", "RAIL"]))],
            )
                .expect("valid plain batch"),
        )
        .expect("write plain values");
    writer.close().expect("finish plain Parquet file");
    let file = std::fs::File::open(&path).expect("open plain Parquet file");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("read plain footer");
    let chunk = builder.metadata().row_group(0).column(0);

    assert!(!is_fully_dictionary_encoded(chunk));
    assert!(dictionary_page_range(chunk, 1024).is_none());
}

fn equality(column: &str, value: i64) -> StatisticsPredicate {
    StatisticsPredicate::Comparison {
        column: column.to_string(),
        comparison: StatisticsComparison::Equal,
        literal: LogicalLiteral::Int64(value),
    }
}

#[test]
fn complete_page_index_coverage_skips_header_pre_read() {
    let predicates = [
        equality("shipmode", 1),
        StatisticsPredicate::And(vec![
            equality("receiptdate", 2),
            equality("shipmode", 3),
        ]),
    ];
    let page_index = PagePruning {
        indexed_columns: 4,
        page_bounds_complete: 8,
        ..PagePruning::default()
    };

    assert_eq!(
        page_header_columns_needed(&predicates, &[0, 1], &[0, 1], true, &page_index),
        Vec::<String>::new()
    );
}

#[test]
fn audit_mode_requests_predicate_headers_with_complete_page_index_coverage() {
    let predicates = [
        equality("shipmode", 1),
        StatisticsPredicate::And(vec![
            equality("receiptdate", 2),
            equality("shipmode", 3),
        ]),
    ];
    let complete_index = PagePruning {
        indexed_columns: 4,
        page_bounds_complete: 8,
        ..PagePruning::default()
    };

    assert_eq!(
        page_header_columns_needed_with_audit(
            &predicates,
            &[0, 1],
            &[0, 1],
            true,
            &complete_index,
            true,
        ),
        vec!["receiptdate", "shipmode"]
    );
}

#[test]
fn incomplete_or_missing_page_index_coverage_requests_predicate_headers() {
    let predicates = [
        equality("shipmode", 1),
        StatisticsPredicate::And(vec![
            equality("receiptdate", 2),
            equality("shipmode", 3),
        ]),
    ];
    let expected = vec!["receiptdate", "shipmode"];
    let incomplete_index = PagePruning {
        indexed_columns: 4,
        page_bounds_incomplete: 1,
        ..PagePruning::default()
    };
    let missing_column_index = PagePruning {
        indexed_columns: 3,
        ..PagePruning::default()
    };

    assert_eq!(
        page_header_columns_needed(&predicates, &[1], &[0, 1], true, &incomplete_index),
        expected
    );
    assert_eq!(
        page_header_columns_needed(
            &predicates,
            &[1],
            &[0, 1],
            true,
            &missing_column_index,
        ),
        expected
    );
    assert_eq!(
        page_header_columns_needed(&predicates, &[1], &[0, 1], false, &PagePruning::default()),
        expected
    );
}

#[test]
fn page_header_checks_are_omitted_without_predicates_or_probe_row_groups() {
    let page_index = PagePruning::default();
    assert!(page_header_columns_needed(&[], &[0], &[0], false, &page_index).is_empty());
    assert!(page_header_columns_needed(
        &[equality("shipmode", 1)],
        &[],
        &[0],
        false,
        &page_index,
    )
        .is_empty());
}

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