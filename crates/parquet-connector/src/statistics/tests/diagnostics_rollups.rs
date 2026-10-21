use super::*;

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
