use std::collections::HashMap;
use std::sync::Arc;

use catalog::{DataUnitStatistics, StatisticsUnitKind};
use plan::{ColumnId, ColumnStatistics, ResolvedColumn, TableStatistics, VeinDataType, VeinSchema};

use crate::estimation::{EstimateConfidence, EstimateSource};

use super::statistics_estimate;

#[test]
fn invalid_column_synopsis_does_not_discard_population_or_other_columns() {
    let schema = VeinSchema::new(vec![
        Arc::new(ResolvedColumn::new(
            ColumnId(1), "valid_key", None, VeinDataType::Int64,
        )),
        Arc::new(ResolvedColumn::new(
            ColumnId(2), "invalid_key", None, VeinDataType::Int64,
        )),
    ]);
    let statistics = TableStatistics {
        num_rows: Some(100),
        column_statistics: HashMap::from([
            (
                "valid_key".into(),
                ColumnStatistics {
                    distinct_count: Some(10),
                    null_count: Some(0),
                    ..Default::default()
                },
            ),
            (
                "invalid_key".into(),
                ColumnStatistics {
                    null_count: Some(101),
                    ..Default::default()
                },
            ),
        ]),
        ..Default::default()
    };

    let estimate = statistics_estimate(
        &statistics,
        &schema,
        |column| column.name.clone(),
        EstimateSource::Statistics,
    );

    assert_eq!(estimate.rows.value(), Some(100.0));
    assert_eq!(estimate.rows.confidence, EstimateConfidence::Statistics);
    assert_eq!(estimate.columns[&ColumnId(1)].distinct, Some(10.0));
    assert_eq!(estimate.columns[&ColumnId(2)].distinct, Some(100.0));
    assert_eq!(estimate.columns[&ColumnId(2)].null_fraction, Some(0.0));
}

#[test]
fn complete_row_group_population_fills_missing_table_row_count() {
    let schema = VeinSchema::new(vec![Arc::new(ResolvedColumn::new(
        ColumnId(1), "key", None, VeinDataType::Int64,
    ))]);
    let mut file = DataUnitStatistics::new(StatisticsUnitKind::File, 0);
    file.parent = None;
    let mut first_group = DataUnitStatistics::new(StatisticsUnitKind::RowGroup, 0);
    first_group.parent = Some(0);
    first_group.num_rows = Some(40);
    let mut second_group = DataUnitStatistics::new(StatisticsUnitKind::RowGroup, 1);
    second_group.parent = Some(0);
    second_group.num_rows = Some(60);
    let statistics = TableStatistics {
        file_count: Some(1),
        row_group_count: Some(2),
        data_units: vec![file, first_group, second_group],
        ..Default::default()
    };

    let estimate = statistics_estimate(
        &statistics,
        &schema,
        |column| column.name.clone(),
        EstimateSource::Statistics,
    );

    assert_eq!(estimate.rows.value(), Some(100.0));
    assert_eq!(estimate.rows.source, EstimateSource::Statistics);
    assert_eq!(estimate.columns[&ColumnId(1)].distinct, Some(100.0));
}

#[test]
fn partial_or_overflowing_data_unit_counts_do_not_claim_a_complete_population() {
    let mut first = DataUnitStatistics::new(StatisticsUnitKind::RowGroup, 0);
    first.num_rows = Some(u64::MAX);
    let mut second = DataUnitStatistics::new(StatisticsUnitKind::RowGroup, 1);
    second.num_rows = Some(1);

    let incomplete = TableStatistics {
        row_group_count: Some(3),
        data_units: vec![first.clone(), second.clone()],
        ..Default::default()
    };
    let overflow = TableStatistics {
        row_group_count: Some(2),
        data_units: vec![first, second],
        ..Default::default()
    };

    assert_eq!(incomplete.population_rows(), None);
    assert_eq!(overflow.population_rows(), None);

    let empty = TableStatistics {
        file_count: Some(0),
        row_group_count: Some(0),
        ..Default::default()
    };
    assert_eq!(empty.population_rows(), Some(0));
}

#[test]
fn conflicting_complete_file_and_row_group_counts_are_rejected() {
    let mut file = DataUnitStatistics::new(StatisticsUnitKind::File, 0);
    file.num_rows = Some(100);
    let mut first_group = DataUnitStatistics::new(StatisticsUnitKind::RowGroup, 0);
    first_group.num_rows = Some(40);
    let mut second_group = DataUnitStatistics::new(StatisticsUnitKind::RowGroup, 1);
    second_group.num_rows = Some(50);
    let statistics = TableStatistics {
        file_count: Some(1),
        row_group_count: Some(2),
        data_units: vec![file, first_group, second_group],
        ..Default::default()
    };

    assert_eq!(statistics.population_rows(), None);
}
