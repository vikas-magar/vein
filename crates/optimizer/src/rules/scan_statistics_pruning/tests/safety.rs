//! Regression coverage for shared provider freshness and bounds validation.
use super::*;
use crate::{StatisticsFreshness, StatisticsProvider, StatisticsSnapshot};

struct StaleProvider;
impl StatisticsProvider for StaleProvider {
    fn table_statistics<'a>(&'a self, table: &'a TableMetadata) -> Option<StatisticsSnapshot<'a>> {
        Some(StatisticsSnapshot {
            statistics: table.statistics.as_ref()?,
            freshness: StatisticsFreshness::Stale,
        })
    }
}

#[test]
fn stale_statistics_cannot_prune_a_scan() {
    let (scan, column) = scan_with_stats();
    let plan = LogicalPlan::Filter {
        input: Box::new(scan),
        predicate: predicate(&column, BinaryOp::Eq, 99),
    };
    let context = OptimizerContext::with_statistics_provider(Default::default(), Arc::new(StaleProvider));
    assert!(ScanStatisticsPruningRule.apply(&plan, &context).unwrap().is_none());
}

#[test]
fn contradictory_statistics_cannot_prune_a_scan() {
    let (mut scan, column) = scan_with_stats();
    let LogicalPlan::Scan { table, .. } = &mut scan else { unreachable!(); };
    let stats = Arc::make_mut(table).statistics.as_mut().unwrap();
    stats.column_statistics.get_mut("id").unwrap().null_count = Some(100);
    let plan = LogicalPlan::Filter {
        input: Box::new(scan),
        predicate: predicate(&column, BinaryOp::Eq, 99),
    };
    assert!(ScanStatisticsPruningRule.apply(&plan, &OptimizerContext::default()).unwrap().is_none());
}

#[test]
fn incomplete_min_max_bounds_cannot_prune_a_scan() {
    let (mut scan, column) = scan_with_stats();
    let LogicalPlan::Scan { table, .. } = &mut scan else { unreachable!(); };
    Arc::make_mut(table)
        .statistics
        .as_mut()
        .unwrap()
        .column_statistics
        .get_mut("id")
        .unwrap()
        .min_max_complete = Some(false);
    let plan = LogicalPlan::Filter {
        input: Box::new(scan),
        predicate: predicate(&column, BinaryOp::Eq, 99),
    };

    assert!(ScanStatisticsPruningRule
        .apply(&plan, &OptimizerContext::default())
        .unwrap()
        .is_none());
}

#[test]
fn decimal_bounds_use_the_shared_checked_comparator() {
    let c = Arc::new(ResolvedColumn::new(ColumnId(1), "price", None, VeinDataType::Decimal128(15, 2)));
    let schema = VeinSchema { fields: vec![c.clone()] };
    let statistics = TableStatistics {
        num_rows: Some(10),
        column_statistics: [("price".into(), ColumnStatistics {
            min_value: Some(LogicalLiteral::Decimal(100, 15, 2)),
            max_value: Some(LogicalLiteral::Decimal(200, 15, 2)),
            min_max_complete: Some(true),
            null_count: Some(0),
            distinct_count: Some(10),
            nan_count: None,
            ..ColumnStatistics::default()
        })].into(),
        ..Default::default()
    };
    let table = Arc::new(TableMetadata::new("arbitrary", Arc::new(schema.clone())).with_statistics(statistics));
    let scan = LogicalPlan::Scan { table, schema: Box::new(schema), projection: None, filters: vec![] };
    let predicate = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(c)),
        op: BinaryOp::Eq,
        right: Box::new(LogicalExpr::Literal(LogicalLiteral::Decimal(300, 15, 2))),
        return_type: VeinDataType::Boolean,
    };
    let plan = LogicalPlan::Filter { input: Box::new(scan), predicate };
    assert!(matches!(ScanStatisticsPruningRule.apply(&plan, &OptimizerContext::default()).unwrap(),
        Some(LogicalPlan::Limit { count: 0, .. })));
}
