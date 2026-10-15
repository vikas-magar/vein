mod safety;

use super::*;
use plan::{
    ColumnId, ColumnStatistics, ResolvedColumn, ResolvedColumnRef, TableMetadata,
    TableStatistics, VeinDataType, VeinSchema,
};
use std::sync::Arc;

fn scan_with_stats() -> (LogicalPlan, ResolvedColumnRef) {
    let column = Arc::new(ResolvedColumn::new(ColumnId(1), "id", None, VeinDataType::Int64));
    let schema = VeinSchema {
        fields: vec![column.clone()],
    };
    let mut table_stats = TableStatistics::default();
    table_stats.column_statistics.insert(
        "id".to_string(),
        ColumnStatistics {
            min_value: Some(LogicalLiteral::Int64(10)),
            max_value: Some(LogicalLiteral::Int64(20)),
            min_max_complete: Some(true),
            null_count: Some(0),
            ..ColumnStatistics::default()
        },
    );
    table_stats.num_rows = Some(11);
    let table = TableMetadata::new("t", Arc::new(schema.clone())).with_statistics(table_stats);
    (
        LogicalPlan::Scan {
            table: Arc::new(table),
            projection: None,
            filters: vec![],
            schema: Box::new(schema),
        },
        column,
    )
}

fn predicate(column: &ResolvedColumnRef, op: BinaryOp, value: i64) -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(column.clone())),
        op,
        right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(value))),
        return_type: VeinDataType::Boolean,
    }
}

#[test]
fn prunes_out_of_range_equality() {
    let (scan, column) = scan_with_stats();
    let plan = LogicalPlan::Filter {
        input: Box::new(scan),
        predicate: predicate(&column, BinaryOp::Eq, 99),
    };
    let rewritten = ScanStatisticsPruningRule
        .apply(&plan, &OptimizerContext::default())
        .unwrap()
        .expect("statistics prove empty");
    assert!(matches!(rewritten, LogicalPlan::Limit { count: 0, .. }));
}

#[test]
fn prunes_impossible_null_checks() {
    let (scan, column) = scan_with_stats();
    let plan = LogicalPlan::Filter {
        input: Box::new(scan),
        predicate: LogicalExpr::IsNull {
            expr: Box::new(LogicalExpr::Column(column)),
        },
    };
    assert!(matches!(
        ScanStatisticsPruningRule
            .apply(&plan, &OptimizerContext::default())
            .unwrap(),
        Some(LogicalPlan::Limit { count: 0, .. })
    ));
}

#[test]
fn preserves_uncertain_predicates() {
    let (scan, column) = scan_with_stats();
    let plan = LogicalPlan::Filter {
        input: Box::new(scan),
        predicate: predicate(&column, BinaryOp::Gt, 15),
    };
    assert!(
        ScanStatisticsPruningRule
            .apply(&plan, &OptimizerContext::default())
            .unwrap()
            .is_none()
    );
}
