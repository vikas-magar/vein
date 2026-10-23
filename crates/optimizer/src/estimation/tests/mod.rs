//! Estimator fixtures share one metadata/plan construction path.

mod statistics;
mod predicates;
mod joins;
mod operators;
mod integration;
mod edges;
mod missing_ndv;

use std::sync::Arc;
use parser::ast::{expr::BinaryOp, pipe_op::JoinType};
use plan::{ColumnId, ColumnStatistics, LogicalExpr, LogicalLiteral, LogicalPlan, PlanTraversal,
           ResolvedColumn, ResolvedColumnRef, TableMetadata, TableStatistics, VeinDataType, VeinSchema};
use super::*;
use crate::context::{CatalogStatisticsProvider, StatisticsFreshness, StatisticsProvider, StatisticsSnapshot};

fn column(id: usize, name: &str, nullable: bool) -> ResolvedColumnRef {
    Arc::new(ResolvedColumn::new(ColumnId(id), name, None, VeinDataType::Int64).with_nullable(nullable))
}

fn scan(id: usize, rows: Option<u64>, distinct: Option<u64>, nulls: Option<u64>) -> LogicalPlan {
    let column = column(id, "key", nulls != Some(0));
    let schema = VeinSchema { fields: vec![column] };
    let statistics = TableStatistics {
        num_rows: rows,
        column_statistics: [("key".into(), ColumnStatistics {
            distinct_count: distinct,
            null_count: nulls,
            ..Default::default()
        })].into(),
        ..Default::default()
    };
    let table = Arc::new(TableMetadata::new("arbitrary_table", Arc::new(schema.clone()))
        .with_statistics(statistics));
    LogicalPlan::Scan { table, projection: None, filters: vec![], schema: Box::new(schema) }
}

fn predicate(c: &ResolvedColumnRef, op: BinaryOp, value: LogicalLiteral) -> LogicalExpr {
    binary(LogicalExpr::Column(c.clone()), op, LogicalExpr::Literal(value))
}

fn binary(left: LogicalExpr, op: BinaryOp, right: LogicalExpr) -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
        return_type: VeinDataType::Boolean,
    }
}

fn filter(input: LogicalPlan, predicate: LogicalExpr) -> LogicalPlan {
    LogicalPlan::Filter { input: Box::new(input), predicate }
}

fn join(left: LogicalPlan, right: LogicalPlan, kind: JoinType, reversed: bool) -> LogicalPlan {
    let a = LogicalExpr::Column(left.schema().fields[0].clone());
    let b = LogicalExpr::Column(right.schema().fields[0].clone());
    let on = if kind == JoinType::Cross { None } else {
        Some(if reversed { binary(b, BinaryOp::Eq, a) } else { binary(a, BinaryOp::Eq, b) })
    };
    let fields = left.schema().fields.iter().chain(&right.schema().fields).cloned().collect();
    LogicalPlan::Join {
        join_type: kind,
        left: Box::new(left),
        right: Box::new(right),
        on,
        using: vec![],
        schema: Box::new(VeinSchema { fields }),
    }
}

fn root(plan: &LogicalPlan) -> NodeEstimate {
    StatisticsEstimator::new(&CatalogStatisticsProvider).estimate_plan(plan).pop().unwrap()
}

fn rows(plan: &LogicalPlan) -> Option<f64> { root(plan).rows.value() }

fn with_stats(plan: &mut LogicalPlan, change: impl FnOnce(&mut TableStatistics)) {
    let LogicalPlan::Scan { table, .. } = plan else { panic!("expected fixture scan"); };
    change(Arc::make_mut(table).statistics.as_mut().unwrap());
}

fn bounds(plan: &mut LogicalPlan, low: LogicalLiteral, high: LogicalLiteral) {
    with_stats(plan, |s| {
        let c = s.column_statistics.get_mut("key").unwrap();
        c.min_value = Some(low);
        c.max_value = Some(high);
    });
}

fn aggregate(input: LogicalPlan, sets: Vec<Vec<LogicalExpr>>) -> LogicalPlan {
    let schema = Box::new(input.schema().clone());
    LogicalPlan::Aggregate { input: Box::new(input), grouping_sets: sets, aggs: vec![], schema }
}

struct MarkedProvider(StatisticsFreshness);
impl StatisticsProvider for MarkedProvider {
    fn table_statistics<'a>(&'a self, table: &'a TableMetadata) -> Option<StatisticsSnapshot<'a>> {
        Some(StatisticsSnapshot { statistics: table.statistics.as_ref()?, freshness: self.0 })
    }
}
