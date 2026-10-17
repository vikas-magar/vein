use std::sync::Arc;

use common::statistics::{intermediate_column_key, QueryStatisticsContext};
use plan::{ColumnStatistics, ResolvedColumn, TableMetadata, TableStatistics, VeinSchema};

use super::*;
use crate::context::{OptimizerConfig, QueryStatisticsProvider};

pub(super) fn field(id: usize) -> plan::ResolvedColumnRef {
    Arc::new(ResolvedColumn::new(ColumnId(id), format!("key_{id}"), None, VeinDataType::Int64).with_nullable(true))
}

pub(super) fn bounds(min: i64, max: i64) -> ColumnStatistics {
    ColumnStatistics {
        min_value: Some(LogicalLiteral::Int64(min)),
        max_value: Some(LogicalLiteral::Int64(max)),
        min_max_complete: Some(true),
        nan_count: Some(0),
        null_count: Some(0),
        ..Default::default()
    }
}

pub(super) fn context(rows: u64, column: Option<ColumnStatistics>) -> OptimizerContext {
    OptimizerContext::with_statistics_provider(OptimizerConfig::default(), provider(rows, column))
}

pub(super) fn provider(rows: u64, column: Option<ColumnStatistics>) -> Arc<QueryStatisticsProvider> {
    let context = Arc::new(QueryStatisticsContext::new([]));
    let mut statistics = TableStatistics { num_rows: Some(rows), ..Default::default() };
    if let Some(column) = column {
        statistics.column_statistics.insert(intermediate_column_key(ColumnId(1)), column);
    }
    context.record_intermediate(7, Arc::new(statistics)).unwrap();
    Arc::new(QueryStatisticsProvider::new(context))
}

pub(super) fn join(rows: u64, join_type: JoinType) -> LogicalPlan {
    let left = field(1);
    let right = field(2);
    let right_schema = VeinSchema { fields: vec![right.clone()] };
    LogicalPlan::Join {
        join_type,
        left: Box::new(LogicalPlan::Intermediate {
            source_id: 7,
            row_count: rows,
            schema: Box::new(VeinSchema { fields: vec![left.clone()] }),
        }),
        right: Box::new(LogicalPlan::Scan {
            table: Arc::new(TableMetadata::new("unread", Arc::new(right_schema.clone()))),
            projection: None,
            filters: vec![],
            schema: Box::new(right_schema),
        }),
        on: Some(LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(left.clone())),
            op: BinaryOp::Eq,
            right: Box::new(LogicalExpr::Column(right.clone())),
            return_type: VeinDataType::Boolean,
        }),
        using: vec![],
        schema: Box::new(VeinSchema { fields: vec![left, right] }),
    }
}

pub(super) fn predicate(op: BinaryOp, value: i64) -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(field(2))),
        op,
        right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(value))),
        return_type: VeinDataType::Boolean,
    }
}
