//! Shared logical-plan fixtures for adaptive checkpoint tests.

use std::sync::Arc;

use catalog::TableMetadata;
use common::statistics::QueryStatisticsContext;
use plan::{ColumnId, ResolvedColumn, VeinDataType, VeinSchema};

use super::super::{
    AdaptiveBoundary, AdaptiveCheckpointPolicy, LogicalPlan, PlanTraversal,
    StatisticsCheckpointPolicy,
};

pub(super) fn predicate() -> plan::LogicalExpr {
    plan::LogicalExpr::BinaryOp {
        left: Box::new(plan::LogicalExpr::Column(Arc::new(ResolvedColumn::new(
            ColumnId(0),
            "id",
            None,
            VeinDataType::Int64,
        )))),
        op: parser::ast::expr::BinaryOp::Gt,
        right: Box::new(plan::LogicalExpr::Literal(plan::LogicalLiteral::Int64(0))),
        return_type: VeinDataType::Boolean,
    }
}

impl StatisticsCheckpointPolicy {
    pub(super) fn choose_for_test(&self, plan: &LogicalPlan) -> Option<AdaptiveBoundary> {
        self.choose_boundary(plan, Arc::new(QueryStatisticsContext::new([])))
    }
}

pub(super) fn scan(name: &str, rows: u64) -> LogicalPlan {
    let schema = Box::new(VeinSchema::new(vec![Arc::new(ResolvedColumn::new(
        ColumnId(0),
        "id",
        None,
        VeinDataType::Int64,
    ))]));
    let mut metadata =
        TableMetadata::new(name, Arc::new(*schema.clone())).with_file_format("parquet");
    metadata.statistics = Some(catalog::TableStatistics {
        num_rows: Some(rows),
        ..Default::default()
    });
    LogicalPlan::Scan {
        table: Arc::new(metadata),
        projection: None,
        filters: vec![predicate()],
        schema,
    }
}

pub(super) fn join(left: LogicalPlan, right: LogicalPlan) -> LogicalPlan {
    let schema = Box::new(VeinSchema::new(
        left.schema()
            .fields
            .iter()
            .chain(&right.schema().fields)
            .cloned()
            .collect(),
    ));
    LogicalPlan::Join {
        join_type: parser::ast::pipe_op::JoinType::Inner,
        left: Box::new(left),
        right: Box::new(right),
        on: None,
        using: vec![],
        schema,
    }
}
