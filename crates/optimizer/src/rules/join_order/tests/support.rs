use std::sync::Arc;

use parser::ast::{expr::BinaryOp, pipe_op::JoinType};
use plan::{
    ColumnId, ColumnStatistics, LogicalExpr, LogicalPlan, ResolvedColumn,
    TableMetadata, TableStatistics, VeinDataType, VeinSchema,
};

use super::*;
use crate::{OptimizerConfig, OptimizerPipeline};

pub(super) fn scan(name: &str, rows: u64, columns: &[(usize, &str, u64)]) -> LogicalPlan {
    let fields = columns
        .iter()
        .map(|(id, name, _)| {
            Arc::new(ResolvedColumn::new(
                ColumnId(*id),
                *name,
                None,
                VeinDataType::Int64,
            ))
        })
        .collect::<Vec<_>>();
    let schema = VeinSchema::new(fields);
    let column_statistics = columns
        .iter()
        .map(|(_, name, distinct)| {
            (
                (*name).to_string(),
                ColumnStatistics {
                    distinct_count: Some(*distinct),
                    null_count: Some(0),
                    ..Default::default()
                },
            )
        })
        .collect();
    let statistics = TableStatistics {
        num_rows: Some(rows),
        column_statistics,
        ..Default::default()
    };
    LogicalPlan::Scan {
        table: Arc::new(
            TableMetadata::new(name, Arc::new(schema.clone())).with_statistics(statistics),
        ),
        projection: None,
        filters: Vec::new(),
        schema: Box::new(schema),
    }
}

pub(super) fn scan_without_statistics(name: &str, columns: &[(usize, &str)]) -> LogicalPlan {
    let fields = columns
        .iter()
        .map(|(id, name)| {
            Arc::new(ResolvedColumn::new(
                ColumnId(*id),
                *name,
                None,
                VeinDataType::Int64,
            ))
        })
        .collect::<Vec<_>>();
    let schema = VeinSchema::new(fields);
    LogicalPlan::Scan {
        table: Arc::new(TableMetadata::new(name, Arc::new(schema.clone()))),
        projection: None,
        filters: Vec::new(),
        schema: Box::new(schema),
    }
}

pub(super) fn equality(left: usize, left_name: &str, right: usize, right_name: &str) -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(Arc::new(ResolvedColumn::new(
            ColumnId(left),
            left_name,
            None,
            VeinDataType::Int64,
        )))),
        op: BinaryOp::Eq,
        right: Box::new(LogicalExpr::Column(Arc::new(ResolvedColumn::new(
            ColumnId(right),
            right_name,
            None,
            VeinDataType::Int64,
        )))),
        return_type: VeinDataType::Boolean,
    }
}

pub(super) fn join(
    left: LogicalPlan,
    right: LogicalPlan,
    on: LogicalExpr,
    kind: JoinType,
) -> LogicalPlan {
    let schema = VeinSchema::new(
        left.schema()
            .fields
            .iter()
            .chain(&right.schema().fields)
            .cloned()
            .collect(),
    );
    LogicalPlan::Join {
        join_type: kind,
        left: Box::new(left),
        right: Box::new(right),
        on: Some(on),
        using: Vec::new(),
        schema: Box::new(schema),
    }
}

pub(super) fn selective_chain() -> LogicalPlan {
    let a = scan("a", 1_000_000, &[(1, "a_key", 1_000_000)]);
    let b = scan("b", 100, &[(2, "b_a_key", 100), (3, "b_c_key", 100)]);
    let c = scan("c", 10, &[(4, "c_key", 10)]);
    let ab = join(a, b, equality(1, "a_key", 2, "b_a_key"), JoinType::Inner);
    join(
        ab,
        c,
        equality(4, "c_key", 3, "b_c_key"),
        JoinType::Inner,
    )
}

pub(super) fn conjunction(left: LogicalExpr, right: LogicalExpr) -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(left),
        op: BinaryOp::And,
        right: Box::new(right),
        return_type: VeinDataType::Boolean,
    }
}

pub(super) fn join_equality_count(plan: &LogicalPlan) -> usize {
    let mut pending = vec![plan];
    let mut count = 0;
    while let Some(node) = pending.pop() {
        if let LogicalPlan::Join { on: Some(on), .. } = node {
            count += equi_atoms(on).map_or(0, |atoms| atoms.len());
        }
        pending.extend(node.children());
    }
    count
}

pub(super) fn optimizer(config: JoinOrderConfig) -> OptimizerPipeline {
    let optimizer_config = OptimizerConfig {
        join_order: config,
        ..OptimizerConfig::default()
    };
    OptimizerPipeline::new(vec![Arc::new(JoinOrderRule::default())])
        .with_config(optimizer_config)
}
