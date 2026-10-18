use plan::schema::{ColumnId, ResolvedColumn, VeinDataType, VeinSchema};
use plan::{ColumnStatistics, LogicalExpr, LogicalPlan, PlanTraversal, TableMetadata, TableStatistics};
use std::sync::Arc;

pub(super) fn test_column(id: usize, name: &str) -> Arc<ResolvedColumn> {
    Arc::new(ResolvedColumn::new(
        ColumnId(id),
        name,
        None,
        VeinDataType::Int64,
    ))
}

pub(super) fn equality(left: Arc<ResolvedColumn>, right: Arc<ResolvedColumn>) -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(left)),
        op: parser::ast::expr::BinaryOp::Eq,
        right: Box::new(LogicalExpr::Column(right)),
        return_type: VeinDataType::Boolean,
    }
}

pub(super) fn scan_with_statistics(
    name: &str,
    rows: u64,
    columns: &[(Arc<ResolvedColumn>, u64)],
) -> LogicalPlan {
    let fields = columns
        .iter()
        .map(|(column, _)| Arc::clone(column))
        .collect::<Vec<_>>();
    let statistics = TableStatistics {
        num_rows: Some(rows),
        column_statistics: columns
            .iter()
            .map(|(column, distinct)| {
                (
                    column.name.clone(),
                    ColumnStatistics {
                        distinct_count: Some(*distinct),
                        null_count: Some(0),
                        ..Default::default()
                    },
                )
            })
            .collect(),
        ..Default::default()
    };
    let schema = VeinSchema::new(fields);
    LogicalPlan::Scan {
        table: Arc::new(
            TableMetadata::new(name, Arc::new(schema.clone())).with_statistics(statistics),
        ),
        projection: None,
        filters: Vec::new(),
        schema: Box::new(schema),
    }
}

pub(super) fn inner_join(left: LogicalPlan, right: LogicalPlan, on: LogicalExpr) -> LogicalPlan {
    let schema = VeinSchema::new(
        left.schema()
            .fields
            .iter()
            .chain(&right.schema().fields)
            .cloned()
            .collect(),
    );
    LogicalPlan::Join {
        join_type: parser::ast::pipe_op::JoinType::Inner,
        left: Box::new(left),
        right: Box::new(right),
        on: Some(on),
        using: Vec::new(),
        schema: Box::new(schema),
    }
}

pub(super) fn join_key_schemas() -> (
    VeinSchema,
    VeinSchema,
    Arc<ResolvedColumn>,
    Arc<ResolvedColumn>,
) {
    let probe_key = test_column(11, "key");
    let build_key = test_column(21, "key");
    let probe_schema = VeinSchema::new(vec![test_column(10, "other"), Arc::clone(&probe_key)]);
    let build_schema = VeinSchema::new(vec![Arc::clone(&build_key), test_column(22, "other")]);
    (probe_schema, build_schema, probe_key, build_key)
}
