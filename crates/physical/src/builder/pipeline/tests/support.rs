use plan::schema::{ColumnId, ResolvedColumn, VeinDataType, VeinSchema};
use plan::LogicalExpr;
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
