use super::*;
use plan::{ColumnId, ResolvedColumn};

fn int_column() -> LogicalExpr {
    LogicalExpr::Column(std::sync::Arc::new(ResolvedColumn {
        id: ColumnId(1),
        name: "id".to_string(),
        qualifier: None,
        data_type: VeinDataType::Int64,
        is_factorized: false,
        nullable: true,
    }))
}

fn bool_column() -> LogicalExpr {
    LogicalExpr::Column(std::sync::Arc::new(ResolvedColumn {
        id: ColumnId(2),
        name: "active".to_string(),
        qualifier: None,
        data_type: VeinDataType::Boolean,
        is_factorized: false,
        nullable: true,
    }))
}

fn binary(
    left: LogicalExpr,
    op: BinaryOp,
    right: LogicalExpr,
    return_type: VeinDataType,
) -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
        return_type,
    }
}

fn int(value: i64) -> LogicalExpr {
    LogicalExpr::Literal(LogicalLiteral::Int64(value))
}

fn boolean(value: bool) -> LogicalExpr {
    LogicalExpr::Literal(LogicalLiteral::Boolean(value))
}

#[test]
fn folds_integer_identities() {
    assert_eq!(
        simplify_expr(&binary(
            int_column(),
            BinaryOp::Plus,
            int(0),
            VeinDataType::Int64,
        )),
        int_column()
    );
    assert_eq!(
        simplify_expr(&binary(
            int(1),
            BinaryOp::Multiply,
            int_column(),
            VeinDataType::Int64,
        )),
        int_column()
    );
}

#[test]
fn folds_nullable_boolean_identities() {
    assert_eq!(
        simplify_expr(&binary(
            bool_column(),
            BinaryOp::And,
            boolean(true),
            VeinDataType::Boolean,
        )),
        bool_column()
    );
    assert_eq!(
        simplify_expr(&binary(
            bool_column(),
            BinaryOp::And,
            boolean(false),
            VeinDataType::Boolean,
        )),
        boolean(false)
    );
    assert_eq!(
        simplify_expr(&binary(
            bool_column(),
            BinaryOp::Or,
            boolean(true),
            VeinDataType::Boolean,
        )),
        boolean(true)
    );
}

#[test]
fn folds_boolean_comparisons_and_double_not() {
    assert_eq!(
        simplify_expr(&binary(
            bool_column(),
            BinaryOp::Eq,
            boolean(false),
            VeinDataType::Boolean,
        )),
        LogicalExpr::Not {
            expr: Box::new(bool_column()),
        }
    );
    assert_eq!(
        simplify_expr(&LogicalExpr::Not {
            expr: Box::new(LogicalExpr::Not {
                expr: Box::new(bool_column()),
            }),
        }),
        bool_column()
    );
}

#[test]
fn removes_only_redundant_casts() {
    let cast = LogicalExpr::Cast {
        expr: Box::new(int_column()),
        target_type: VeinDataType::Int64,
    };
    assert_eq!(simplify_expr(&cast), int_column());

    let cast = LogicalExpr::Cast {
        expr: Box::new(int_column()),
        target_type: VeinDataType::Float64,
    };
    assert_eq!(simplify_expr(&cast), cast);
}
