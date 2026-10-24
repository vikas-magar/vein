use super::super::{equality_keys, extract_equi_join_keys};
use super::support::{equality, join_key_schemas, test_column};
use parser::ast::expr::BinaryOp;
use plan::schema::{VeinDataType, VeinSchema};
use plan::{LogicalExpr, LogicalLiteral};
use std::sync::Arc;

#[test]
fn expression_only_join_equality_remains_an_exact_residual() {
    let probe_value = test_column(12, "probe_value");
    let build_value = test_column(22, "build_value");
    let probe = VeinSchema::new(vec![Arc::clone(&probe_value)]);
    let build = VeinSchema::new(vec![Arc::clone(&build_value)]);
    let expression_equality = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(probe_value)),
            op: BinaryOp::Plus,
            right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(1))),
            return_type: VeinDataType::Int64,
        }),
        op: BinaryOp::Eq,
        right: Box::new(LogicalExpr::Column(build_value)),
        return_type: VeinDataType::Boolean,
    };

    let ((probe_keys, build_keys), residual) =
        extract_equi_join_keys(&[&expression_equality], &probe, &build).unwrap();
    assert!(probe_keys.is_empty() && build_keys.is_empty());
    assert_eq!(residual, vec![expression_equality]);
}

#[test]
fn non_equality_join_uses_bounded_residual_candidates() {
    let probe_value = test_column(12, "probe_value");
    let build_value = test_column(22, "build_value");
    let probe = VeinSchema::new(vec![Arc::clone(&probe_value)]);
    let build = VeinSchema::new(vec![Arc::clone(&build_value)]);
    let non_equi = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(probe_value)),
        op: BinaryOp::Gt,
        right: Box::new(LogicalExpr::Column(build_value)),
        return_type: VeinDataType::Boolean,
    };

    let ((probe_keys, build_keys), residual) =
        extract_equi_join_keys(&[&non_equi], &probe, &build).unwrap();
    assert!(probe_keys.is_empty() && build_keys.is_empty());
    assert_eq!(residual, vec![non_equi]);
}

#[test]
fn rejects_equality_keys_from_the_same_input_side() {
    let (probe, build, probe_key, _) = join_key_schemas();

    let error = equality_keys(
        Some(&equality(Arc::clone(&probe_key), probe_key)),
        &probe,
        &build,
    )
    .expect_err("same-side equality cannot be a hash join key");

    assert_eq!(error.category(), error::ErrorCategory::Planning);
    let message = error.to_string();
    assert!(message.contains("key#11"), "{message}");
    assert!(
        message.contains("must resolve to one column from each input"),
        "{message}"
    );
    assert!(
        message.contains("probe schema [other#10, key#11]"),
        "{message}"
    );
    assert!(
        message.contains("build schema [key#21, other#22]"),
        "{message}"
    );
}
