use super::support::{equality, join_key_schemas, test_column};
use super::super::{equality_keys, extract_equi_join_keys};
use parser::ast::expr::BinaryOp;
use plan::schema::{VeinDataType, VeinSchema};
use plan::{LogicalExpr, LogicalLiteral};
use std::sync::Arc;

#[test]
fn rejects_expression_only_join_equality_with_supported_boundary_error() {
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

    let error = extract_equi_join_keys(&[&expression_equality], &probe, &build)
        .expect_err("expression-only equality cannot run as a hash join");

    let message = error.to_string();
    assert!(message.contains("direct columns from opposite inputs"), "{message}");
    assert!(message.contains("expression-only and non-equi joins are not supported"), "{message}");
}

#[test]
fn rejects_pure_non_equi_join_with_supported_boundary_error() {
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

    let error = extract_equi_join_keys(&[&non_equi], &probe, &build)
        .expect_err("pure non-equi conditions need a non-hash join algorithm");

    let message = error.to_string();
    assert!(message.contains("direct columns from opposite inputs"), "{message}");
    assert!(message.contains("expression-only and non-equi joins are not supported"), "{message}");
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
    assert!(message.contains("must resolve to one column from each input"), "{message}");
    assert!(message.contains("probe schema [other#10, key#11]"), "{message}");
    assert!(message.contains("build schema [key#21, other#22]"), "{message}");
}
