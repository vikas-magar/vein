use super::super::extract_equi_join_keys;
use super::support::{equality, test_column};
use parser::ast::expr::BinaryOp;
use plan::schema::{ColumnId, ResolvedColumn, VeinDataType, VeinSchema};
use plan::{LogicalExpr, LogicalLiteral};
use std::sync::Arc;

#[test]
fn extracts_composite_equi_join_keys_and_keeps_other_conjuncts_residual() {
    let probe_key = test_column(11, "probe_key");
    let probe_region = test_column(12, "probe_region");
    let probe_value = test_column(10, "probe_value");
    let build_key = test_column(21, "build_key");
    let build_region = test_column(23, "build_region");
    let build_value = test_column(22, "build_value");
    let probe = VeinSchema::new(vec![
        Arc::clone(&probe_value),
        Arc::clone(&probe_key),
        Arc::clone(&probe_region),
    ]);
    let build = VeinSchema::new(vec![
        Arc::clone(&build_key),
        Arc::clone(&build_value),
        Arc::clone(&build_region),
    ]);
    let residual = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(probe_value)),
        op: parser::ast::expr::BinaryOp::Gt,
        right: Box::new(LogicalExpr::Column(build_value)),
        return_type: VeinDataType::Boolean,
    };
    let conjuncts = [
        equality(Arc::clone(&probe_key), Arc::clone(&build_key)),
        equality(Arc::clone(&build_region), Arc::clone(&probe_region)),
        residual.clone(),
    ];
    let conjunct_refs: Vec<_> = conjuncts.iter().collect();

    let ((probe_keys, build_keys), residuals) =
        extract_equi_join_keys(&conjunct_refs, &probe, &build)
            .expect("both cross-input equalities should be hash keys");

    assert_eq!(probe_keys, vec![1, 2]);
    assert_eq!(build_keys, vec![0, 2]);
    assert_eq!(residuals, vec![residual]);
}

#[test]
fn keeps_same_side_equality_residual_when_extracting_join_keys() {
    let probe_key = test_column(11, "probe_key");
    let probe_value = test_column(10, "probe_value");
    let build_key = test_column(21, "build_key");
    let probe = VeinSchema::new(vec![Arc::clone(&probe_value), Arc::clone(&probe_key)]);
    let build = VeinSchema::new(vec![Arc::clone(&build_key)]);
    let same_side = equality(Arc::clone(&probe_value), Arc::clone(&probe_key));
    let cross_side = equality(Arc::clone(&build_key), Arc::clone(&probe_key));
    let conjuncts = [same_side.clone(), cross_side];
    let conjunct_refs: Vec<_> = conjuncts.iter().collect();

    let ((probe_keys, build_keys), residuals) =
        extract_equi_join_keys(&conjunct_refs, &probe, &build)
            .expect("cross-input equality should provide the hash key");

    assert_eq!(probe_keys, vec![1]);
    assert_eq!(build_keys, vec![0]);
    assert_eq!(residuals, vec![same_side]);
}
#[test]
fn non_hashable_equality_remains_an_exact_residual() {
    let probe_key = Arc::new(ResolvedColumn::new(
        ColumnId(31),
        "probe_key",
        None,
        VeinDataType::Boolean,
    ));
    let build_key = Arc::new(ResolvedColumn::new(
        ColumnId(41),
        "build_key",
        None,
        VeinDataType::Boolean,
    ));
    let probe = VeinSchema::new(vec![Arc::clone(&probe_key)]);
    let build = VeinSchema::new(vec![Arc::clone(&build_key)]);
    let conjunct = equality(probe_key, build_key);

    let ((probe_keys, build_keys), residual) =
        extract_equi_join_keys(&[&conjunct], &probe, &build).unwrap();
    assert!(probe_keys.is_empty() && build_keys.is_empty());
    assert_eq!(residual, vec![conjunct]);
}
#[test]
fn int32_equality_is_extracted_as_hash_key_alongside_int64() {
    let probe_key = test_column(11, "probe_key");
    let build_key = test_column(21, "build_key");
    let probe_i32 = Arc::new(ResolvedColumn::new(
        ColumnId(12),
        "probe_i32",
        None,
        VeinDataType::Int32,
    ));
    let build_i32 = Arc::new(ResolvedColumn::new(
        ColumnId(22),
        "build_i32",
        None,
        VeinDataType::Int32,
    ));
    let probe = VeinSchema::new(vec![Arc::clone(&probe_key), Arc::clone(&probe_i32)]);
    let build = VeinSchema::new(vec![Arc::clone(&build_key), Arc::clone(&build_i32)]);
    let supported = equality(probe_key, build_key);
    let int32_equality = equality(probe_i32, build_i32);

    let ((probe_keys, build_keys), residuals) =
        extract_equi_join_keys(&[&supported, &int32_equality], &probe, &build)
            .expect("both supported equalities should become hash keys");

    assert_eq!(probe_keys, vec![0, 1]);
    assert_eq!(build_keys, vec![0, 1]);
    assert!(residuals.is_empty());
}

#[test]
fn expression_equality_remains_residual_when_a_supported_hash_key_exists() {
    let probe_key = test_column(11, "probe_key");
    let probe_value = test_column(12, "probe_value");
    let build_key = test_column(21, "build_key");
    let build_value = test_column(22, "build_value");
    let probe = VeinSchema::new(vec![Arc::clone(&probe_key), Arc::clone(&probe_value)]);
    let build = VeinSchema::new(vec![Arc::clone(&build_key), Arc::clone(&build_value)]);
    let supported = equality(Arc::clone(&probe_key), Arc::clone(&build_key));
    let expression_equality = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(probe_value)),
            op: BinaryOp::Plus,
            right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(1))),
            return_type: VeinDataType::Int64,
        }),
        op: BinaryOp::Eq,
        right: Box::new(LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(build_value)),
            op: BinaryOp::Plus,
            right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(1))),
            return_type: VeinDataType::Int64,
        }),
        return_type: VeinDataType::Boolean,
    };

    let ((probe_keys, build_keys), residuals) =
        extract_equi_join_keys(&[&supported, &expression_equality], &probe, &build)
            .expect("the direct equality supplies a hash key");

    assert_eq!(probe_keys, vec![0]);
    assert_eq!(build_keys, vec![0]);
    assert_eq!(residuals, vec![expression_equality]);
}
