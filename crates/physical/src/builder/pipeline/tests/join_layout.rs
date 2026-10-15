use super::support::{equality, join_key_schemas, test_column};
use super::super::{available_join_outputs, equality_keys, join_kind};
use common::protocol::JoinKind;
use parser::ast::pipe_op::JoinType;
use plan::schema::VeinSchema;
use std::sync::Arc;

#[test]
fn parser_join_flavors_map_to_their_physical_kinds() {
    let mappings = [
        (JoinType::Inner, JoinKind::Inner),
        (JoinType::Left, JoinKind::Left),
        (JoinType::Right, JoinKind::Right),
        (JoinType::Full, JoinKind::Full),
        (JoinType::Semi, JoinKind::Semi),
        (JoinType::Anti, JoinKind::Anti),
        (JoinType::Single, JoinKind::Single),
        (JoinType::Cross, JoinKind::Cross),
    ];

    for (logical, physical) in mappings {
        assert_eq!(join_kind(&logical), physical);
    }
}

#[test]
fn resolves_probe_to_build_equality_keys() {
    let (probe, build, probe_key, build_key) = join_key_schemas();

    assert_eq!(
        equality_keys(Some(&equality(probe_key, build_key)), &probe, &build)
            .expect("probe-to-build equality should resolve"),
        (1, 0)
    );
}

#[test]
fn resolves_build_to_probe_equality_keys() {
    let (probe, build, probe_key, build_key) = join_key_schemas();

    assert_eq!(
        equality_keys(Some(&equality(build_key, probe_key)), &probe, &build)
            .expect("build-to-probe equality should resolve"),
        (1, 0)
    );
}

#[test]
fn invalid_join_key_reports_a_planning_contract_error() {
    let probe = VeinSchema::new(vec![test_column(10, "probe_key")]);
    let build = VeinSchema::new(vec![test_column(20, "build_key")]);
    let predicate = equality(test_column(99, "missing_key"), test_column(20, "build_key"));

    let error = equality_keys(Some(&predicate), &probe, &build)
        .expect_err("a join key absent from both inputs must fail planning");

    assert_eq!(error.category(), error::ErrorCategory::Planning);
    let message = error.to_string();
    assert!(message.contains("missing_key#99"), "{message}");
    assert!(message.contains("build_key#20"), "{message}");
    assert!(message.contains("probe schema [probe_key#10]"), "{message}");
    assert!(message.contains("build schema [build_key#20]"), "{message}");
    assert!(message.contains("check ON or USING"), "{message}");
}

#[test]
fn join_output_mapping_rejects_declared_columns_missing_from_both_inputs() {
    let probe_key = test_column(11, "probe_key");
    let build_key = test_column(21, "build_key");
    let missing_output = test_column(99, "required_value");
    let probe = VeinSchema::new(vec![Arc::clone(&probe_key)]);
    let build = VeinSchema::new(vec![Arc::clone(&build_key)]);
    let declared_output = VeinSchema::new(vec![missing_output]);

    let error = available_join_outputs(&declared_output, &probe, &build)
        .expect_err("a declared but unavailable output must fail planning");

    let message = error.to_string();
    assert!(message.contains("required_value"), "{message}");
    assert!(message.contains("ColumnId(99)"), "{message}");
    assert!(message.contains("both physical inputs"), "{message}");
}

#[test]
fn join_output_mapping_preserves_only_declared_pruned_outputs() {
    let probe_key = test_column(11, "probe_key");
    let probe_value = test_column(12, "probe_value");
    let build_key = test_column(21, "build_key");
    let unused_build_value = test_column(22, "unused_build_value");
    let probe = VeinSchema::new(vec![Arc::clone(&probe_key), Arc::clone(&probe_value)]);
    let build = VeinSchema::new(vec![
        Arc::clone(&build_key),
        Arc::clone(&unused_build_value),
    ]);
    let declared_output = VeinSchema::new(vec![Arc::clone(&probe_value)]);

    let (outputs, output_schema) = available_join_outputs(&declared_output, &probe, &build)
        .expect("a narrowed join output should map successfully");

    assert_eq!(outputs.len(), 1);
    assert!(matches!(outputs[0], common::protocol::JoinOutput::Probe(1)));
    assert_eq!(
        output_schema
            .fields
            .iter()
            .map(|field| field.id)
            .collect::<Vec<_>>(),
        vec![probe_value.id]
    );
    assert!(Arc::ptr_eq(&output_schema.fields[0], &probe_value));
}
