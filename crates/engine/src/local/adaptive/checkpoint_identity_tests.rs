//! Regressions for stable adaptive scan-population identity.

use super::super::super::test_support::scan;
use super::{scan_population_signature, signature_match_counts};
use parser::ast::expr::BinaryOp;
use plan::{ColumnId, LogicalExpr, LogicalPlan, ResolvedColumn, VeinDataType};
use std::sync::Arc;

fn aliased_scan(id: usize, qualifier: &str) -> LogicalPlan {
    let mut plan = scan("nation", 25);
    let LogicalPlan::Scan { filters, schema, .. } = &mut plan else { unreachable!() };
    let column = Arc::new(ResolvedColumn::new(ColumnId(id), "id", Some(qualifier.to_owned()), VeinDataType::Int64));
    schema.fields[0] = Arc::clone(&column);
    filters[0] = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(column)),
        op: BinaryOp::Gt,
        right: Box::new(LogicalExpr::Literal(plan::LogicalLiteral::Int64(0))),
        return_type: VeinDataType::Boolean,
    };
    plan
}

#[test]
fn scan_identity_ignores_alias_column_ids_and_qualifiers() {
    let first = scan_population_signature(&scan("nation", 25)).unwrap();
    let alias = scan_population_signature(&aliased_scan(7, "nation_alias")).unwrap();
    assert_eq!(first, alias);
}

#[test]
fn scan_identity_canonicalizes_reordered_commutative_filters() {
    let first = scan_population_signature(&country_scan("FRANCE", "GERMANY")).unwrap();
    let reversed = scan_population_signature(&country_scan("GERMANY", "FRANCE")).unwrap();
    assert_eq!(first, reversed);
}

#[test]
fn scan_identity_preserves_filter_semantics_and_source_identity() {
    let first = scan_population_signature(&scan("nation", 25)).unwrap();
    let different_filter = scan_population_signature(&with_filter_bound(
        aliased_scan(7, "nation_alias"),
        1,
    ))
        .unwrap();
    assert_ne!(first, different_filter);

    let mut changed_source = scan("nation", 25);
    if let LogicalPlan::Scan { table, .. } = &mut changed_source {
        let mut metadata = (**table).clone();
        metadata.files.push("different-source.parquet".into());
        *table = Arc::new(metadata);
    }
    assert_ne!(first, scan_population_signature(&changed_source).unwrap());

    let mut changed_provider = scan("nation", 25);
    if let LogicalPlan::Scan { table, .. } = &mut changed_provider {
        let mut metadata = (**table).clone();
        metadata.table_provider = Some("provider.snapshot.v1".into());
        *table = Arc::new(metadata);
    }
    assert_ne!(first, scan_population_signature(&changed_provider).unwrap());
}

#[test]
fn scan_identity_includes_the_emitted_projection() {
    let mut projected = aliased_scan(0, "");
    {
        let LogicalPlan::Scan { projection, schema, .. } = &mut projected else { unreachable!() };
        schema.fields.push(Arc::new(ResolvedColumn::new(
            ColumnId(1), "payload", None, VeinDataType::Utf8,
        )));
        *projection = Some(vec![ColumnId(0)]);
    }
    let selected = scan_population_signature(&projected).unwrap();
    if let LogicalPlan::Scan { projection, .. } = &mut projected {
        *projection = Some(vec![ColumnId(0), ColumnId(1)]);
    }
    let expanded = scan_population_signature(&projected).unwrap();
    assert_ne!(selected, expanded);
}

#[test]
fn signature_match_counts_locate_source_filter_and_projection_differences() {
    let first = scan_population_signature(&scan("nation", 25)).unwrap();
    let different_filter = scan_population_signature(&with_filter_bound(
        aliased_scan(7, "nation_alias"),
        1,
    ))
        .unwrap();
    let counts = signature_match_counts(&different_filter, &[first.clone()]);
    assert_eq!(counts.same_source, 1);
    assert_eq!(counts.same_filters, 0);
    assert_eq!(counts.same_projection, 1);
    assert_eq!(counts.exact, 0);

    let counts = signature_match_counts(&first, &[first.clone()]);
    assert_eq!(counts.exact, 1);
}

fn with_filter_bound(mut plan: LogicalPlan, bound: i64) -> LogicalPlan {
    if let LogicalPlan::Scan { filters, .. } = &mut plan {
        if let LogicalExpr::BinaryOp { right, .. } = &mut filters[0] {
            *right = Box::new(LogicalExpr::Literal(plan::LogicalLiteral::Int64(bound)));
        }
    }
    plan
}

fn country_scan(first: &str, second: &str) -> LogicalPlan {
    let mut plan = scan("nation", 25);
    let LogicalPlan::Scan { filters, schema, .. } = &mut plan else { unreachable!() };
    let column = Arc::new(ResolvedColumn::new(
        ColumnId(0), "n_name", Some("nation_alias".to_owned()), VeinDataType::Utf8,
    ));
    schema.fields[0] = Arc::clone(&column);
    let equals = |country: &str| LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(Arc::clone(&column))),
        op: BinaryOp::Eq,
        right: Box::new(LogicalExpr::Literal(plan::LogicalLiteral::String(country.to_owned()))),
        return_type: VeinDataType::Boolean,
    };
    filters[0] = LogicalExpr::BinaryOp {
        left: Box::new(equals(first)),
        op: BinaryOp::Or,
        right: Box::new(equals(second)),
        return_type: VeinDataType::Boolean,
    };
    plan
}
