//! Regression coverage for the documented large-volume customer query.
use crate::testing::{TestCatalog, plan_with_catalog};
use plan::{ColumnId, ResolvedColumn, TableMetadata, VeinDataType, VeinSchema};
use std::sync::Arc;

fn catalog() -> Arc<TestCatalog> {
    use VeinDataType::*;
    let tables = [
        ("customer", vec![("c_custkey", Int64), ("c_name", Utf8)]),
        ("orders", vec![
            ("o_orderkey", Int64), ("o_custkey", Int64),
            ("o_orderdate", Date32), ("o_totalprice", Decimal128(15, 2)),
        ]),
        ("lineitem", vec![("l_orderkey", Int64), ("l_quantity", Decimal128(15, 2))]),
    ];
    Arc::new(TestCatalog::from_tables(tables.into_iter().map(|(name, columns)| {
        let fields = columns.into_iter().enumerate().map(|(id, (column, data_type))| {
            Arc::new(ResolvedColumn::new(ColumnId(id), column, None, data_type))
        }).collect();
        TableMetadata::new(name, Arc::new(VeinSchema::new(fields)))
    })))
}

fn documented_query() -> &'static str {
    let examples = include_str!("../../../../../example_queries.md");
    let section = examples.split("### Q18:").nth(1).expect("documented Q18");
    let section = section.split("### Q19:").next().unwrap();
    let query = section.split("-- \"").nth(1).expect("query argument");
    query.split("\n\"").next().unwrap().trim()
}

#[tokio::test]
async fn documented_q18_binds_with_explicit_membership_projection() {
    let query = documented_query();
    assert!(query.contains("|> SELECT l_orderkey"));
    plan_with_catalog(query, catalog()).await.expect("documented Q18 must bind");
}

#[tokio::test]
async fn original_q18_reports_two_column_membership_subquery() {
    let query = documented_query().replace(" |> SELECT l_orderkey", "");
    let error = plan_with_catalog(&query, catalog()).await.expect_err("two-column IN");
    let message = error.to_string();
    assert!(message.contains("IN subquery must return exactly one column"), "{message}");
    assert!(message.contains("returned 2"), "{message}");
    assert!(message.contains("explicit SELECT"), "{message}");
}
