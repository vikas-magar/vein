use std::sync::Arc;

use plan::{LogicalExpr as E, LogicalLiteral as L, LogicalPlan as P, TableMetadata, VeinSchema};
use super::*;

fn scan(filters: Vec<E>) -> P {
    P::Scan {
        table: Arc::new(TableMetadata::new("t", Arc::new(VeinSchema::empty()))),
        projection: None,
        filters,
        schema: Box::new(VeinSchema::empty()),
    }
}

#[test]
fn links_unchanged_predicate_after_pushdown_without_retaining_literal_data() {
    let predicate = E::Literal(L::Boolean(true));
    let input = P::Filter { input: Box::new(scan(vec![])), predicate: predicate.clone() };
    let lineage = PredicateLineage::capture(&input);
    let placements = lineage.placements(&scan(vec![predicate]));
    assert_eq!(placements.len(), 1);
    assert_eq!(placements[0].matching_input_predicates, vec![0]);
    assert_eq!(placements[0].location, PredicateLocation::Scan);
    assert_eq!(placements[0].retention, None);
}

#[test]
fn rewritten_predicates_do_not_claim_an_exact_input_origin() {
    let lineage = PredicateLineage::capture(&scan(vec![E::Literal(L::Boolean(true))]));
    let placements = lineage.placements(&scan(vec![E::Literal(L::Boolean(false))]));
    assert!(placements[0].matching_input_predicates.is_empty());
}

#[test]
fn limit_boundary_and_logical_preorder_are_visible() {
    let plan = P::Filter {
        input: Box::new(P::Limit {
            input: Box::new(scan(vec![])),
            count: 10,
            offset: 0,
            schema: Box::new(VeinSchema::empty()),
        }),
        predicate: E::Literal(L::Boolean(true)),
    };
    let placement = PredicateLineage::capture(&plan).placements(&plan).remove(0);
    assert_eq!(placement.node_id, 0);
    assert_eq!(placement.retention, Some(PredicateRetention::RowLimit));
}
