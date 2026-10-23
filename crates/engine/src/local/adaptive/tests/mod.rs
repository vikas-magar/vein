//! Checkpoint policy and replacement regression tests.

mod checkpoint_identity;
mod policy_eligibility;
mod policy_selection;
mod replay_safety;
mod support;
mod cost;

use super::*;
use catalog::TableMetadata;
use plan::{ColumnId, ResolvedColumn, VeinDataType, VeinSchema};
use support::{join, predicate, scan};

#[test]
fn selects_smallest_estimated_unique_scan_without_table_name_rules() {
    let plan = join(scan("arbitrary_left", 12), scan("arbitrary_right", 120));
    let boundary = StatisticsCheckpointPolicy
        .choose_for_test(&plan)
        .expect("known unique scan should be eligible");
    let LogicalPlan::Scan {
        table, projection, ..
    } = boundary.producer
    else {
        panic!("producer should be a scan");
    };
    assert_eq!(table.name, "arbitrary_left");
    assert!(
        projection.is_none(),
        "checkpoint producer must expose its schema"
    );
}

#[test]
fn replacement_uses_observed_rows_and_preserves_schema() {
    let plan = join(scan("a", 10), scan("b", 20));
    let boundary = StatisticsCheckpointPolicy.choose_for_test(&plan).unwrap();
    let source = IntermediateSourceId::new();
    let rewritten = boundary.replace(plan, source, 3).unwrap();
    let LogicalPlan::Join { left, right, .. } = rewritten else {
        panic!("join root must remain in the remaining plan");
    };
    let LogicalPlan::Intermediate {
        source_id,
        row_count,
        schema,
    } = *left
    else {
        panic!("selected scan should become an intermediate source");
    };
    assert_eq!(source_id, source.into_raw());
    assert_eq!(row_count, 3);
    assert_eq!(schema.fields.len(), 1);
    assert!(matches!(*right, LogicalPlan::Scan { .. }));
}

#[test]
fn checkpoint_producer_preserves_projection_and_intermediate_schema() {
    let fields = vec![
        Arc::new(ResolvedColumn::new(
            ColumnId(0),
            "id",
            None,
            VeinDataType::Int64,
        )),
        Arc::new(ResolvedColumn::new(
            ColumnId(1),
            "payload",
            None,
            VeinDataType::Utf8,
        )),
    ];
    let schema = Box::new(VeinSchema::new(fields));
    let mut metadata = TableMetadata::new("projected", Arc::new(*schema.clone()))
        .with_file_format("parquet");
    metadata.statistics = Some(catalog::TableStatistics {
        num_rows: Some(100),
        ..Default::default()
    });
    let projected = LogicalPlan::Scan {
        table: Arc::new(metadata),
        projection: Some(vec![ColumnId(0)]),
        filters: vec![predicate()],
        schema,
    };
    let plan = join(projected, scan("other", 1_000));

    let boundary = StatisticsCheckpointPolicy
        .choose_for_test(&plan)
        .expect("projected scan should be eligible");
    let LogicalPlan::Scan { projection, .. } = &boundary.producer else {
        panic!("producer should remain a scan");
    };
    assert_eq!(projection.as_deref(), Some(&[ColumnId(0)][..]));

    let rewritten = boundary
        .replace(plan, IntermediateSourceId::new(), 25)
        .expect("replace projected scan");
    let LogicalPlan::Join { left, .. } = rewritten else {
        panic!("join root must remain in the continuation");
    };
    let LogicalPlan::Intermediate {
        schema, row_count, ..
    } = *left
    else {
        panic!("selected scan should become an intermediate source");
    };
    assert_eq!(row_count, 25);
    assert_eq!(schema.fields.len(), 1);
    assert_eq!(schema.fields[0].name, "id");
}

#[test]
fn declines_plans_without_join_or_catalog_cardinality() {
    assert!(
        StatisticsCheckpointPolicy
            .choose_for_test(&scan("single", 10))
            .is_none()
    );
    assert!(
        StatisticsCheckpointPolicy
            .choose_for_test(&join(scan("known", 10), scan("empty", 0)))
            .is_some()
    );
    let unknown = |name| LogicalPlan::Scan {
        table: Arc::new(TableMetadata::new(name, Arc::new(VeinSchema::empty()))),
        projection: None,
        filters: vec![],
        schema: Box::new(VeinSchema::empty()),
    };
    assert!(
        StatisticsCheckpointPolicy
            .choose_for_test(&join(unknown("unknown"), unknown("also_unknown")))
            .is_none()
    );
    assert!(
        StatisticsCheckpointPolicy
            .choose_for_test(&join(unknown("unknown"), scan("empty", 0)))
            .is_some(),
        "known zero rows must remain distinct from unknown cardinality"
    );
}

#[test]
fn known_unfiltered_tables_do_not_add_materialization_barriers() {
    let unfiltered = |name| {
        let mut plan = scan(name, 100);
        if let LogicalPlan::Scan { filters, .. } = &mut plan {
            filters.clear();
        }
        plan
    };
    assert!(
        StatisticsCheckpointPolicy
            .choose_for_test(&join(unfiltered("a"), unfiltered("b")))
            .is_none()
    );
}

#[test]
fn checkpoint_replacement_distinguishes_repeated_table_occurrences() {
    let source = scan("same_table", 100);
    let plan = join(source.clone(), source);
    let boundary = StatisticsCheckpointPolicy.choose_for_test(&plan).unwrap();
    let rewritten = boundary
        .replace(plan, IntermediateSourceId::new(), 3)
        .unwrap();
    let LogicalPlan::Join { left, right, .. } = rewritten else {
        panic!("join remains");
    };
    assert!(matches!(
        *left,
        LogicalPlan::Intermediate { row_count: 3, .. }
    ));
    assert!(matches!(*right, LogicalPlan::Scan { .. }));
}

#[test]
fn inferred_filters_over_completed_relations_do_not_create_repeated_checkpoints() {
    let complete = LogicalPlan::Intermediate {
        source_id: 17,
        row_count: 100,
        schema: Box::new(scan("a", 100).schema().clone()),
    };
    let filtered = LogicalPlan::Filter { input: Box::new(complete), predicate: predicate() };
    let mut other = scan("b", 100);
    if let LogicalPlan::Scan { filters, .. } = &mut other { filters.clear(); }
    assert!(StatisticsCheckpointPolicy.choose_for_test(&join(filtered, other)).is_none());
}
