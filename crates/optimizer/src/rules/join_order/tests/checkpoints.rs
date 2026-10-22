use super::*;
use super::super::diagnostics::relation_order_labels;

#[test]
fn checkpoint_distribution_reorders_the_remaining_joins_without_changing_population() {
    use common::statistics::QueryStatisticsContext;
    let a = scan("a", 1000, &[(1, "a_key", 1)]);
    let b = scan("b", 1000, &[(2, "b_a", 1), (3, "b_c", 1000)]);
    let c = scan("c", 10000, &[(4, "c_key", 100)]);
    let bc = join(b, c, equality(3, "b_c", 4, "c_key"), JoinType::Inner);
    let original = join(a.clone(), bc.clone(), equality(1, "a_key", 2, "b_a"), JoinType::Inner);
    let context = Arc::new(QueryStatisticsContext::new([]));
    let (_, before) = OptimizerPipeline::default()
        .optimize_with_statistics_context(original, Arc::clone(&context)).unwrap();
    assert!(!before.join_order_decisions.iter().any(|decision| decision.outcome == JoinOrderOutcome::Reordered));

    // Same row count, different observed key distribution. The synopsis
    // belongs only to this materialized population and uses column IDs.
    context.record_intermediate(17, Arc::new(TableStatistics {
        num_rows: Some(1000),
        column_statistics: std::collections::HashMap::from([("1".into(), ColumnStatistics {
            distinct_count: Some(200),
            null_count: Some(0),
            ..Default::default()
        })]),
        ..Default::default()
    })).unwrap();
    let observed = LogicalPlan::Intermediate {
        source_id: 17,
        row_count: 1000,
        schema: Box::new(a.schema().clone()),
    };
    let provider = Arc::new(crate::QueryStatisticsProvider::new(Arc::clone(&context)));
    let estimates = OptimizerContext::with_statistics_provider(OptimizerConfig::default(), provider);
    assert_eq!(estimates.estimate_relation(&observed).columns[&ColumnId(1)].distinct, Some(200.0));
    let remaining = join(observed, bc, equality(1, "a_key", 2, "b_a"), JoinType::Inner);
    let expected_schema = remaining.schema().clone();
    let (optimized, after) = OptimizerPipeline::default()
        .optimize_with_statistics_context(remaining, context).unwrap();
    let decision = after.join_order_decisions.iter()
        .find(|decision| decision.outcome == JoinOrderOutcome::Reordered)
        .expect("observed key distribution must change the remaining join order");
    assert!(decision.selected_cost < decision.original_cost);
    assert_eq!(optimized.schema(), &expected_schema);
}

#[test]
fn mismatched_checkpoint_population_does_not_reuse_its_column_synopses() {
    let source = scan("a", 100, &[(1, "key", 10)]);
    let context = Arc::new(common::statistics::QueryStatisticsContext::new([]));
    context.record_intermediate(17, Arc::new(TableStatistics {
        num_rows: Some(8),
        column_statistics: std::collections::HashMap::from([
            ("1".into(), ColumnStatistics { distinct_count: Some(2), null_count: Some(0), ..Default::default() }),
        ]),
        ..Default::default()
    })).unwrap();
    let provider = Arc::new(crate::QueryStatisticsProvider::new(context));
    let context = OptimizerContext::with_statistics_provider(OptimizerConfig::default(), provider);
    let input = LogicalPlan::Intermediate { source_id: 17, row_count: 9, schema: Box::new(source.schema().clone()) };
    let estimate = context.estimate_relation(&input);
    assert_eq!(estimate.rows.value(), Some(9.0));
    assert_ne!(estimate.columns[&ColumnId(1)].distinct, Some(2.0));
}

#[test]
fn binary_join_runs_cost_search_and_reports_its_candidate() {
    let plan = join(
        scan("orders", 100, &[(1, "order_key", 100)]),
        scan("lineitem", 400, &[(2, "order_key", 200)]),
        equality(1, "order_key", 2, "order_key"),
        JoinType::Inner,
    );
    let original = plan.clone();

    let (optimized, diagnostics) = optimizer(JoinOrderConfig::default())
        .optimize_with_diagnostics(plan)
        .expect("binary join should be retained");

    assert_eq!(optimized, original);
    let decision = diagnostics
        .join_order_decisions
        .first()
        .expect("binary join decision should be visible");
    assert_eq!(decision.outcome, JoinOrderOutcome::NoImprovement);
    assert_eq!(decision.relation_labels, vec!["orders", "lineitem"]);
    assert_eq!(decision.candidate_splits, 1);
    assert!(decision.original_cost.is_some());
    assert!(decision.selected_cost.is_some());
    assert_eq!(
        relation_order_labels(&decision.original_order, &decision.relation_labels),
        vec!["orders", "lineitem"]
    );
}

#[test]
fn pruned_join_output_schema_preserves_original_relation_lineage_for_costing() {
    let a = scan("a", 100, &[(1, "a_key", 10)]);
    let b = scan("b", 100, &[(2, "b_key", 10), (3, "b_c", 10)]);
    let c = scan("c", 100, &[(4, "c_key", 10)]);
    let mut ab = join(
        a,
        b,
        equality(1, "a_key", 2, "b_key"),
        JoinType::Inner,
    );
    let LogicalPlan::Join { schema, .. } = &mut ab else {
        unreachable!("helper creates a join");
    };
    schema.fields.retain(|column| column.id == ColumnId(3));

    let plan = join(
        ab,
        c,
        equality(3, "b_c", 4, "c_key"),
        JoinType::Inner,
    );
    let (_, diagnostics) = optimizer(JoinOrderConfig::default())
        .optimize_with_diagnostics(plan)
        .expect("column pruning must not hide an original relation from costing");

    let decision = diagnostics
        .join_order_decisions
        .first()
        .expect("the three-relation inner join is eligible for costing");
    assert_eq!(decision.relation_count, 3);
    assert!(decision.original_cost.is_some());
    assert_ne!(decision.outcome, JoinOrderOutcome::InvalidCost);
}
