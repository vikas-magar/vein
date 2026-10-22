use super::*;

#[test]
fn selective_connected_inner_joins_choose_smaller_intermediate_and_preserve_schema() {
    let plan = selective_chain();
    let original_schema = plan.schema().clone();
    let (optimized, diagnostics) = optimizer(JoinOrderConfig::default())
        .optimize_with_diagnostics(plan)
        .expect("optimizer should select a safe alternative");

    let LogicalPlan::Join { left, right, schema, .. } = &optimized else {
        panic!("expected a join root");
    };
    assert!(matches!(left.as_ref(), LogicalPlan::Scan { table, .. } if table.name == "a"));
    assert!(matches!(
        right.as_ref(),
        LogicalPlan::Join { left, right, .. }
            if matches!(left.as_ref(), LogicalPlan::Scan { table, .. } if table.name == "b")
                && matches!(right.as_ref(), LogicalPlan::Scan { table, .. } if table.name == "c")
    ));
    assert_eq!(schema.as_ref(), &original_schema);
    assert_eq!(join_equality_count(&optimized), 2, "each input predicate must survive once");
    let decision = diagnostics.join_order_decisions.first().expect("join decision");
    assert_eq!(decision.outcome, JoinOrderOutcome::Reordered);
    assert!(decision.selected_cost.unwrap() < decision.original_cost.unwrap());
    assert_eq!(decision.relation_count, 3);
    assert!(decision.candidate_splits > 0);
    assert_eq!(
        decision.cardinality_assumption,
        JoinCardinalityAssumption::UniformDistinctValueOverlap
    );
    assert!(decision.alternatives.iter().any(|alternative| {
        alternative.relation_mask == 0b111 && alternative.best_for_subset
    }));
    assert_eq!(
        decision
            .alternatives
            .iter()
            .filter(|alternative| alternative.original_tree_split)
            .count(),
        2
    );
    assert!(decision.alternatives.iter().any(|alternative| {
        alternative.cost.local_cost == Some(alternative.output_rows)
            && (alternative.cost.left_subtree_cost
            + alternative.cost.right_subtree_cost
            + alternative.cost.local_cost.unwrap()
            == alternative.cost.cumulative_cost)
    }));
}

#[test]
fn inconsistent_decomposition_estimates_preserve_original_join_tree() {
    let a = scan("a", 1_000, &[(1, "a_ab", 100), (2, "a_ac", 1_000)]);
    let b = scan("b", 1_000, &[(3, "b_ab", 100), (4, "b_bc", 10)]);
    let c = scan("c", 1_000, &[(5, "c_bc", 10), (6, "c_ac", 1_000)]);
    let ab = join(a, b, equality(1, "a_ab", 3, "b_ab"), JoinType::Inner);
    let predicates = conjunction(
        equality(2, "a_ac", 6, "c_ac"),
        equality(4, "b_bc", 5, "c_bc"),
    );
    let plan = join(ab, c, predicates, JoinType::Inner);
    let original = plan.clone();

    let (optimized, diagnostics) = optimizer(JoinOrderConfig::default())
        .optimize_with_diagnostics(plan)
        .expect("ambiguous estimates should safely fall back");

    assert_eq!(optimized, original);
    assert_eq!(join_equality_count(&optimized), 3);
    assert_eq!(
        diagnostics.join_order_decisions[0].outcome,
        JoinOrderOutcome::AmbiguousEstimates
    );
}

#[test]
fn unknown_statistics_preserve_original_join_tree() {
    let a = scan("a", 1_000, &[(1, "a_key", 100)]);
    let b = scan_without_statistics("b", &[(2, "b_key")]);
    let c = scan("c", 10, &[(3, "c_key", 10)]);
    let ab = join(a, b, equality(1, "a_key", 2, "b_key"), JoinType::Inner);
    let plan = join(ab, c, equality(2, "b_key", 3, "c_key"), JoinType::Inner);
    let original = plan.clone();

    let (optimized, diagnostics) = optimizer(JoinOrderConfig::default())
        .optimize_with_diagnostics(plan)
        .expect("unknown estimates should safely fall back");

    assert_eq!(optimized, original);
    assert_eq!(
        diagnostics.join_order_decisions[0].outcome,
        JoinOrderOutcome::InsufficientStatistics
    );
}

#[test]
fn complete_row_group_population_supports_join_order_costing() {
    let mut a = scan("a", 100, &[(1, "a_key", 10)]);
    let LogicalPlan::Scan { table, .. } = &mut a else {
        unreachable!("scan fixture must produce a scan");
    };
    let statistics = Arc::make_mut(table)
        .statistics
        .as_mut()
        .expect("scan fixture has statistics");
    statistics.num_rows = None;
    statistics.row_group_count = Some(2);
    statistics.data_units = [40, 60]
        .into_iter()
        .enumerate()
        .map(|(ordinal, num_rows)| {
            let mut unit = DataUnitStatistics::new(StatisticsUnitKind::RowGroup, ordinal);
            unit.num_rows = Some(num_rows);
            unit
        })
        .collect();

    let b = scan("b", 200, &[(2, "b_key", 20)]);
    let plan = join(a, b, equality(1, "a_key", 2, "b_key"), JoinType::Inner);
    let original = plan.clone();

    let (optimized, diagnostics) = optimizer(JoinOrderConfig::default())
        .optimize_with_diagnostics(plan)
        .expect("complete row-group counts provide a population estimate");

    assert_eq!(optimized, original);
    let decision = diagnostics
        .join_order_decisions
        .first()
        .expect("the two-relation join must be costed");
    assert_eq!(decision.outcome, JoinOrderOutcome::NoImprovement);
    assert_eq!(decision.original_cost, decision.selected_cost);
    assert_eq!(decision.candidate_splits, 1);
}

#[test]
fn candidate_budget_exhaustion_preserves_original_tree() {
    let plan = selective_chain();
    let original = plan.clone();
    let config = JoinOrderConfig {
        max_relations: 8,
        max_candidate_splits: 0,
    };

    let (optimized, diagnostics) = optimizer(config)
        .optimize_with_diagnostics(plan)
        .expect("budget exhaustion should fall back");

    assert_eq!(optimized, original);
    let decision = diagnostics.join_order_decisions.first().expect("fallback diagnostic");
    assert_eq!(decision.outcome, JoinOrderOutcome::SearchBudgetExceeded);
    assert_eq!(decision.selected_cost, None);
}
