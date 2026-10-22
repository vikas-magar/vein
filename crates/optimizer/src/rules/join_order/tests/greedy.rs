use super::*;

#[test]
fn exact_search_budget_can_fall_back_to_a_complete_cheaper_greedy_tree() {
    let plan = selective_chain();
    let expected_schema = plan.schema().clone();
    let (optimized, diagnostics) = optimizer(JoinOrderConfig {
        max_relations: 8,
        max_candidate_splits: 3,
    }).optimize_with_diagnostics(plan).unwrap();
    let decision = &diagnostics.join_order_decisions[0];
    assert_eq!(decision.outcome, JoinOrderOutcome::Reordered);
    assert!(decision.selected_cost < decision.original_cost);
    assert!(decision.candidate_splits <= 3);
    assert_eq!(join_equality_count(&optimized), 2);
    assert_eq!(optimized.schema(), &expected_schema);
}

#[test]
fn greedy_search_preserves_shared_table_metadata_handles() {
    fn tables(plan: &LogicalPlan) -> Vec<Arc<plan::TableMetadata>> {
        let mut pending = vec![plan];
        let mut tables = Vec::new();
        while let Some(node) = pending.pop() {
            if let LogicalPlan::Scan { table, .. } = node { tables.push(Arc::clone(table)); }
            pending.extend(node.children());
        }
        tables.sort_by(|a, b| a.name.cmp(&b.name));
        tables
    }
    let plan = selective_chain();
    let original = tables(&plan);
    let (optimized, _) = optimizer(JoinOrderConfig { max_relations: 2, max_candidate_splits: 100 })
        .optimize_with_diagnostics(plan).unwrap();
    for (before, after) in original.iter().zip(tables(&optimized)) {
        assert!(Arc::ptr_eq(before, &after));
    }
}

#[test]
fn greedy_search_with_unknown_statistics_keeps_the_original_tree() {
    let a = scan("a", 1000, &[(1, "a_key", 100)]);
    let b = scan_without_statistics("b", &[(2, "b_key")]);
    let c = scan("c", 10, &[(3, "c_key", 10)]);
    let plan = join(join(a, b, equality(1, "a_key", 2, "b_key"), JoinType::Inner),
                    c, equality(2, "b_key", 3, "c_key"), JoinType::Inner);
    let original = plan.clone();
    let (optimized, diagnostics) = optimizer(JoinOrderConfig { max_relations: 2, max_candidate_splits: 100 })
        .optimize_with_diagnostics(plan).unwrap();
    assert_eq!(optimized, original);
    assert_eq!(diagnostics.join_order_decisions[0].outcome, JoinOrderOutcome::InsufficientStatistics);
}
