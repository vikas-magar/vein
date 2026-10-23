use super::*;

#[test]
fn exact_relation_limit_uses_bounded_greedy_search() {
    let plan = selective_chain();
    let original = plan.clone();
    let config = JoinOrderConfig {
        max_relations: 2,
        max_candidate_splits: 100,
    };

    let (optimized, diagnostics) = optimizer(config)
        .optimize_with_diagnostics(plan)
        .expect("relation limit should switch to approximate search");

    assert_ne!(optimized, original);
    assert_eq!(optimized.schema(), original.schema());
    assert_eq!(
        diagnostics.join_order_decisions[0].outcome,
        JoinOrderOutcome::Reordered
    );
    let decision = &diagnostics.join_order_decisions[0];
    assert!(decision.selected_cost < decision.original_cost);
}

#[test]
fn outer_join_is_a_reordering_boundary() {
    let left = scan("left", 100, &[(1, "left_key", 10)]);
    let right = scan("right", 100, &[(2, "right_key", 10)]);
    let plan = join(
        left,
        right,
        equality(1, "left_key", 2, "right_key"),
        JoinType::Left,
    );
    let original = plan.clone();

    let (optimized, diagnostics) = optimizer(JoinOrderConfig::default())
        .optimize_with_diagnostics(plan)
        .expect("outer joins are retained");

    assert_eq!(optimized, original);
    assert!(diagnostics.join_order_decisions.is_empty());
}

#[test]
fn safe_residual_inner_join_predicate_participates_in_search() {
    let a = scan("a", 100, &[(1, "a_key", 10)]);
    let b = scan("b", 100, &[(2, "b_key", 10)]);
    let c = scan("c", 100, &[(3, "c_key", 10)]);
    let key_equality = equality(1, "a_key", 2, "b_key");
    let residual = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(Arc::new(ResolvedColumn::new(
            ColumnId(1),
            "a_key",
            None,
            VeinDataType::Int64,
        )))),
        op: BinaryOp::Lt,
        right: Box::new(LogicalExpr::Column(Arc::new(ResolvedColumn::new(
            ColumnId(2),
            "b_key",
            None,
            VeinDataType::Int64,
        )))),
        return_type: VeinDataType::Boolean,
    };
    let on = LogicalExpr::BinaryOp {
        left: Box::new(key_equality),
        op: BinaryOp::And,
        right: Box::new(residual),
        return_type: VeinDataType::Boolean,
    };
    let ab = join(a, b, on, JoinType::Inner);
    let plan = join(ab, c, equality(2, "b_key", 3, "c_key"), JoinType::Inner);
    let original = plan.clone();

    let (optimized, diagnostics) = optimizer(JoinOrderConfig::default())
        .optimize_with_diagnostics(plan)
        .expect("safe residual predicates may be reassociated");

    assert_eq!(optimized, original);
    assert_eq!(diagnostics.join_order_decisions.len(), 1);
    let decision = &diagnostics.join_order_decisions[0];
    assert_eq!(decision.relation_count, 3);
    assert_eq!(decision.outcome, JoinOrderOutcome::NoImprovement);
    assert!(decision.original_cost.is_some());
    assert_eq!(decision.original_order, decision.selected_order);
}

#[test]
fn catalog_resolved_external_join_has_a_costed_join_order_decision() {
    let LogicalPlan::Scan {
        table,
        schema,
        ..
    } = scan("external", 100, &[(1, "external_key", 10)])
    else {
        unreachable!();
    };
    let external = LogicalPlan::ExternalScan {
        url: "file:///external".into(),
        options: vec![],
        files: vec![],
        source_metadata: Some(table),
        schema,
    };
    let right = scan("right", 100, &[(2, "right_key", 10)]);
    let plan = join(
        external,
        right,
        equality(1, "external_key", 2, "right_key"),
        JoinType::Inner,
    );

    let (_, diagnostics) = optimizer(JoinOrderConfig::default())
        .optimize_with_diagnostics(plan)
        .expect("resolved external source statistics should support join costing");
    let decision = diagnostics
        .join_order_decisions
        .first()
        .expect("two relations produce a join-order decision");
    assert_ne!(decision.outcome, JoinOrderOutcome::InsufficientStatistics);
    assert!(decision.original_cost.is_some());
    assert_eq!(decision.candidate_splits, 1);
}
