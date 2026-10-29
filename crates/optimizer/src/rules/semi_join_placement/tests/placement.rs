use super::*;

#[test]
fn places_semi_join_on_either_input_without_changing_schema_or_predicates() {
    for probe_key in [1, 2] {
        let plan = candidate(probe_key);
        let schema = plan.schema().clone();
        let optimized = rewrite(plan);
        let LogicalPlan::Join { join_type, left, right, on, .. } = &optimized else { panic!("inner"); };
        assert_eq!(*join_type, JoinType::Inner);
        assert_eq!(on.as_ref(), Some(&equality(1, 2)));
        assert_eq!(optimized.schema(), &schema);
        let input = if probe_key == 1 { left } else { right };
        let LogicalPlan::Join { join_type, left, right, on, schema, .. } = input.as_ref()
        else { panic!("semi join in selected input"); };
        assert_eq!(*join_type, JoinType::Semi);
        assert_eq!(on.as_ref(), Some(&equality(probe_key, 3)));
        assert_eq!(left.as_ref(), &relation(probe_key));
        assert_eq!(right.as_ref(), &relation(3));
        assert_eq!(schema.as_ref(), left.schema());
    }
}

#[test]
fn q18_shaped_membership_reaches_orders_before_customer_and_lineitem() {
    let input = join(
        join(relation(1), relation(2), JoinType::Inner, equality(1, 2)),
        relation(3), JoinType::Inner, equality(2, 3),
    );
    let plan = join(input, relation(4), JoinType::Semi, equality(2, 4));
    let (optimized, changed, _) = SemiJoinPlacementRule::default()
        .apply_owned_plan(plan, &OptimizerContext::default()).unwrap();
    assert!(changed);
    let LogicalPlan::Join { left, .. } = &optimized else { panic!("lineitem join"); };
    let LogicalPlan::Join { right, .. } = left.as_ref() else { panic!("customer join"); };
    let LogicalPlan::Join { join_type: JoinType::Semi, left, .. } = right.as_ref()
    else { panic!("membership must precede both joins"); };
    assert_eq!(left.as_ref(), &relation(2));
    let (again, changed, _) = SemiJoinPlacementRule::default()
        .apply_owned_plan(optimized.clone(), &OptimizerContext::default()).unwrap();
    assert!(!changed);
    assert_eq!(again, optimized);
}

#[test]
fn production_pipeline_keeps_placement_and_converges_on_a_second_optimization() {
    let pipeline = OptimizerPipeline::production();
    let optimized = pipeline.optimize(candidate(2)).unwrap();
    assert_eq!(pipeline.optimize(optimized.clone()).unwrap(), optimized);
    assert!(matches!(optimized, LogicalPlan::Join { join_type: JoinType::Inner, .. }));
}

#[test]
fn preserves_safe_residual_predicates_in_both_joins() {
    let mut plan = candidate(2);
    let LogicalPlan::Join { left, on, .. } = &mut plan else { unreachable!() };
    let residual = binary(column(2), BinaryOp::Lt, column(3));
    *on = Some(binary(on.take().unwrap(), BinaryOp::And, residual.clone()));
    let semi_on = on.clone();
    let LogicalPlan::Join { on, .. } = left.as_mut() else { unreachable!() };
    *on = Some(binary(on.take().unwrap(), BinaryOp::And,
                      binary(column(1), BinaryOp::Leq, column(2))));
    let inner_on = on.clone();
    let (optimized, changed, _) = SemiJoinPlacementRule::default()
        .apply_owned_plan(plan, &dense_key_context()).unwrap();
    assert!(changed, "dense keys make early residual membership cheaper");
    let LogicalPlan::Join { on, right, .. } = optimized else { panic!("inner"); };
    assert_eq!(on, inner_on);
    let LogicalPlan::Join { on, .. } = *right else { panic!("semi"); };
    assert_eq!(on, semi_on);
}
