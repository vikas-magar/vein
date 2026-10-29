use super::*;

#[test]
fn preserves_non_inner_probe_boundaries_and_non_semi_membership() {
    for kind in [JoinType::Left, JoinType::Right, JoinType::Full, JoinType::Semi,
        JoinType::Anti, JoinType::NullAwareAnti, JoinType::Single, JoinType::Cross] {
        let mut plan = candidate(1);
        let LogicalPlan::Join { left, .. } = &mut plan else { unreachable!() };
        let LogicalPlan::Join { join_type, .. } = left.as_mut() else { unreachable!() };
        *join_type = kind;
        assert_eq!(rewrite(plan.clone()), plan, "probe boundary {kind:?}");

        let mut plan = candidate(1);
        let LogicalPlan::Join { join_type, .. } = &mut plan else { unreachable!() };
        *join_type = kind;
        if kind != JoinType::Semi {
            assert_eq!(rewrite(plan.clone()), plan, "membership {kind:?}");
        }
    }
}

#[test]
fn declines_predicates_needing_both_probe_inputs_or_unknown_columns() {
    for predicate in [
        binary(equality(1, 3), BinaryOp::And, equality(2, 3)),
        equality(99, 3),
        equality(3, 3),
    ] {
        let mut plan = candidate(1);
        let LogicalPlan::Join { on, .. } = &mut plan else { unreachable!() };
        *on = Some(predicate);
        assert_eq!(rewrite(plan.clone()), plan);
    }
}

#[test]
fn declines_volatile_and_fallible_predicates_on_either_join() {
    let volatile = LogicalExpr::FunctionCall {
        name: "random".into(),
        args: vec![],
        is_aggregate: false,
        distinct: false,
        return_type: VeinDataType::Boolean,
    };
    let fallible = binary(column(1), BinaryOp::Divide, column(2));
    for unsafe_expression in [volatile, fallible] {
        for inner in [false, true] {
            let mut plan = candidate(1);
            let LogicalPlan::Join { left, on, .. } = &mut plan else { unreachable!() };
            let on = if inner {
                let LogicalPlan::Join { on, .. } = left.as_mut() else { unreachable!() };
                on
            } else { on };
            *on = Some(binary(on.take().unwrap(), BinaryOp::And, unsafe_expression.clone()));
            assert_eq!(rewrite(plan.clone()), plan);
        }
    }
}

#[test]
fn declines_using_missing_conditions_and_overlapping_identities() {
    for inner in [false, true] {
        let mut plan = candidate(1);
        let LogicalPlan::Join { left, using, .. } = &mut plan else { unreachable!() };
        let using = if inner {
            let LogicalPlan::Join { using, .. } = left.as_mut() else { unreachable!() };
            using
        } else { using };
        let LogicalExpr::Column(column) = column(1) else { unreachable!() };
        using.push(column);
        assert_eq!(rewrite(plan.clone()), plan);
    }
    for inner in [false, true] {
        let mut plan = candidate(1);
        let LogicalPlan::Join { left, on, .. } = &mut plan else { unreachable!() };
        let on = if inner {
            let LogicalPlan::Join { on, .. } = left.as_mut() else { unreachable!() };
            on
        } else { on };
        *on = None;
        assert_eq!(rewrite(plan.clone()), plan);
    }
    let mut plan = candidate(1);
    let LogicalPlan::Join { right, .. } = &mut plan else { unreachable!() };
    *right = Box::new(relation(1));
    assert_eq!(rewrite(plan.clone()), plan);
}

#[test]
fn injected_semantics_can_decline_movement() {
    struct Conservative;
    impl crate::rules::predicate_analysis::PredicateSemantics for Conservative {
        fn reorder_safe(&self, _: &LogicalExpr) -> bool { false }
        fn rejects_nulls(&self, _: &LogicalExpr, _: &std::collections::HashSet<ColumnId>) -> bool {
            false
        }
    }
    let plan = candidate(1);
    let (optimized, changed, _) = SemiJoinPlacementRule::new(Conservative)
        .apply_owned_plan(plan.clone(), &OptimizerContext::default()).unwrap();
    assert!(!changed);
    assert_eq!(optimized, plan);
}
