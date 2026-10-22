use super::*;

#[test]
fn full_join_reductions_remove_exactly_the_rejected_unmatched_side() {
    for (reject_left, reject_right, expected) in [
        (true, false, JoinType::Left), (false, true, JoinType::Right), (true, true, JoinType::Inner),
    ] {
        let predicate = match (reject_left, reject_right) {
            (true, true) => binary(column(1), BinaryOp::Eq, column(2)),
            (true, false) => E::IsNotNull { expr: Box::new(column(1)) },
            _ => E::IsNotNull { expr: Box::new(column(2)) },
        };
        let mut plan = filtered(predicate);
        let LogicalPlan::Filter { input, .. } = &mut plan else { unreachable!() };
        let LogicalPlan::Join { join_type, .. } = input.as_mut() else { unreachable!() };
        *join_type = JoinType::Full;
        let result = rule().apply(&plan, &OptimizerContext::default()).unwrap().unwrap();
        let LogicalPlan::Filter { input, .. } = result else { unreachable!() };
        let LogicalPlan::Join { join_type, .. } = input.as_ref() else { unreachable!() };
        assert_eq!(*join_type, expected);
    }
}

#[test]
fn right_join_reduces_when_where_rejects_null_extended_left_rows() {
    let mut plan = filtered(E::IsNotNull { expr: Box::new(column(1)) });
    let LogicalPlan::Filter { input, .. } = &mut plan else { unreachable!() };
    let LogicalPlan::Join { join_type, .. } = input.as_mut() else { unreachable!() };
    *join_type = JoinType::Right;
    let result = rule().apply(&plan, &OptimizerContext::default()).unwrap().unwrap();
    let LogicalPlan::Filter { input, .. } = result else { unreachable!() };
    assert!(matches!(input.as_ref(), LogicalPlan::Join { join_type: JoinType::Inner, .. }));
}
