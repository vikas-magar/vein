use super::*;

#[test]
fn residual_existence_does_not_move_when_estimates_offer_no_probe_reduction() {
    let mut original = candidate(1);
    let LogicalPlan::Join { right, on, .. } = &mut original else { unreachable!() };
    let LogicalPlan::Intermediate { schema, .. } = right.as_mut() else { unreachable!() };
    let LogicalExpr::Column(other) = column(4) else { unreachable!() };
    schema.fields.push(other);
    *on = Some(binary(on.take().unwrap(), BinaryOp::And,
                      binary(column(1), BinaryOp::Neq, column(4))));
    assert_eq!(rewrite(original.clone()), original);
}

#[test]
fn q21_shaped_existence_stays_after_a_selective_inner_join() {
    let mut large = relation(1);
    let LogicalPlan::Intermediate { row_count, .. } = &mut large else { unreachable!() };
    *row_count = 1_000_000;
    let filtered = join(large, relation(2), JoinType::Inner, equality(1, 2));
    let original = join(filtered, relation(3), JoinType::Semi, equality(1, 3));
    assert_eq!(rewrite(original.clone()), original,
               "do not probe the million-row input when the inner join has reduced it");
}

#[test]
fn a_q18_shaped_reduction_still_moves_membership_to_the_smaller_probe_input() {
    let mut large = relation(2);
    let LogicalPlan::Intermediate { row_count, .. } = &mut large else { unreachable!() };
    *row_count = 1_000_000;
    let original = join(join(relation(1), large, JoinType::Inner, equality(1, 2)),
                        relation(3), JoinType::Semi, equality(1, 3));
    assert!(matches!(rewrite(original), LogicalPlan::Join { join_type: JoinType::Inner, .. }));
}

#[test]
fn unknown_population_does_not_speculate_about_early_membership_work() {
    let mut original = candidate(1);
    let LogicalPlan::Join { left: probe, .. } = &mut original else { unreachable!() };
    let LogicalPlan::Join { left, .. } = probe.as_mut() else { unreachable!() };
    let schema = left.schema().clone();
    *left = Box::new(LogicalPlan::Scan {
        table: Arc::new(plan::TableMetadata::new("unmeasured", Arc::new(schema.clone()))),
        projection: None,
        filters: vec![],
        schema: Box::new(schema),
    });
    assert_eq!(rewrite(original.clone()), original);
}
