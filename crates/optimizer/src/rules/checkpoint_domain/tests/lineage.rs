use super::*;

#[test]
fn traces_q17_style_outer_keys_through_an_inner_join() {
    let outer = join(source(1, &[1]), source(3, &[3]), JoinType::Inner, eq(1, 3));
    let plan = join(outer, aggregate(source(2, &[10, 20]), &[10]), JoinType::Left, eq(1, 10));
    let (rewritten, changed) = rewrite(plan);
    assert!(changed);
    assert_eq!(semi_keys(&rewritten), vec![(ColumnId(10), ColumnId(1))]);
}

#[test]
fn maps_group_key_aliases_without_moving_the_aggregate_expression() {
    let aggregate = aggregate(source(2, &[10, 20]), &[10]);
    let right = P::Project {
        input: Box::new(aggregate),
        exprs: vec![E::Column(field(10)), E::Column(field(99))],
        schema: Box::new(VeinSchema::new(vec![field(50), field(51)])),
    };
    let original = join(source(1, &[1]), right, JoinType::Left, eq(1, 50));
    let (rewritten, changed) = rewrite(original.clone());
    assert!(changed);
    assert_eq!(semi_keys(&rewritten), vec![(ColumnId(10), ColumnId(1))]);
    let P::Join { right: old, .. } = original else { unreachable!() };
    let P::Join { right: new, .. } = rewritten else { unreachable!() };
    let P::Project { exprs: old, .. } = *old else { unreachable!() };
    let P::Project { exprs: new, .. } = *new else { unreachable!() };
    assert_eq!(new, old);
}

#[test]
fn maps_outer_projection_aliases_back_to_checkpoint_identities() {
    let outer = P::Project {
        input: Box::new(source(1, &[1])),
        exprs: vec![E::Column(field(1))],
        schema: Box::new(VeinSchema::new(vec![field(5)])),
    };
    let plan = join(outer, aggregate(source(2, &[10, 20]), &[10]), JoinType::Left, eq(5, 10));
    let (rewritten, changed) = rewrite(plan);
    assert!(changed);
    assert_eq!(semi_keys(&rewritten), vec![(ColumnId(10), ColumnId(1))]);
}
