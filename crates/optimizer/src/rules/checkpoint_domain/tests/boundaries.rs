use super::*;

#[test]
fn declines_non_group_keys_grouping_sets_and_global_aggregates() {
    for groups in [vec![], vec![11]] {
        let plan = join(source(1, &[1]), aggregate(source(2, &[10, 11, 20]), &groups),
                        JoinType::Left, eq(1, 10));
        assert!(!rewrite(plan).1);
    }
    let mut plan = candidate();
    let P::Join { right, .. } = &mut plan else { unreachable!() };
    let P::Aggregate { grouping_sets, .. } = right.as_mut() else { unreachable!() };
    grouping_sets.push(vec![]);
    assert!(!rewrite(plan).1);
}

#[test]
fn declines_non_replayable_sources_and_unknown_key_ownership() {
    let mut plan = candidate();
    let P::Join { left, .. } = &mut plan else { unreachable!() };
    let schema = left.schema().clone();
    *left = Box::new(P::Scan {
        table: Arc::new(plan::TableMetadata::new("items", Arc::new(schema.clone()))),
        projection: None,
        filters: vec![],
        schema: Box::new(schema),
    });
    assert!(!rewrite(plan).1);
    let mut plan = candidate();
    let P::Join { on, .. } = &mut plan else { unreachable!() };
    *on = Some(eq(88, 10));
    assert!(!rewrite(plan).1);
}

#[test]
fn declines_unsupported_join_semantics_and_volatile_aggregates() {
    for kind in [JoinType::Full, JoinType::Right, JoinType::Anti, JoinType::NullAwareAnti, JoinType::Single] {
        let mut plan = candidate();
        let P::Join { join_type, .. } = &mut plan else { unreachable!() };
        *join_type = kind;
        assert!(!rewrite(plan).1);
    }
    let mut plan = candidate();
    let P::Join { right, .. } = &mut plan else { unreachable!() };
    let P::Aggregate { aggs, .. } = right.as_mut() else { unreachable!() };
    *aggs = vec![E::FunctionCall {
        name: "random".into(),
        args: vec![],
        is_aggregate: false,
        distinct: false,
        return_type: T::Int64
    }];
    assert!(!rewrite(plan).1);
}

#[test]
fn declines_tuple_keys_split_between_independent_checkpoints() {
    let outer = join(source(1, &[1]), source(3, &[2]), JoinType::Inner, eq(1, 2));
    let plan = join(outer, aggregate(source(2, &[10, 11, 20]), &[10, 11]),
                    JoinType::Left, crate::rules::predicate_analysis::conjunction([eq(1, 10), eq(2, 11)]).unwrap());
    assert!(!rewrite(plan).1);
}
