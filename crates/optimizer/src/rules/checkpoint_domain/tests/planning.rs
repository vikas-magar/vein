use super::*;

fn pending() -> P {
    let schema = source(1, &[1]).schema().clone();
    let probe = P::Scan {
        table: Arc::new(plan::TableMetadata::new("probe", Arc::new(schema.clone()))),
        projection: None,
        filters: vec![],
        schema: Box::new(schema),
    };
    join(
        probe,
        aggregate(source(2, &[10, 20]), &[10]),
        JoinType::Left,
        eq(1, 10),
    )
}

#[test]
fn requests_tuple_retention_independently_of_missing_statistics() {
    let plan = pending();
    let P::Join { left, .. } = &plan else {
        unreachable!()
    };
    assert!(std::ptr::eq(
        CheckpointAggregateDomainRule::checkpoint_input(&plan).unwrap(),
        left.as_ref()
    ));
    assert_eq!(CheckpointAggregateDomainRule::pending_checkpoints(&plan), 1);
    assert_eq!(
        CheckpointAggregateDomainRule::pending_checkpoints(&candidate()),
        0
    );
}

#[test]
fn inner_join_reordering_can_place_the_grouped_input_on_the_left() {
    let P::Join {
        left,
        right,
        on,
        schema,
        ..
    } = candidate()
    else {
        unreachable!()
    };
    let original = P::Join {
        join_type: JoinType::Inner,
        left: right,
        right: left,
        on,
        using: vec![],
        schema,
    };
    let (rewritten, changed) = rewrite(original.clone());
    assert!(changed);
    assert_eq!(rewritten.schema(), original.schema());
    let P::Join { left, right, .. } = &rewritten else {
        unreachable!()
    };
    assert_eq!(right.as_ref(), &source(1, &[1]));
    let P::Aggregate { input, .. } = left.as_ref() else {
        panic!("aggregate");
    };
    assert!(matches!(
        input.as_ref(),
        P::Join {
            join_type: JoinType::Semi,
            ..
        }
    ));
    assert!(!rewrite(rewritten).1);
}

#[test]
fn swapped_inner_join_still_requests_the_probe_domain() {
    let P::Join {
        left,
        right,
        on,
        schema,
        ..
    } = pending()
    else {
        unreachable!()
    };
    let plan = P::Join {
        join_type: JoinType::Inner,
        left: right,
        right: left,
        on,
        using: vec![],
        schema,
    };
    let P::Join { right, .. } = &plan else {
        unreachable!()
    };
    assert!(std::ptr::eq(
        CheckpointAggregateDomainRule::checkpoint_input(&plan).unwrap(),
        right.as_ref()
    ));
}
