use super::*;

#[test]
fn restricts_grouped_input_without_changing_the_outer_join_or_output() {
    let original = candidate();
    let schema = original.schema().clone();
    let (rewritten, changed) = rewrite(original.clone());
    assert!(changed);
    assert_eq!(rewritten.schema(), &schema);
    let P::Join { join_type, left, on, .. } = &rewritten else { unreachable!() };
    assert_eq!(*join_type, JoinType::Left);
    assert_eq!(left.as_ref(), &source(1, &[1]));
    assert_eq!(on, &Some(eq(1, 10)));
    assert_eq!(semi_keys(&rewritten), vec![(ColumnId(10), ColumnId(1))]);
    let P::Join { left, right, .. } = grouped_input(&rewritten) else { unreachable!() };
    assert_eq!(left.as_ref(), &source(2, &[10, 20]));
    let P::Project { input, exprs, .. } = right.as_ref() else { panic!("key projection"); };
    assert_eq!(input.as_ref(), &source(1, &[1]));
    assert_eq!(exprs, &vec![E::Column(field(1))]);
    assert!(!rewrite(rewritten).1, "a second sweep must not duplicate the restriction");
}

#[test]
fn tuple_membership_preserves_composite_key_correlation() {
    let plan = join(source(1, &[1, 2]), aggregate(source(2, &[10, 11, 20]), &[10, 11]),
                    JoinType::Left, crate::rules::predicate_analysis::conjunction([eq(1, 10), eq(2, 11)]).unwrap());
    let (rewritten, changed) = rewrite(plan);
    assert!(changed);
    assert_eq!(semi_keys(&rewritten),
               vec![(ColumnId(10), ColumnId(1)), (ColumnId(11), ColumnId(2))]);
    assert!(!rewrite(rewritten).1);
}

#[test]
fn handles_empty_and_null_capable_checkpoint_populations_without_special_defaults() {
    let mut plan = candidate();
    let P::Join { left, .. } = &mut plan else { unreachable!() };
    let P::Intermediate { row_count, .. } = left.as_mut() else { unreachable!() };
    *row_count = 0;
    let (rewritten, changed) = rewrite(plan.clone());
    assert!(changed);
    assert_eq!(rewritten.schema(), plan.schema());
    assert!(matches!(rewritten, P::Join { join_type: JoinType::Left, .. }));
    // No new global aggregate or COUNT default is invented. NULL equality
    // remains the existing semi-join kernel's SQL three-valued comparison.
    assert_eq!(semi_keys(&rewritten), vec![(ColumnId(10), ColumnId(1))]);
}

#[test]
fn production_pipeline_converges_after_domain_insertion() {
    let pipeline = OptimizerPipeline::production();
    let optimized = pipeline.optimize(candidate()).unwrap();
    assert_eq!(pipeline.optimize(optimized.clone()).unwrap(), optimized);
}
