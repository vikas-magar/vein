use super::*;

#[test]
fn equijoin_keys_are_oriented_by_identity_and_nulls_are_excluded() {
    let left = scan(17, Some(100), Some(10), Some(20));
    let right = scan(93, Some(50), Some(5), Some(10));
    let forward = join(left.clone(), right.clone(), JoinType::Inner, false);
    let reversed = join(left, right, JoinType::Inner, true);
    assert!((rows(&forward).unwrap() - 320.0).abs() < 1e-10);
    assert_eq!(rows(&forward), rows(&reversed));
    assert_eq!(root(&forward).rows.confidence, EstimateConfidence::Model);
}

#[test]
fn cross_join_does_not_assume_uniqueness_or_copy_inputs() {
    let plan = join(scan(1, Some(100), Some(10), Some(0)),
                    scan(2, Some(50), Some(5), Some(0)), JoinType::Cross, false);
    assert_eq!(rows(&plan), Some(5000.0));
    let estimates = StatisticsEstimator::new(&CatalogStatisticsProvider).estimate_plan(&plan);
    assert_eq!(estimates.iter().map(|e| e.node_id).collect::<Vec<_>>(), vec![0, 1, 2]);
    assert_eq!(estimates.iter().map(|e| e.operator).collect::<Vec<_>>(), vec!["Scan", "Scan", "Join"]);
}

#[test]
fn empty_build_preserves_outer_and_anti_rows() {
    for (kind, expected) in [(JoinType::Inner, 0.0), (JoinType::Left, 100.0),
        (JoinType::Right, 0.0), (JoinType::Full, 100.0), (JoinType::Semi, 0.0),
        (JoinType::Anti, 100.0), (JoinType::Single, 100.0), (JoinType::Cross, 0.0)]
    {
        let plan = join(scan(1, Some(100), Some(10), Some(0)),
                        scan(2, Some(0), Some(0), Some(0)), kind, false);
        assert_eq!(rows(&plan), Some(expected));
    }
}

#[test]
fn outer_semi_and_anti_estimates_respect_preserved_side_cardinality() {
    for kind in [JoinType::Left, JoinType::Right, JoinType::Full,
        JoinType::Semi, JoinType::Anti, JoinType::Single]
    {
        let plan = join(scan(1, Some(100), Some(100), Some(0)),
                        scan(2, Some(5), Some(5), Some(0)), kind, false);
        let value = rows(&plan).unwrap();
        match kind {
            JoinType::Left | JoinType::Full => assert!(value >= 100.0),
            JoinType::Right => assert!(value >= 5.0),
            JoinType::Semi | JoinType::Anti => assert!((0.0..=100.0).contains(&value)),
            JoinType::Single => assert_eq!(value, 100.0),
            _ => unreachable!(),
        }
    }
}

#[test]
fn unsupported_residuals_and_missing_ndv_stay_unknown() {
    let mut plan = join(scan(1, Some(100), Some(10), Some(0)),
                        scan(2, Some(5), Some(5), Some(0)), JoinType::Inner, false);
    let LogicalPlan::Join { on, .. } = &mut plan else { unreachable!(); };
    *on = Some(binary(on.take().unwrap(), BinaryOp::And,
                      LogicalExpr::Literal(LogicalLiteral::Boolean(true))));
    assert_eq!(root(&plan).rows.source, EstimateSource::UnsupportedExpression);
    let plan = join(scan(1, Some(100), None, Some(0)),
                    scan(2, Some(5), Some(5), Some(0)), JoinType::Inner, false);
    assert_eq!(rows(&plan), None);
}

#[test]
fn composite_key_estimates_do_not_multiply_correlated_conditions() {
    let mut plan = join(scan(1, Some(100), Some(10), Some(0)),
                        scan(2, Some(50), Some(5), Some(0)), JoinType::Inner, false);
    let expected = rows(&plan);
    let LogicalPlan::Join { on, .. } = &mut plan else { unreachable!(); };
    let key = on.take().unwrap();
    *on = Some(binary(key.clone(), BinaryOp::And, key));
    assert_eq!(rows(&plan), expected);
}

#[test]
fn null_extended_columns_do_not_retain_non_null_distribution() {
    let left = scan(1, Some(100), Some(10), Some(0));
    let right = scan(2, Some(50), Some(5), Some(0));
    let c = right.schema().fields[0].clone();
    let plan = join(left, right, JoinType::Left, false);
    let predicate = LogicalExpr::IsNull { expr: Box::new(LogicalExpr::Column(c)) };
    assert_eq!(rows(&filter(plan, predicate)), None);
}

#[test]
fn missing_inputs_never_receive_fabricated_cardinalities() {
    let plan = join(scan(1, None, None, None),
                    scan(2, Some(10), Some(10), Some(0)), JoinType::Cross, false);
    assert_eq!(rows(&plan), None);
}
