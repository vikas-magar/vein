//! Regressions for policy candidate selection and safe checkpoint placement.

use super::test_support::{join, scan};
use super::{LogicalPlan, StatisticsCheckpointPolicy};
use common::statistics::QueryStatisticsContext;
use std::sync::Arc;

#[test]
fn checkpoint_policy_prefers_small_input_over_large_join_subtree() {
    let plan = join(
        join(scan("large", 10_000), scan("small", 25)),
        scan("medium", 1_000),
    );

    let boundary = StatisticsCheckpointPolicy
        .choose_for_test(&plan)
        .expect("a filtered join input needs runtime statistics");
    let LogicalPlan::Scan { table, .. } = &boundary.producer else {
        panic!("policy should checkpoint the inexpensive leaf input");
    };
    assert_eq!(table.name, "small");
}

#[test]
fn same_source_population_is_checkpointed_only_once_per_query() {
    let plan = join(scan("same_source", 25), scan("same_source", 25));
    let statistics = Arc::new(QueryStatisticsContext::new([]));
    let policy = StatisticsCheckpointPolicy;

    assert!(super::AdaptiveCheckpointPolicy::choose_boundary(
        &policy,
        &plan,
        Arc::clone(&statistics),
    )
        .is_some());
    assert!(
        super::AdaptiveCheckpointPolicy::choose_boundary(&policy, &plan, statistics).is_none(),
        "an equivalent scan should not trigger another checkpoint"
    );
}

#[test]
fn a_different_filter_on_the_same_source_remains_checkpoint_eligible() {
    let mut different = scan("same_source", 25);
    if let LogicalPlan::Scan { filters, .. } = &mut different {
        if let plan::LogicalExpr::BinaryOp { right, .. } = &mut filters[0] {
            *right = Box::new(plan::LogicalExpr::Literal(plan::LogicalLiteral::Int64(10)));
        }
    }
    let plan = join(scan("same_source", 25), different);
    let statistics = Arc::new(QueryStatisticsContext::new([]));
    let policy = StatisticsCheckpointPolicy;

    assert!(super::AdaptiveCheckpointPolicy::choose_boundary(
        &policy,
        &plan,
        Arc::clone(&statistics),
    )
        .is_some());
    assert!(
        super::AdaptiveCheckpointPolicy::choose_boundary(&policy, &plan, statistics).is_some(),
        "a distinct filter represents a different population"
    );
}
