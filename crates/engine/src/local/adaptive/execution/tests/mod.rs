use super::stage::outcome_needs_more_statistics;
use optimizer::context::JoinOrderOutcome;

#[test]
fn only_statistics_resolvable_join_outcomes_request_another_checkpoint() {
    assert!(outcome_needs_more_statistics(
        JoinOrderOutcome::InsufficientStatistics
    ));
    assert!(outcome_needs_more_statistics(
        JoinOrderOutcome::AmbiguousEstimates
    ));

    for outcome in [
        JoinOrderOutcome::Reordered,
        JoinOrderOutcome::NoImprovement,
        JoinOrderOutcome::InsufficientRelations,
        JoinOrderOutcome::RelationLimitExceeded,
        JoinOrderOutcome::SearchBudgetExceeded,
        JoinOrderOutcome::DisconnectedGraph,
        JoinOrderOutcome::UnsupportedShape,
        JoinOrderOutcome::InvalidCost,
    ] {
        assert!(
            !outcome_needs_more_statistics(outcome),
            "unexpected follow-up checkpoint for {outcome:?}"
        );
    }
}
