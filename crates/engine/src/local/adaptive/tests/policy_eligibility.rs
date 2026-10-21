use super::super::execution::join_checkpoint_has_search_space;
use plan::{LogicalPlan, PlanTraversal, VeinSchema};

fn relation(source_id: u128) -> LogicalPlan {
    LogicalPlan::Intermediate {
        source_id,
        row_count: 1,
        schema: Box::new(VeinSchema::empty()),
    }
}

fn join(left: LogicalPlan, right: LogicalPlan) -> LogicalPlan {
    LogicalPlan::Join {
        join_type: parser::ast::pipe_op::JoinType::Inner,
        left: Box::new(left),
        right: Box::new(right),
        on: None,
        using: Vec::new(),
        schema: Box::new(VeinSchema::empty()),
    }
}

#[test]
fn join_checkpoint_with_one_remaining_join_is_skipped() {
    let plan = join(join(relation(1), relation(2)), relation(3));
    let candidate = plan.children()[0];

    assert!(!join_checkpoint_has_search_space(&plan, candidate, &[0]));
}

#[test]
fn join_checkpoint_with_two_remaining_joins_stays_eligible() {
    let plan = join(
        join(join(relation(1), relation(2)), relation(3)),
        relation(4),
    );
    let left = plan.children()[0];
    let candidate = left.children()[0];

    assert!(join_checkpoint_has_search_space(&plan, candidate, &[0, 0]));
}
