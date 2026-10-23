//! Compact final-plan visibility for predicate placement around joins.

use parser::ast::pipe_op::JoinType;
use plan::{LogicalPlan, PlanTraversal};

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct FilterPlacementVisibility {
    pub(crate) scan_count: usize,
    pub(crate) scan_filter_count: usize,
    pub(crate) residual_filter_count: usize,
    pub(crate) left_join_count: usize,
    pub(crate) left_join_with_on_count: usize,
    pub(crate) semi_join_count: usize,
    pub(crate) anti_join_count: usize,
    pub(crate) semi_anti_left_scan_filter_count: usize,
    pub(crate) semi_anti_right_scan_filter_count: usize,
    pub(crate) filter_above_semi_anti_count: usize,
}

impl FilterPlacementVisibility {
    pub(crate) fn from_plan(plan: &LogicalPlan) -> Self {
        fn is_semi_anti(plan: &LogicalPlan) -> bool {
            matches!(
                plan,
                LogicalPlan::Join {
                    join_type: JoinType::Semi | JoinType::Anti,
                    ..
                }
            )
        }

        #[derive(Clone, Copy)]
        enum SemiAntiInput {
            Left,
            Right,
        }

        fn visit(
            plan: &LogicalPlan,
            summary: &mut FilterPlacementVisibility,
            semi_anti_input: Option<SemiAntiInput>,
        ) {
            match plan {
                LogicalPlan::Scan { filters, .. } => {
                    summary.scan_count += 1;
                    summary.scan_filter_count += filters.len();
                    match semi_anti_input {
                        Some(SemiAntiInput::Left) => {
                            summary.semi_anti_left_scan_filter_count += filters.len();
                        }
                        Some(SemiAntiInput::Right) => {
                            summary.semi_anti_right_scan_filter_count += filters.len();
                        }
                        None => {}
                    }
                }
                LogicalPlan::Filter { input, .. } => {
                    summary.residual_filter_count += 1;
                    summary.filter_above_semi_anti_count += usize::from(is_semi_anti(input));
                }
                LogicalPlan::Join {
                    join_type: JoinType::Left,
                    on,
                    ..
                } => {
                    summary.left_join_count += 1;
                    summary.left_join_with_on_count += usize::from(on.is_some());
                }
                LogicalPlan::Join {
                    join_type: JoinType::Semi,
                    left,
                    right,
                    ..
                } => {
                    summary.semi_join_count += 1;
                    visit(left, summary, Some(SemiAntiInput::Left));
                    visit(right, summary, Some(SemiAntiInput::Right));
                    for subquery in plan.subquery_plans() {
                        visit(subquery, summary, None);
                    }
                    return;
                }
                LogicalPlan::Join {
                    join_type: JoinType::Anti,
                    left,
                    right,
                    ..
                } => {
                    summary.anti_join_count += 1;
                    visit(left, summary, Some(SemiAntiInput::Left));
                    visit(right, summary, Some(SemiAntiInput::Right));
                    for subquery in plan.subquery_plans() {
                        visit(subquery, summary, None);
                    }
                    return;
                }
                _ => {}
            }
            for child in plan.children() {
                visit(child, summary, semi_anti_input);
            }
            for subquery in plan.subquery_plans() {
                visit(subquery, summary, None);
            }
        }

        let mut summary = Self::default();
        visit(plan, &mut summary, None);
        summary
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plan::{LogicalExpr, LogicalLiteral, TableMetadata, VeinSchema};
    use std::sync::Arc;

    fn scan(name: &str, filter_count: usize) -> LogicalPlan {
        LogicalPlan::Scan {
            table: Arc::new(TableMetadata::new(name, Arc::new(VeinSchema::empty()))),
            projection: None,
            filters: vec![LogicalExpr::Literal(LogicalLiteral::Int64(1)); filter_count],
            schema: Box::new(VeinSchema::empty()),
        }
    }

    fn join(join_type: JoinType, left: LogicalPlan, right: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Join {
            join_type,
            left: Box::new(left),
            right: Box::new(right),
            on: Some(LogicalExpr::Literal(LogicalLiteral::Int64(1))),
            using: vec![],
            schema: Box::new(VeinSchema::empty()),
        }
    }

    #[test]
    fn summarizes_left_on_and_semi_anti_branch_filter_placement() {
        let left = join(JoinType::Left, scan("orders", 2), scan("lineitem", 1));
        let semi = join(JoinType::Semi, scan("orders", 3), scan("lineitem", 4));
        let anti = join(JoinType::Anti, semi, scan("lineitem", 5));
        let plan = LogicalPlan::Filter {
            input: Box::new(anti),
            predicate: LogicalExpr::Literal(LogicalLiteral::Int64(1)),
        };
        let summary = FilterPlacementVisibility::from_plan(&left);
        let visibility = FilterPlacementVisibility::from_plan(&plan);

        assert_eq!(summary.left_join_count, 1);
        assert_eq!(summary.left_join_with_on_count, 1);
        assert_eq!(visibility.semi_join_count, 1);
        assert_eq!(visibility.anti_join_count, 1);
        assert_eq!(visibility.semi_anti_left_scan_filter_count, 3);
        assert_eq!(visibility.semi_anti_right_scan_filter_count, 9);
        assert_eq!(visibility.filter_above_semi_anti_count, 1);
    }
}
