//! Bounded dynamic programming for safe inner equijoins.
//!
//! The rule operates on relation metadata and borrowed estimates only. It
//! moves existing logical subtrees when rebuilding a selected alternative;
//! execution batches and column buffers never enter the optimizer.

use std::collections::HashMap;

use error::{Result, VeinError};
use parser::ast::{expr::BinaryOp, pipe_op::JoinType};
use plan::{LogicalExpr, LogicalPlan, PlanTraversal, VeinDataType, VeinSchema};

use crate::context::{
    JoinCardinalityAssumption, JoinCostBreakdown, JoinOrderAlternative, JoinOrderConfig,
    JoinOrderDecision, JoinOrderOutcome, OptimizerContext,
};
use crate::estimation::{inner_join, RelationEstimate};
use crate::rule::{OptimizerRule, RuleType};


mod cost;
mod work_cost;
mod diagnostics;
mod eligibility;
mod exact;
mod graph;
mod predicates;
mod greedy;
mod search_state;
mod helpers;
mod rebuild;
mod rule;

pub use cost::{CumulativeCardinalityCost, JoinCostInput, JoinCostModel};
pub use work_cost::HashJoinWorkCost;
pub use rule::JoinOrderRule;
use diagnostics::log_decision;
use eligibility::is_reorderable_join;
#[cfg(test)]
use eligibility::equi_atoms;
use exact::enumerate;
use helpers::*;
use rebuild::rebuild;

struct JoinGraph<'a> {
    leaves: Vec<&'a LogicalPlan>,
    predicates: Vec<JoinPredicate<'a>>,
    original_splits: HashMap<usize, (usize, usize)>,
    adjacency: Vec<usize>,
    analysis_schema: VeinSchema,
    full_mask: usize,
    labels: Vec<String>,
}
struct GraphFailure {
    relation_count: usize,
    relation_labels: Vec<String>,
    outcome: JoinOrderOutcome,
}

struct JoinPredicate<'a> {
    expression: &'a LogicalExpr,
    relations: usize,
}

struct OwnedPredicate {
    expression: LogicalExpr,
    relations: usize,
}

struct PreparedJoinOrder {
    splits: HashMap<usize, (usize, usize)>,
    predicates: Vec<OwnedPredicate>,
    relation_count: usize,
    decision: JoinOrderDecision,
}

enum SearchAttempt {
    Selected(PreparedJoinOrder),
    NoChange(JoinOrderDecision),
    Fallback(JoinOrderDecision),
}

enum SearchResult {
    Selected {
        splits: HashMap<usize, (usize, usize)>,
        order: Vec<usize>,
        candidate_splits: usize,
        original_cost: f64,
        selected_cost: f64,
        alternatives: Vec<JoinOrderAlternative>,
    },
    NoImprovement {
        order: Vec<usize>,
        candidate_splits: usize,
        original_cost: f64,
        selected_cost: f64,
        alternatives: Vec<JoinOrderAlternative>,
    },
    Fallback {
        outcome: JoinOrderOutcome,
        candidate_splits: usize,
        original_cost: Option<f64>,
        alternatives: Vec<JoinOrderAlternative>,
    },
}

struct SubplanState<'a> {
    relation: RelationEstimate<'a>,
    cumulative_cost: f64,
    split: Option<(usize, usize)>,
    order: Vec<usize>,
}

#[cfg(test)]
mod tests;
