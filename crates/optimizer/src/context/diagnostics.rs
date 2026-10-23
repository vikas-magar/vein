//! Rule-execution and cost-based join-order diagnostics.

use std::time::Duration;

use crate::estimation::NodeEstimate;

/// A logical phase in which an optimizer rule executes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizerPhase {
    /// Deterministic expression and plan normalization.
    Normalize,
    /// Always-safe heuristic rewrites.
    Heuristic,
    /// Statistics-driven alternative selection.
    CostBased,
}

/// Diagnostics for one rule invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleExecution {
    /// Rule name.
    pub rule_name: String,
    /// Rule phase.
    pub phase: OptimizerPhase,
    /// Sweep number, starting at one.
    pub sweep: usize,
    /// Whether the rule changed the plan.
    pub changed: bool,
    /// Time spent applying the rule.
    pub elapsed: Duration,
}

/// Diagnostics returned with an optimized plan.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct OptimizationDiagnostics {
    /// Per-rule execution records.
    pub rule_executions: Vec<RuleExecution>,
    /// Number of completed sweeps.
    pub sweeps: usize,
    /// Postorder cardinality estimates for the converged logical tree.
    pub cardinalities: Vec<NodeEstimate>,
    /// Cost-based join-order decisions made after heuristic convergence.
    pub join_order_decisions: Vec<JoinOrderDecision>,
    /// Final predicate positions and exact correspondence with input conjuncts.
    pub predicate_placements: Vec<super::PredicatePlacement>,
}

/// Outcome of bounded cost-based planning for one join region.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinOrderOutcome {
    /// A lower-cost legal join tree replaced the input tree.
    Reordered,
    /// Search completed and found no strict cost improvement.
    NoImprovement,
    /// The safe join-order search currently requires at least three relations.
    InsufficientRelations,
    /// The region exceeded its configured relation limit.
    RelationLimitExceeded,
    /// Search exceeded its configured candidate-split budget.
    SearchBudgetExceeded,
    /// A base input has no population estimate, so join-order cardinalities cannot be formed.
    InsufficientStatistics,
    /// Legal decompositions produced different relation summaries.
    AmbiguousEstimates,
    /// The predicate graph could not form a connected join tree.
    DisconnectedGraph,
    /// Column identities or predicates could not be mapped safely.
    UnsupportedShape,
    /// The configured cost model returned an invalid or unavailable cost.
    InvalidCost,
}

/// Distribution assumption used by the current equijoin cardinality model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinCardinalityAssumption {
    /// Non-null values are treated as uniform, with the smaller NDV domain
    /// assumed to overlap the larger one unless exact bounds prove disjoint.
    /// When a column synopsis is absent, row count is used as a maximum-NDV
    /// prior and null fraction defaults to zero. These are model inputs, not
    /// observed statistics or correctness proofs. Frequency skew cannot be
    /// detected from the current catalog statistics.
    UniformDistinctValueOverlap,
}

/// Cost-model terms for one oriented join alternative.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct JoinCostBreakdown {
    /// Cumulative cost below the left input.
    pub left_subtree_cost: f64,
    /// Cumulative cost below the right input.
    pub right_subtree_cost: f64,
    /// Cost assigned to this join by the cost model, when the model exposes it.
    pub local_cost: Option<f64>,
    /// Cumulative cost returned by the cost model.
    pub cumulative_cost: f64,
}

/// Cost inputs and result for one oriented partition of a join region.
#[derive(Debug, Clone, PartialEq)]
pub struct JoinOrderAlternative {
    /// Bit mask identifying the relation subset produced by this join.
    pub relation_mask: usize,
    /// Bit mask identifying the left input; bit positions map to relation ordinals.
    pub left_relation_mask: usize,
    /// Bit mask identifying the right input; bit positions map to relation ordinals.
    pub right_relation_mask: usize,
    /// Estimated rows from the left input.
    pub left_rows: f64,
    /// Estimated rows from the right input.
    pub right_rows: f64,
    /// Estimated rows from this join.
    pub output_rows: f64,
    /// Cumulative and local costs supplied to or returned by the cost model.
    pub cost: JoinCostBreakdown,
    /// Whether this is the lowest-cost split selected for its relation subset.
    pub best_for_subset: bool,
    /// Whether this oriented split matches the input logical tree.
    pub original_tree_split: bool,
}

/// Structured visibility into one cost-based join-order attempt.
#[derive(Debug, Clone, PartialEq)]
pub struct JoinOrderDecision {
    /// Number of atomic relation inputs in the region.
    pub relation_count: usize,
    /// Table names or stable operator names in the original leaf order.
    pub relation_labels: Vec<String>,
    /// Leaf ordinals in the original tree traversal order.
    pub original_order: Vec<usize>,
    /// Leaf ordinals selected by the search, or the original order on fallback.
    pub selected_order: Vec<usize>,
    /// Number of subset partitions examined.
    pub candidate_splits: usize,
    /// Estimated cumulative cardinality cost of the original tree.
    pub original_cost: Option<f64>,
    /// Estimated cumulative cardinality cost of the selected tree.
    pub selected_cost: Option<f64>,
    /// Distribution assumption behind equijoin cardinality estimates.
    pub cardinality_assumption: JoinCardinalityAssumption,
    /// Bounded list of cost inputs evaluated during subset search.
    pub alternatives: Vec<JoinOrderAlternative>,
    /// Result of the attempt.
    pub outcome: JoinOrderOutcome,
}
