//! Concrete optimizer rules.

/// Heuristic evaluation of constant expressions at plan time.
pub mod constant_fold;

/// Semantics-preserving algebraic expression simplification.
pub mod expression_simplify;

/// Filter predicate and plan-node normalization.
pub mod filter_normalize;

/// Propagation of implied ordered bounds through deterministic filter predicates.
pub mod filter_transitivity;

/// Top-down required-column analysis and scan projection pruning.
pub mod column_pruning;

/// Shared helpers for safe predicate rewrites.
mod predicate_utils;

/// Predicate pushdown through relational operators.
pub mod predicate_pushdown;

/// Pushdown of safe logical filters into table scans.
pub mod scan_filter_pushdown;

/// Conservative statistics-based scan elimination.
pub mod scan_statistics_pruning;

/// Decorrelation of supported correlated scalar aggregates into grouped joins.
pub mod scalar_aggregate_decorrelation;

/// Decorrelation of correlated EXISTS predicates into semi/anti joins.
pub mod exists_decorrelation;

/// Rewrite filter-level IN/NOT IN subqueries to relational joins.
pub mod in_subquery;

/// Equality-based predicate propagation across inner joins.
pub mod join_transitivity;

/// Bounded cost-based enumeration for safe inner equijoins.
pub mod join_order;

/// Exact checkpoint key bounds propagated to unread inner-join inputs.
pub mod checkpoint_filters;
pub use checkpoint_filters::CheckpointJoinFilterRule;

pub use column_pruning::ColumnPruningRule;
pub use constant_fold::ConstantFoldingRule;
pub use expression_simplify::ExpressionSimplificationRule;
pub use filter_normalize::FilterNormalizationRule;
pub use filter_transitivity::FilterTransitivityRule;
pub use join_transitivity::JoinPredicateTransitivityRule;
pub use predicate_pushdown::PredicatePushdownRule;
pub use scan_filter_pushdown::ScanFilterPushdownRule;
pub use scan_statistics_pruning::ScanStatisticsPruningRule;
pub use scalar_aggregate_decorrelation::ScalarAggregateDecorrelationRule;
pub use exists_decorrelation::ExistsDecorrelationRule;
pub use in_subquery::InSubqueryJoinRule;
