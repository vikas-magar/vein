use common::Result;
use parser::ast::query::Query;
use crate::plan::LogicalPlan;

/// Abstraction for the Logical Binder.
/// Validates the raw SQL AST against the catalog schema and resolves types/functions.
pub trait LogicalBinder: Send + Sync {
    /// Takes a parsed AST and returns a validated Semantic/Logical Plan.
    fn bind(&self, ast: Query) -> Result<LogicalPlan>;
}

/// Abstract Query Optimizer.
/// Allows us to seamlessly swap a traditional Cost-Based Optimizer (Cascades) 
/// with our cutting-edge Reinforcement Learning engine (ADOPT).
pub trait JoinOptimizer: Send + Sync {
    /// Takes a Logical AST and returns an optimized LogicalPlan.
    fn optimize_join_order(&self, logical_plan: LogicalPlan) -> Result<LogicalPlan>;
}
