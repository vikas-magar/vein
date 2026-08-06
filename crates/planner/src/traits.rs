use common::Result;

/// Abstraction for the Logical Binder.
/// Validates the raw SQL AST against the catalog schema and resolves types/functions.
pub trait LogicalBinder: Send + Sync {
    /// Takes a parsed AST string (or struct) and returns a validated Semantic/Logical Plan.
    fn bind(&self, ast: String) -> Result<String>;
}

/// Abstraction for the Physical Planner.
/// Responsible for lowering a bound Logical Plan into a distributed physical Execution DAG.
pub trait PhysicalPlanner: Send + Sync {
    /// Takes a Logical Plan and generates a distributed StageDag of MorselTasks.
    fn create_physical_plan(&self, logical_plan: String) -> Result<String>;
}
