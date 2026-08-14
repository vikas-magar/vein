/// Core logical plan nodes
pub mod logical;
/// Auxiliary types for logical plan definitions
pub mod types;
/// Traversal traits for logical plan tree operations
pub mod traversal;

pub use logical::LogicalPlan;
pub use types::SortKey;
pub use traversal::PlanTraversal;
