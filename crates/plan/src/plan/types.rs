use crate::expr::LogicalExpr;

/// Defines sorting order for a specific key
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SortKey {
    /// The expression to sort by
    pub expr: LogicalExpr,
    /// Whether to sort in ascending order
    pub asc: bool,
    /// Whether nulls appear first
    pub nulls_first: bool,
}
