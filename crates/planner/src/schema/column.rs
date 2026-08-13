use crate::schema::data_type::VeinDataType;

/// Query-local, monotonically assigned column identity.
/// Stable across optimizer rewrites (pushdown/reordering never invalidates ids).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnId(pub usize);

/// A column as visible in a scope / produced by a plan node.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResolvedColumn {
    /// Unique identifier for this column in the query
    pub id: ColumnId,
    /// The name of the column
    pub name: String,
    /// The optional table or alias qualifier
    pub qualifier: Option<String>,
    /// The data type of the column
    pub data_type: VeinDataType,
    /// Whether this column uses dictionary encoding (factorized processing)
    pub is_factorized: bool,
}
