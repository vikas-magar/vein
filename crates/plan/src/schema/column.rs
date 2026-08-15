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
    /// Whether the column can hold NULL values (outer-join output columns are).
    pub nullable: bool,
}

impl ResolvedColumn {
    /// Creates a non-factorized, non-nullable column with a fresh identity.
    /// Binder-produced columns default to `is_factorized: false` and
    /// `nullable: false`; join nullability is applied explicitly afterwards.
    pub fn new(
        id: ColumnId,
        name: impl Into<String>,
        qualifier: Option<String>,
        data_type: VeinDataType,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            qualifier,
            data_type,
            is_factorized: false,
            nullable: false,
        }
    }
}
