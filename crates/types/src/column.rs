//! Column identity and resolved column definitions.

use crate::data_type::VeinDataType;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Query-local, monotonically assigned column identity.
/// Stable across optimizer rewrites (pushdown/reordering never invalidates ids).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
)]
pub struct ColumnId(pub usize);

impl std::fmt::Display for ColumnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self.0)
    }
}

/// A column as visible in a scope or produced by an operator.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
)]
pub struct ResolvedColumn {
    /// Unique identifier for this column in the query.
    pub id: ColumnId,
    /// The name of the column.
    pub name: String,
    /// The optional table or alias qualifier.
    pub qualifier: Option<String>,
    /// The data type of the column.
    pub data_type: VeinDataType,
    /// Whether this column uses dictionary encoding (factorized processing).
    pub is_factorized: bool,
    /// Whether the column can hold NULL values (outer-join output columns are nullable).
    pub nullable: bool,
}

impl ResolvedColumn {
    /// Creates a non-factorized, non-nullable column with a fresh identity.
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

    /// Sets whether this column is nullable.
    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Sets whether this column is factorized.
    pub fn with_factorized(mut self, factorized: bool) -> Self {
        self.is_factorized = factorized;
        self
    }

    /// Returns the fully qualified column name if a qualifier exists, or just the column name.
    pub fn qualified_name(&self) -> String {
        match &self.qualifier {
            Some(q) => format!("{q}.{}", self.name),
            None => self.name.clone(),
        }
    }

    /// Derives a new shared column with the given identity and name.
    ///
    /// Columns are immutable once visible, so renaming or requalifying produces
    /// a new column rather than mutating a shared one. The data type and flags
    /// carry over without copying.
    pub fn derived(
        &self,
        id: ColumnId,
        name: impl Into<String>,
        qualifier: Option<String>,
    ) -> ResolvedColumnRef {
        Arc::new(ResolvedColumn {
            id,
            name: name.into(),
            qualifier,
            data_type: self.data_type.clone(),
            is_factorized: self.is_factorized,
            nullable: self.nullable,
        })
    }
}

/// A reference-counted column.
///
/// The only way a column travels: cloning is one atomic increment, so a schema,
/// a plan node, or a column reference inside an expression costs a pointer
/// rather than a name and a type tree.
pub type ResolvedColumnRef = Arc<ResolvedColumn>;
