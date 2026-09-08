use crate::schema::column::{ColumnId, ResolvedColumn};
use std::sync::Arc;

/// Rkyv-safe mirror of Arrow schema
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VeinSchema {
    /// The fields making up this schema
    pub fields: Vec<ResolvedColumn>,
}

impl VeinSchema {
    /// Creates a new [`VeinSchema`] from a vector of resolved columns.
    pub fn new(fields: Vec<ResolvedColumn>) -> Self {
        Self { fields }
    }

    /// Find the 0-based physical field index for a given `ColumnId`.
    pub fn position_of_id(&self, id: ColumnId) -> Option<usize> {
        self.fields.iter().position(|field| field.id == id)
    }
}

/// Reference-counted VeinSchema
pub type VeinSchemaRef = Arc<VeinSchema>;
