use crate::schema::column::ResolvedColumn;
use std::sync::Arc;

/// Rkyv-safe mirror of Arrow schema
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VeinSchema {
    /// The fields making up this schema
    pub fields: Vec<ResolvedColumn>,
}

/// Reference-counted VeinSchema
pub type VeinSchemaRef = Arc<VeinSchema>;
