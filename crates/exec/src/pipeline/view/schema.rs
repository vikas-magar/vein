//! Schema metadata attached to a reference-backed view.

use arrow::datatypes::{Schema, SchemaRef};
use common::Result;
use std::sync::Arc;

use super::DataChunkView;

impl DataChunkView {
    /// Returns the schema when this view has crossed a schema-aware boundary.
    pub fn schema(&self) -> Option<&SchemaRef> {
        self.schema.as_ref()
    }

    /// Attaches a schema after validating the view's selected values and types.
    ///
    /// Only schema metadata is added; Arrow arrays and row mappings are retained.
    pub fn with_schema(mut self, schema: SchemaRef) -> Result<Self> {
        self.validate_schema(&schema)?;
        self.schema = Some(schema);
        Ok(self)
    }

    /// Returns a selected logical row window while retaining source arrays.
    pub fn slice_rows(&self, offset: usize, limit: usize) -> Result<Self> {
        self.with_selection(self.selection.slice(offset, limit))
    }
}

pub(super) fn project_schema(schema: &SchemaRef, columns: &[usize]) -> Result<SchemaRef> {
    let fields = columns
        .iter()
        .map(|&index| {
            schema.fields().get(index).cloned().ok_or_else(|| {
                common::VeinError::internal(format!(
                    "view schema projection column {index} is out of bounds"
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}
