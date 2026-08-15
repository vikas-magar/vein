use crate::plan::LogicalPlan;
use crate::schema::{ColumnId, VeinSchemaRef};
use crate::traits::{FunctionRegistry, SchemaProvider};
use std::collections::HashMap;

/// Context for the binder, holding references to external catalogs and functions.
pub struct BinderContext<'a> {
    /// The global schema provider for resolving tables
    pub catalog: &'a dyn SchemaProvider,
    /// The registry for resolving functions
    pub functions: &'a dyn FunctionRegistry,
    /// Common Table Expressions (CTEs) bound in the current query scope
    pub ctes: HashMap<String, Box<LogicalPlan>>,
    /// Optional lineage schema for auditing
    pub lineage_schema: Option<VeinSchemaRef>,
    /// Optional calibration schema for conformal prediction
    pub calibration_schema: Option<VeinSchemaRef>,
    /// Global column ID generator for the query
    pub next_col_id: ColumnId,
}

impl<'a> BinderContext<'a> {
    /// Creates a new BinderContext
    pub fn new(catalog: &'a dyn SchemaProvider, functions: &'a dyn FunctionRegistry) -> Self {
        Self {
            catalog,
            functions,
            ctes: HashMap::new(),
            lineage_schema: None,
            calibration_schema: None,
            next_col_id: ColumnId(1),
        }
    }

    /// Generates the next monotonically increasing ColumnId
    pub fn next_id(&mut self) -> ColumnId {
        let id = self.next_col_id;
        self.next_col_id = ColumnId(self.next_col_id.0 + 1);
        id
    }
}
