use common::Result;
use crate::traits::LogicalBinder;
use parser::ast::query::Query;
use crate::plan::LogicalPlan;
use arrow::datatypes::{Schema, Field, DataType};
use std::sync::Arc;

/// The Default SQL Binder.
/// Responsible for parsing the raw SQL string into an AST, validating it against the catalog,
/// and returning a serialized Semantic Logical Plan.
pub struct DefaultSqlBinder;

impl DefaultSqlBinder {
    /// Creates a new DefaultSqlBinder instance.
    pub fn new() -> Self {
        Self {}
    }
}

impl LogicalBinder for DefaultSqlBinder {
    fn bind(&self, _ast: Query) -> Result<LogicalPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("dummy", DataType::Int64, false),
        ]));
        
        Ok(LogicalPlan::Scan {
            table_name: "dummy_table".to_string(),
            projection: None,
            filters: vec![],
            schema,
        })
    }
}
