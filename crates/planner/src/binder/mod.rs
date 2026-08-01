//! The Binder.
//! 
//! Responsible for validating the pure-syntax AST against the catalog schema
//! and converting it into a valid, unoptimized `LogicalPlan`.

/// Expression binding logic.
pub mod expr;

use std::sync::Arc;
use arrow::datatypes::{Schema, SchemaRef};
use error::{Result, VeinError};
use parser::ast::query::{Query, TableSource};
use parser::ast::pipe_op::PipeOperator;
use crate::plan::LogicalPlan;
use crate::expr::LogicalExpr;
use self::expr::ExprBinder;

/// A trait for providing schema information during binding.
pub trait SchemaProvider: Send + Sync {
    /// Returns the schema for a given table name.
    fn get_table_schema(&self, name: &str) -> Result<SchemaRef>;
}

/// Translates AST into a LogicalPlan.
pub struct Binder {
    catalog: Arc<dyn SchemaProvider>,
}

impl Binder {
    /// Creates a new Binder with a given catalog schema provider.
    pub fn new(catalog: Arc<dyn SchemaProvider>) -> Self {
        Self { catalog }
    }

    /// Binds a parsed AST query into a LogicalPlan.
    pub fn bind(&self, query: &Query) -> Result<LogicalPlan> {
        // 1. Bind the FROM source to a LogicalPlan::Scan
        let mut plan = match &query.source {
            TableSource::Table { name, .. } => {
                let schema = self.catalog.get_table_schema(name)?;
                LogicalPlan::Scan {
                    table_name: name.clone(),
                    projection: None,
                    filters: vec![],
                    schema,
                }
            },
            TableSource::ObjectStore { url, .. } => {
                // For now, object stores just use an empty dummy schema or we'd infer it
                LogicalPlan::Scan {
                    table_name: url.clone(),
                    projection: None,
                    filters: vec![],
                    schema: Arc::new(Schema::empty()),
                }
            },
            TableSource::Subquery { .. } => {
                return Err(VeinError::internal("Subqueries in FROM clause not supported yet"));
            }
        };

        // 2. We skip joins for a moment, let's process the pipes
        for pipe in &query.pipes {
            plan = self.bind_pipe(plan, pipe)?;
        }

        Ok(plan)
    }

    fn bind_pipe(&self, input: LogicalPlan, pipe: &PipeOperator) -> Result<LogicalPlan> {
        let input_schema = input.schema();
        let expr_binder = ExprBinder::new(&input_schema);

        match pipe {
            PipeOperator::Where { predicate } => {
                let logical_predicate = expr_binder.bind(predicate)?;
                Ok(LogicalPlan::Filter {
                    input: Arc::new(input),
                    predicate: logical_predicate,
                })
            }
            PipeOperator::Select { items } => {
                let mut logical_exprs = Vec::with_capacity(items.len());
                for item in items {
                    logical_exprs.push(expr_binder.bind(&item.expr)?);
                }
                
                Ok(LogicalPlan::Project {
                    schema: input_schema.clone(), // In a real system, we must construct a new schema from exprs
                    input: Arc::new(input),
                    exprs: logical_exprs,
                })
            }
            // ... add others
            _ => Err(VeinError::internal(format!("Binding for pipe operator {:?} not yet implemented", pipe))),
        }
    }
}

