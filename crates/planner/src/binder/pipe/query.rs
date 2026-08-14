use error::{Result, VeinError};
use parser::ast::query::Query;
use crate::plan::LogicalPlan;
use crate::binder::context::BinderContext;
use crate::binder::scope::ScopeStack;

use crate::binder::scope::Scope;
use crate::schema::VeinSchema;
use parser::ast::query::TableSource;
use parser::ast::pipe_op::PipeOperator;

use crate::binder::pipe::relational::{bind_where, bind_select, bind_aggregate, bind_join, bind_distinct};
use crate::binder::pipe::order_limit::{bind_order_by, bind_limit};
use crate::binder::pipe::dataflow::{bind_set_op, bind_fork, bind_tee};
use crate::binder::pipe::mutation::{bind_extend, bind_set, bind_drop, bind_rename, bind_as};
use crate::binder::pipe::advanced::{bind_pivot, bind_unpivot, bind_table_sample, bind_assert, bind_call, bind_with, bind_write};

use std::sync::Arc;
use crate::traits::{LogicalBinder, SchemaProvider, FunctionRegistry};
use async_trait::async_trait;

/// Binds SQL query clauses into LogicalPlans
pub struct QueryBinder {
    catalog: Arc<dyn SchemaProvider>,
    functions: Arc<dyn FunctionRegistry>,
}

impl QueryBinder {
    /// Creates a new QueryBinder with the required dependencies
    pub fn new(catalog: Arc<dyn SchemaProvider>, functions: Arc<dyn FunctionRegistry>) -> Self {
        Self { catalog, functions }
    }
}

#[async_trait]
impl LogicalBinder for QueryBinder {
    /// Recursively binds a parsed SQL Query into a LogicalPlan tree
    async fn bind<'a>(&self, query: &'a Query<'a>) -> Result<LogicalPlan> {
        let catalog_ref = self.catalog.as_ref();
        let functions_ref = self.functions.as_ref();
        
        let mut context = BinderContext::new(catalog_ref, functions_ref);
        let mut scopes = ScopeStack::new(None);

        bind_query_internal(query, &mut context, &mut scopes).await
    }
}

/// Helper to recursively bind a parsed SQL Query into a LogicalPlan tree
pub async fn bind_query_internal<'a>(
    query: &Query<'_>,
    context: &mut BinderContext<'a>,
    scopes: &mut ScopeStack<'a>,
) -> Result<LogicalPlan> {
    // 1. Bind the Source (FROM clause equivalent)
    let plan = match &query.source {
        TableSource::Table { name, alias } => {
            let schema = context.catalog.get_table_schema(name).await?;
            
            let mut columns = Vec::new();
            for field in &schema.fields {
                let mut bound_field = field.clone();
                bound_field.id = context.next_id();
                bound_field.qualifier = alias.map(|a| a.to_string()).or_else(|| Some(name.to_string()));
                columns.push(bound_field);
            }
            
            scopes.push(Scope {
                columns: columns.clone(),
                qualifier: alias.map(|a| a.to_string()).or_else(|| Some(name.to_string())),
            });

            LogicalPlan::Scan {
                table_name: name.to_string(),
                projection: None,
                filters: vec![],
                schema: Box::new(VeinSchema { fields: columns }),
            }
        }
        _ => return Err(VeinError::internal("Only Base Tables are supported right now in the binder")),
    };

    // 2. Process Pipe Operators sequentially using the standalone helper
    bind_pipeline(plan, &query.pipes, context, scopes).await
}

/// Binds a sequence of PipeOperators against an existing LogicalPlan
pub async fn bind_pipeline<'a>(
    mut plan: LogicalPlan, 
    pipes: &[PipeOperator<'_>],
    context: &mut BinderContext<'a>,
    scopes: &mut ScopeStack<'a>,
) -> Result<LogicalPlan> {
    for pipe in pipes {
        plan = match pipe {
            // Relational Core
            PipeOperator::Where { predicate } => bind_where(plan, predicate, &mut *context, &mut *scopes).await?,
            PipeOperator::Select { items } => bind_select(plan, items, &mut *context, &mut *scopes).await?,
            PipeOperator::Aggregate { group_by, aggregates } => bind_aggregate(plan, group_by, aggregates, &mut *context, &mut *scopes).await?,
            PipeOperator::Join { join } => bind_join(plan, join, &mut *context, &mut *scopes).await?,
            PipeOperator::Distinct => bind_distinct(plan)?,
            
            // Sorting & Limiting
            PipeOperator::OrderBy { order_bys } => bind_order_by(plan, order_bys, &mut *context, &mut *scopes).await?,
            PipeOperator::Limit { limit, offset } => bind_limit(plan, *limit, *offset)?,
            
            // Dataflow
            PipeOperator::SetOp { kind, right, all } => bind_set_op(plan, kind, right, *all, &mut *context, &mut *scopes).await?,
            PipeOperator::Fork { pipelines } => bind_fork(plan, pipelines, &mut *context, &mut *scopes).await?,
            PipeOperator::Tee { pipelines } => bind_tee(plan, pipelines, &mut *context, &mut *scopes).await?,
            
            // Projection Mutations
            PipeOperator::Extend { items } => bind_extend(plan, items, &mut *context, &mut *scopes).await?,
            PipeOperator::Set { assignments } => bind_set(plan, assignments, &mut *context, &mut *scopes).await?,
            PipeOperator::Drop { columns } => bind_drop(plan, columns, &mut *scopes)?,
            PipeOperator::Rename { renames } => bind_rename(plan, renames, &mut *scopes)?,
            PipeOperator::As { alias } => bind_as(plan, alias)?,
            
            // Advanced Analytical
            PipeOperator::Pivot => bind_pivot(plan)?,
            PipeOperator::Unpivot => bind_unpivot(plan)?,
            PipeOperator::TableSample { method, percent } => bind_table_sample(plan, method, *percent)?,
            PipeOperator::Assert { condition, message } => bind_assert(plan, condition, message)?,
            PipeOperator::Call { name, args } => bind_call(plan, name, args, &mut *context, &mut *scopes).await?,
            PipeOperator::With { ctes } => bind_with(plan, ctes)?,
            PipeOperator::Write { target } => bind_write(plan, target)?,
        };
    }

    Ok(plan)
}
