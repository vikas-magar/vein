use crate::context::BinderContext;
use crate::scope::ScopeStack;
use plan::LogicalPlan;
use error::{Result, VeinError};
use parser::ast::query::Query;

use crate::scope::Scope;
use plan::VeinSchema;
use parser::ast::pipe_op::PipeOperator;
use parser::ast::query::TableSource;

use crate::pipe::advanced::{
    bind_assert, bind_call, bind_pivot, bind_table_sample, bind_unpivot, bind_with, bind_write,
};
use crate::pipe::dataflow::{bind_fork, bind_set_op, bind_tee};
use crate::pipe::mutation::{bind_as, bind_drop, bind_extend, bind_rename, bind_set};
use crate::pipe::order_limit::{bind_limit, bind_order_by};
use crate::pipe::relational::{
    bind_aggregate, bind_distinct, bind_join, bind_select, bind_where,
};

use crate::traits::{FunctionRegistry, LogicalBinder};
use async_trait::async_trait;
use std::sync::Arc;

/// Binds SQL query clauses into LogicalPlans
pub struct QueryBinder {
    catalog: Arc<dyn vcs::TableCatalog>,
    functions: Arc<dyn FunctionRegistry>,
}

impl QueryBinder {
    /// Creates a new QueryBinder with the required dependencies
    pub fn new(catalog: Arc<dyn vcs::TableCatalog>, functions: Arc<dyn FunctionRegistry>) -> Self {
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
                bound_field.qualifier = alias
                    .map(|a| a.to_string())
                    .or_else(|| Some(name.to_string()));
                columns.push(bound_field);
            }

            let qualifier = alias
                .map(|a| a.to_string())
                .or_else(|| Some(name.to_string()));
            scopes.push(Scope {
                columns: columns.clone(),
                aliases: vec![qualifier.clone().unwrap_or_default()],
            });

            LogicalPlan::Scan {
                table_name: name.to_string(),
                projection: None,
                filters: vec![],
                schema: Box::new(VeinSchema { fields: columns }),
            }
        }
_ => {
                return Err(VeinError::planning(
                    "Only Base Tables are supported right now in the binder",
                    None,
                ));
            }
    };

    // 2. Process Pipe Operators sequentially using the standalone helper
    Box::pin(bind_pipeline(plan, &query.pipes, context, scopes)).await
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
            PipeOperator::Where { predicate } => {
                bind_where(plan, predicate, &mut *context, &mut *scopes).await?
            }
            PipeOperator::Select { items } => {
                bind_select(plan, items, &mut *context, &mut *scopes).await?
            }
            PipeOperator::Aggregate {
                group_by,
                aggregates,
            } => bind_aggregate(plan, group_by, aggregates, &mut *context, &mut *scopes).await?,
            PipeOperator::Join { join } => {
                bind_join(plan, join, &mut *context, &mut *scopes).await?
            }
            PipeOperator::Distinct => bind_distinct(plan)?,

            // Sorting & Limiting
            PipeOperator::OrderBy { order_bys } => {
                bind_order_by(plan, order_bys, &mut *context, &mut *scopes).await?
            }
            PipeOperator::Limit { limit, offset } => bind_limit(plan, *limit, *offset)?,

            // Dataflow
            PipeOperator::SetOp { kind, right, all } => {
                bind_set_op(plan, kind, right, *all, &mut *context, &mut *scopes).await?
            }
            PipeOperator::Fork { pipelines } => {
                bind_fork(plan, pipelines, &mut *context, &mut *scopes).await?
            }
            PipeOperator::Tee { pipelines } => {
                bind_tee(plan, pipelines, &mut *context, &mut *scopes).await?
            }

            // Projection Mutations
            PipeOperator::Extend { items } => {
                bind_extend(plan, items, &mut *context, &mut *scopes).await?
            }
            PipeOperator::Set { assignments } => {
                bind_set(plan, assignments, &mut *context, &mut *scopes).await?
            }
            PipeOperator::Drop { columns } => bind_drop(plan, columns, &mut *scopes)?,
            PipeOperator::Rename { renames } => bind_rename(plan, renames, &mut *scopes)?,
            PipeOperator::As { alias } => bind_as(plan, alias)?,

            // Advanced Analytical
            PipeOperator::Pivot => bind_pivot(plan)?,
            PipeOperator::Unpivot => bind_unpivot(plan)?,
            PipeOperator::TableSample { method, percent } => {
                bind_table_sample(plan, method, *percent)?
            }
            PipeOperator::Assert { condition, message } => {
                bind_assert(plan, condition, message, &mut *context, &mut *scopes).await?
            }
            PipeOperator::Call { name, args } => {
                bind_call(plan, name, args, &mut *context, &mut *scopes).await?
            }
            PipeOperator::With { ctes } => bind_with(plan, ctes)?,
            PipeOperator::Write { target } => bind_write(plan, target)?,
        };
    }

    Ok(plan)
}
