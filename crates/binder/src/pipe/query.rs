use crate::context::BinderContext;
use crate::scope::ScopeStack;
use error::Result;
use parser::ast::query::{Query, QueryNode};
use plan::{CatalogProvider, LogicalPlan, PlanTraversal};

use crate::scope::Scope;
use parser::ast::pipe_op::PipeOperator;
use parser::ast::query::TableSource;
use plan::VeinSchema;

use crate::pipe::advanced::{
    bind_assert, bind_call, bind_pivot, bind_table_sample, bind_unpivot, bind_with, bind_write,
};
use crate::pipe::dataflow::{bind_fork, bind_set_op, bind_tee};
use crate::pipe::mutation::{bind_as, bind_drop, bind_extend, bind_rename, bind_set};
use crate::pipe::order_limit::{bind_limit, bind_order_by};
use crate::pipe::relational::{
    bind_aggregate, bind_distinct, bind_join, bind_qualify, bind_select, bind_where,
};

use crate::traits::{BoundQuery, FunctionRegistry, LogicalBinder};
use async_trait::async_trait;
use std::sync::Arc;

/// Binds SQL query clauses into LogicalPlans
pub struct QueryBinder {
    catalog: Arc<dyn CatalogProvider>,
    functions: Arc<dyn FunctionRegistry>,
}

impl QueryBinder {
    /// Creates a new QueryBinder with the required dependencies
    pub fn new(catalog: Arc<dyn CatalogProvider>, functions: Arc<dyn FunctionRegistry>) -> Self {
        Self { catalog, functions }
    }

    /// Access the underlying table catalog.
    pub fn catalog(&self) -> &Arc<dyn CatalogProvider> {
        &self.catalog
    }
}

#[async_trait]
impl LogicalBinder for QueryBinder {
    fn catalog(&self) -> Option<&Arc<dyn CatalogProvider>> {
        Some(&self.catalog)
    }

    /// Recursively binds a parsed SQL statement into a LogicalPlan tree.
    /// `EXPLAIN` statements bind the inner query and wrap it in a
    /// [`LogicalPlan::Explain`] node so downstream stages can render the
    /// plan instead of executing it.
    async fn bind<'a>(&self, node: &'a QueryNode<'a>) -> Result<LogicalPlan> {
        Ok(self.bind_with_catalog_revision(node).await?.plan)
    }

    async fn bind_with_catalog_revision<'a>(&self, node: &'a QueryNode<'a>) -> Result<BoundQuery> {
        let catalog_ref = self.catalog.as_ref();
        let functions_ref = self.functions.as_ref();
        let planning_session = self.catalog.begin_planning_session().await?;
        let mut context =
            BinderContext::with_planning_session(catalog_ref, functions_ref, planning_session);
        let mut scopes = ScopeStack::new();

        let plan = match node {
            QueryNode::Plain { query } => {
                bind_query_internal(query, &mut context, &mut scopes).await?
            }
            QueryNode::Explain { query } => {
                let inner = bind_query_internal(query, &mut context, &mut scopes).await?;
                let schema = inner.schema().clone();
                LogicalPlan::Explain {
                    input: Box::new(inner),
                    schema: Box::new(schema),
                }
            }
        };

        Ok(BoundQuery {
            plan,
            catalog_revision: context.planning_session.revision,
        })
    }
}

/// Helper to recursively bind a parsed SQL Query into a LogicalPlan tree
pub async fn bind_query_internal<'a>(
    query: &Query<'_>,
    context: &mut BinderContext<'a>,
    scopes: &mut ScopeStack,
) -> Result<LogicalPlan> {
    // 1. Bind the Source (FROM clause equivalent)
    let plan = bind_source(&query.source, context, scopes).await?;

    // 2. Process Pipe Operators sequentially using the standalone helper
    Box::pin(bind_pipeline(plan, &query.pipes, context, scopes)).await
}

/// Binds a query `FROM` source (base table, subquery, or object store) into a
/// `LogicalPlan`, pushing the resulting columns into the current scope.
///
/// A `Subquery` source is bound in a fresh, non-correlated `ScopeStack`
/// (mirroring set operations) and wrapped in a [`LogicalPlan::SubqueryAlias`]
/// so its columns answer to the given alias.
pub async fn bind_source<'a>(
    source: &TableSource<'_>,
    context: &mut BinderContext<'a>,
    scopes: &mut ScopeStack,
) -> Result<LogicalPlan> {
    match source {
        TableSource::Table { name, alias } => {
            // A registered CTE takes precedence over a catalog table lookup.
            if let Some(cte) = context.ctes.get(name.as_ref()) {
                let qualifier = alias
                    .map(|a| a.to_string())
                    .unwrap_or_else(|| name.to_string());
                let fields = cte
                    .schema()
                    .fields
                    .iter()
                    .map(|f| f.derived(f.id, f.name.as_str(), alias.map(|a| a.to_string())))
                    .collect::<Vec<_>>();
                let plan = LogicalPlan::SubqueryAlias {
                    input: cte.clone(),
                    alias: name.to_string(),
                    schema: Box::new(VeinSchema {
                        fields: fields.clone(),
                    }),
                };
                scopes.push(Scope {
                    columns: fields,
                    aliases: vec![qualifier],
                });
                return Ok(plan);
            }

            let table = context.resolve_table(name).await?;

            let mut columns = Vec::new();
            for field in &table.schema.fields {
                columns.push(field.derived(
                    context.next_id(),
                    field.name.as_str(),
                    alias
                        .map(|a| a.to_string())
                        .or_else(|| Some(name.to_string())),
                ));
            }

            let qualifier = alias
                .map(|a| a.to_string())
                .or_else(|| Some(name.to_string()));
            scopes.push(Scope {
                columns: columns.clone(),
                aliases: vec![qualifier.clone().unwrap_or_default()],
            });

            Ok(LogicalPlan::Scan {
                table: table.clone(),
                projection: None,
                filters: vec![],
                schema: Box::new(VeinSchema { fields: columns }),
            })
        }
        TableSource::Subquery { query, alias } => {
            let mut subquery_scopes = ScopeStack::new();
            let inner = Box::pin(bind_query_internal(query, context, &mut subquery_scopes)).await?;

            // Rebind the inner fields with fresh IDs owned by the outer query,
            // then expose them under the subquery's alias.
            let mut fields = Vec::with_capacity(inner.schema().fields.len());
            for field in &inner.schema().fields {
                fields.push(field.derived(
                    context.next_id(),
                    field.name.as_str(),
                    alias.map(|a| a.to_string()),
                ));
            }

            let qualifier = alias.map(|a| a.to_string()).unwrap_or_default();
            scopes.push(Scope {
                columns: fields.clone(),
                aliases: vec![qualifier.clone()],
            });

            Ok(LogicalPlan::SubqueryAlias {
                input: Box::new(inner),
                alias: qualifier,
                schema: Box::new(VeinSchema { fields }),
            })
        }
        TableSource::ObjectStore {
            url,
            inline_options,
            alias,
        } => {
            let mut options: Vec<(String, String)> = inline_options
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            let table = context.resolve_table(url).await;

            let (schema, files, detected_format) = match table {
                Ok(tbl) => (
                    tbl.schema.clone(),
                    tbl.files.clone(),
                    tbl.format.as_ref().map(|f| f.to_string()),
                ),
                Err(_error)
                    if options
                        .iter()
                        .any(|(key, _)| key.eq_ignore_ascii_case("format")) =>
                {
                    (
                        Arc::new(VeinSchema { fields: vec![] }),
                        vec![url.to_string()],
                        None,
                    )
                }
                Err(error) => return Err(error),
            };

            let mut columns = Vec::with_capacity(schema.fields.len());
            for field in &schema.fields {
                let mut bound_field = field.as_ref().clone();
                bound_field.id = context.next_id();
                bound_field.qualifier = alias
                    .map(|a| a.to_string())
                    .or_else(|| Some(url.to_string()));
                columns.push(std::sync::Arc::new(bound_field));
            }

            let qualifier = alias
                .map(|a| a.to_string())
                .or_else(|| Some(url.to_string()));
            scopes.push(Scope {
                columns: columns.clone(),
                aliases: vec![qualifier.clone().unwrap_or_default()],
            });

            if let Some(fmt) = detected_format {
                if !options
                    .iter()
                    .any(|(k, _)| k.eq_ignore_ascii_case("format"))
                {
                    options.push(("format".to_string(), fmt));
                }
            }

            Ok(LogicalPlan::ExternalScan {
                url: url.to_string(),
                options,
                files,
                schema: Box::new(VeinSchema { fields: columns }),
            })
        }
    }
}

/// Binds a sequence of PipeOperators against an existing LogicalPlan
pub async fn bind_pipeline<'a>(
    mut plan: LogicalPlan,
    pipes: &[PipeOperator<'_>],
    context: &mut BinderContext<'a>,
    scopes: &mut ScopeStack,
) -> Result<LogicalPlan> {
    tracing::info!(
        target: "vein.audit",
        event = "binding_started",
        operator_count = pipes.len(),
        "binding_started"
    );
    for (operator_index, pipe) in pipes.iter().enumerate() {
        tracing::debug!(
            target: "vein.audit",
            event = "binding_operator",
            operator_index,
            operator = ?pipe,
            "binding_operator"
        );
        plan = match pipe {
            // Relational Core
            PipeOperator::Where { predicate } => {
                bind_where(plan, predicate, &mut *context, &mut *scopes).await?
            }
            PipeOperator::Qualify { predicate } => {
                bind_qualify(plan, predicate, &mut *context, &mut *scopes).await?
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
            PipeOperator::Pivot {
                aggregate,
                alias,
                value_column,
                values,
            } => {
                bind_pivot(
                    plan,
                    aggregate,
                    alias.as_deref(),
                    value_column,
                    values,
                    &mut *context,
                    &mut *scopes,
                )
                .await?
            }
            PipeOperator::Unpivot {
                value_name,
                name_name,
                columns,
                include_nulls,
            } => {
                bind_unpivot(
                    plan,
                    value_name,
                    name_name,
                    columns,
                    *include_nulls,
                    &mut *context,
                    &mut *scopes,
                )
                .await?
            }
            PipeOperator::TableSample { method, percent } => {
                bind_table_sample(plan, method, *percent)?
            }
            PipeOperator::Assert { condition, message } => {
                bind_assert(plan, condition, message, &mut *context, &mut *scopes).await?
            }
            PipeOperator::Call { name, args } => {
                bind_call(plan, name, args, &mut *context, &mut *scopes).await?
            }
            PipeOperator::With { ctes } => {
                bind_with(plan, ctes, &mut *context, &mut *scopes).await?
            }
            PipeOperator::Write { target } => bind_write(plan, target)?,
        };
    }

    tracing::info!(
        target: "vein.audit",
        event = "binding_completed",
        operator_count = pipes.len(),
        "binding_completed"
    );
    Ok(plan)
}
