use crate::context::BinderContext;
use crate::expr::ExprBinder;
use crate::pipe::query::bind_query_internal;
use crate::scope::{Scope, ScopeStack};
use crate::window::expr_contains_window_call;
use plan::LogicalPlan;
use plan::PlanTraversal;
use plan::{ResolvedColumn, ResolvedColumnRef, VeinDataType, VeinSchema};
use error::{Result, VeinError};
use parser::ast::expr::SpannedExpr;
use parser::ast::pipe_op::{CommonTableExpr, WriteTarget};

/// Binds a PipeOperator::Pivot
pub async fn bind_pivot<'a>(
    input: LogicalPlan,
    aggregate: &SpannedExpr<'_>,
    alias: Option<&str>,
    value_column: &str,
    values: &[SpannedExpr<'_>],
    context: &mut BinderContext<'a>,
    scopes: &mut ScopeStack,
) -> Result<LogicalPlan> {
    let value_col = scopes.resolve(None, value_column)?;
    let mut expr_binder = ExprBinder::new(context, scopes);
    let aggregate_expr = expr_binder.bind(aggregate).await?;
    let mut bound_values = Vec::new();
    for v in values {
        bound_values.push(expr_binder.bind(v).await?);
    }
    let pivot_type = aggregate_expr.data_type();
    let mut fields = input.schema().fields.clone();
    for (i, _) in values.iter().enumerate() {
        fields.push(std::sync::Arc::new(ResolvedColumn::new(
            context.next_id(),
            format!("{}_{}", alias.unwrap_or("pivot"), i),
            None,
            pivot_type.clone(),
        )
            .with_nullable(aggregate_expr.nullable())));
    }
    scopes.push(Scope::unqualified(fields.clone()));
    Ok(LogicalPlan::Pivot {
        input: Box::new(input),
        aggregate: aggregate_expr,
        alias: alias.map(|a| a.to_string()),
        value_column: value_col,
        values: bound_values,
        schema: Box::new(VeinSchema { fields }),
    })
}

/// Binds a PipeOperator::Unpivot
pub async fn bind_unpivot<'a>(
    input: LogicalPlan,
    value_name: &str,
    name_name: &str,
    columns: &[&str],
    include_nulls: bool,
    context: &mut BinderContext<'a>,
    scopes: &mut ScopeStack,
) -> Result<LogicalPlan> {
    let mut col_refs = Vec::new();
    for c in columns {
        col_refs.push(scopes.resolve(None, c)?);
    }
    let value_type = col_refs
        .first()
        .map(|c| c.data_type.clone())
        .unwrap_or(VeinDataType::Utf8);
    let mut fields: Vec<ResolvedColumnRef> = input
        .schema()
        .fields
        .iter()
        .filter(|f| !columns.contains(&f.name.as_str()))
        .cloned()
        .collect();
    fields.push(std::sync::Arc::new(ResolvedColumn::new(
        context.next_id(),
        name_name.to_string(),
        None,
        VeinDataType::Utf8,
    )));
    fields.push(std::sync::Arc::new(ResolvedColumn::new(
        context.next_id(),
        value_name.to_string(),
        None,
        value_type,
    )));
    scopes.push(Scope::unqualified(fields.clone()));
    Ok(LogicalPlan::Unpivot {
        input: Box::new(input),
        value_name: value_name.to_string(),
        name_name: name_name.to_string(),
        columns: col_refs,
        include_nulls,
        schema: Box::new(VeinSchema { fields }),
    })
}

/// Binds a PipeOperator::TableSample
pub fn bind_table_sample(input: LogicalPlan, method: &str, percent: f64) -> Result<LogicalPlan> {
    let schema = input.schema().clone();
    Ok(LogicalPlan::Sample {
        input: Box::new(input),
        method: method.to_string(),
        percent,
        schema: Box::new(schema),
    })
}

/// Binds a PipeOperator::Assert (Data Quality Validation)
pub async fn bind_assert<'a>(
    input: LogicalPlan,
    condition: &SpannedExpr<'_>,
    message: &Option<&str>,
    context: &mut BinderContext<'a>,
    scopes: &mut ScopeStack,
) -> Result<LogicalPlan> {
    let schema = input.schema().clone();
    let span = condition.span;
    let mut expr_binder = ExprBinder::new(context, scopes);
    let condition = expr_binder.bind(condition).await?;

    // Window functions are only allowed inside projection operators
    // (Google rule; QUALIFY-replacement is deferred).
    if expr_contains_window_call(&condition) {
        return Err(VeinError::semantic(
            "window functions are only allowed in SELECT and EXTEND pipe operators",
            Some(span),
        ));
    }

    Ok(LogicalPlan::Assert {
        input: Box::new(input),
        condition,
        message: message.map(|m| m.to_string()),
        schema: Box::new(schema),
    })
}

/// Binds a PipeOperator::Call (Executes a function directly on the stream)
pub async fn bind_call<'a>(
    input: LogicalPlan,
    name: &str,
    args: &[SpannedExpr<'_>],
    context: &mut BinderContext<'a>,
    scopes: &mut ScopeStack,
) -> Result<LogicalPlan> {
    let mut bound_args = Vec::with_capacity(args.len());
    for arg in args {
        bound_args.push(Box::pin(ExprBinder::new(context, scopes).bind(arg)).await?);
    }
    let schema = input.schema().clone();
    Ok(LogicalPlan::Call {
        input: Box::new(input),
        name: name.to_string(),
        args: bound_args,
        schema: Box::new(schema),
    })
}

/// Binds a PipeOperator::With (Common Table Expressions)
///
/// Each CTE body is bound in a fresh, non-correlated scope and registered in
/// `context.ctes` under its alias, so a later (nested) source can resolve it.
/// The `WITH` pipe itself is transparent: the input stream passes through
/// unchanged, and only the registered CTEs are the side effect.
pub async fn bind_with<'a>(
    input: LogicalPlan,
    ctes: &[CommonTableExpr<'_>],
    context: &mut BinderContext<'a>,
    _scopes: &mut ScopeStack,
) -> Result<LogicalPlan> {
    for cte in ctes {
        let mut cte_scopes = ScopeStack::new();
        let mut plan = Box::pin(bind_query_internal(&cte.query, context, &mut cte_scopes)).await?;

        // Apply explicit column names, if declared, renaming the output and
        // qualifying it under the CTE alias.
        if let Some(cols) = &cte.columns {
            let fields = plan.schema().fields.clone();
            if fields.len() != cols.len() {
                return Err(VeinError::semantic(
                    format!(
                        "CTE '{}' declares {} columns but its query returns {}",
                        cte.alias,
                        cols.len(),
                        fields.len()
                    ),
                    None,
                ));
            }
            let new_fields: Vec<ResolvedColumnRef> = fields
                .iter()
                .zip(cols.iter())
                .map(|(field, name)| field.derived(field.id, *name, Some(cte.alias.to_string())))
                .collect();
            plan = LogicalPlan::SubqueryAlias {
                input: Box::new(plan),
                alias: cte.alias.to_string(),
                schema: Box::new(VeinSchema { fields: new_fields }),
            };
        }

        context.ctes.insert(cte.alias.to_string(), Box::new(plan));
    }

    Ok(input)
}

/// Binds a PipeOperator::Write (Outputs stream to storage)
pub fn bind_write(input: LogicalPlan, target: &WriteTarget<'_>) -> Result<LogicalPlan> {
    let schema = input.schema().clone();
    Ok(LogicalPlan::Write {
        input: Box::new(input),
        target: target.table.to_string(),
        options: target
            .options
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        schema: Box::new(schema),
    })
}
