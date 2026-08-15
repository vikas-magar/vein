use crate::context::BinderContext;
use crate::expr::{ExprBinder, derive_column_name};
use crate::scope::{Scope, ScopeStack};
use crate::star::expand_projection_items;
use crate::window::{expr_contains_window_call, split_window_items, wrap_window};
use plan::LogicalPlan;
use plan::PlanTraversal;
use plan::{ResolvedColumn, VeinSchema};
use error::{Result, VeinError};
use parser::ast::expr::SpannedExpr;
use parser::ast::pipe_op::{AggregateExpr, JoinClause, SelectItem};

/// Binds a PipeOperator::Where into a LogicalPlan::Filter
pub async fn bind_where<'a>(
    input: LogicalPlan,
    predicate: &SpannedExpr<'_>,
    context: &mut BinderContext<'a>,
    scopes: &mut ScopeStack<'a>,
) -> Result<LogicalPlan> {
    let mut expr_binder = ExprBinder::new(context, scopes);
    let bound_predicate = expr_binder.bind(predicate).await?;

    // Window functions are only allowed inside projection operators
    // (Google rule; QUALIFY-replacement is deferred).
    if expr_contains_window_call(&bound_predicate) {
        return Err(VeinError::semantic(
            "window functions are only allowed in SELECT and EXTEND pipe operators",
            Some(predicate.span),
        ));
    }

    Ok(LogicalPlan::Filter {
        input: Box::new(input),
        predicate: bound_predicate,
    })
}

/// Binds a PipeOperator::Select into a LogicalPlan::Project
pub async fn bind_select<'a>(
    input: LogicalPlan,
    items: &[SelectItem<'_>],
    context: &mut BinderContext<'a>,
    scopes: &mut ScopeStack<'a>,
) -> Result<LogicalPlan> {
    // 1. Expand items: wildcards → one placeholder column per visible column;
    //    other items bind via ExprBinder. Each pair carries its output name.
    let bound_items = expand_projection_items(items, context, scopes).await?;

    // 2. Lift window calls into a Window node below the projection.
    let (plain_items, window_exprs) = split_window_items(bound_items, context, scopes)?;
    let input = wrap_window(input, window_exprs)?;

    // 3. Build the projection on top (SELECT replaces the input columns).
    let mut bound_exprs = Vec::with_capacity(plain_items.len());
    let mut new_fields = Vec::with_capacity(plain_items.len());
    for (expr, name) in plain_items {
        new_fields.push(ResolvedColumn::new(
            context.next_id(),
            name.unwrap_or_else(|| "_col".to_string()),
            None,
            expr.data_type(),
        ));
        bound_exprs.push(expr);
    }

    // Select replaces the current scope with ONLY the selected columns.
    scopes.push(Scope::unqualified(new_fields.clone()));

    Ok(LogicalPlan::Project {
        input: Box::new(input),
        exprs: bound_exprs,
        schema: Box::new(VeinSchema { fields: new_fields }),
    })
}

/// Binds a PipeOperator::Aggregate into a LogicalPlan::Aggregate
pub async fn bind_aggregate<'a>(
    input: LogicalPlan,
    group_by: &[parser::ast::pipe_op::OrderByExpr<'_>],
    aggregates: &[AggregateExpr<'_>],
    context: &mut BinderContext<'a>,
    scopes: &mut ScopeStack<'a>,
) -> Result<LogicalPlan> {
    let mut bound_group_by = Vec::new();
    let mut new_fields = Vec::new();

    for gb in group_by {
        let bound_expr = ExprBinder::new(context, scopes).bind(&gb.expr).await?;
        // Window functions are only allowed inside projection operators
        // (Google: use EXTEND before AGGREGATE instead).
        if expr_contains_window_call(&bound_expr) {
            return Err(VeinError::semantic(
                "window functions are not allowed inside AGGREGATE; use EXTEND before AGGREGATE",
                Some(gb.expr.span),
            ));
        }
        let name = derive_column_name(&gb.expr.node);
        new_fields.push(ResolvedColumn::new(
            context.next_id(),
            name,
            None,
            bound_expr.data_type(),
        ));
        bound_group_by.push(bound_expr);
    }

    let mut bound_aggs = Vec::new();
    for agg in aggregates {
        let bound_expr = ExprBinder::new(context, scopes).bind(&agg.expr).await?;
        if expr_contains_window_call(&bound_expr) {
            return Err(VeinError::semantic(
                "window functions are not allowed inside AGGREGATE; use EXTEND before AGGREGATE",
                Some(agg.expr.span),
            ));
        }
        let col_name = agg
            .alias
            .map(|a| a.to_string())
            .unwrap_or_else(|| derive_column_name(&agg.expr.node));

        new_fields.push(ResolvedColumn::new(
            context.next_id(),
            col_name,
            None,
            bound_expr.data_type(),
        ));
        bound_aggs.push(bound_expr);
    }

    // Aggregate also replaces the current scope
    scopes.push(crate::scope::Scope::unqualified(new_fields.clone()));

    Ok(LogicalPlan::Aggregate {
        input: Box::new(input),
        grouping_sets: vec![bound_group_by],
        aggs: bound_aggs,
        schema: Box::new(VeinSchema { fields: new_fields }),
    })
}

/// Binds a PipeOperator::Join into a LogicalPlan::Join
pub async fn bind_join<'a>(
    _input: LogicalPlan,
    _join: &JoinClause<'_>,
    _context: &mut BinderContext<'a>,
    _scopes: &mut ScopeStack<'a>,
) -> Result<LogicalPlan> {
// Requires recursive call to QueryBinder for the right side
        Err(VeinError::planning("Join binding not fully implemented yet", None))
}

/// Binds a PipeOperator::Distinct into a LogicalPlan::Distinct
pub fn bind_distinct(input: LogicalPlan) -> Result<LogicalPlan> {
    let schema = input.schema().clone();
    Ok(LogicalPlan::Distinct {
        input: Box::new(input),
        schema: Box::new(schema),
    })
}
