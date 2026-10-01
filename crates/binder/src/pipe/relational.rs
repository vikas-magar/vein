use crate::context::BinderContext;
use crate::expr::{ExprBinder, derive_column_name};
use crate::pipe::query::bind_query_internal;
use crate::scope::{Scope, ScopeStack};
use crate::star::expand_projection_items;
use crate::window::{expr_contains_window_call, split_window_items, wrap_window};
use plan::LogicalPlan;
use plan::PlanTraversal;
use plan::{LogicalExpr, ResolvedColumn, ResolvedColumnRef, VeinDataType, VeinSchema};
use error::{Result, VeinError};
use parser::ast::expr::{BinaryOp, SpannedExpr};
use parser::ast::pipe_op::{AggregateExpr, JoinClause, SelectItem};

/// Binds a PipeOperator::Where into a LogicalPlan::Filter
pub async fn bind_where<'a>(
    input: LogicalPlan,
    predicate: &SpannedExpr<'_>,
    context: &mut BinderContext<'a>,
    scopes: &mut ScopeStack,
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

/// Binds a PipeOperator::Qualify into a `LogicalPlan::Window` followed by a
/// `LogicalPlan::Filter`, filtering on the results of a window function
/// (GoogleSQL parity). Equivalent to `SELECT ... WHERE <window-expr> ...`.
pub async fn bind_qualify<'a>(
    input: LogicalPlan,
    predicate: &SpannedExpr<'_>,
    context: &mut BinderContext<'a>,
    scopes: &mut ScopeStack,
) -> Result<LogicalPlan> {
    let mut expr_binder = ExprBinder::new(context, scopes);
    let bound_predicate = expr_binder.bind(predicate).await?;

    if !expr_contains_window_call(&bound_predicate) {
        return Err(VeinError::semantic(
            "QUALIFY predicate must contain a window function",
            Some(predicate.span),
        ));
    }

    // Lift the window calls out of the predicate into a Window node below,
    // replacing each call with a placeholder column the Filter can reference.
    let (plain_items, window_exprs) =
        split_window_items(vec![(bound_predicate, None)], context, scopes)?;
    let input = wrap_window(input, window_exprs)?;

    let predicate = plain_items
        .into_iter()
        .next()
        .map(|(expr, _)| expr)
        .ok_or_else(|| VeinError::internal("QUALIFY produced no predicate"))?;

    Ok(LogicalPlan::Filter {
        input: Box::new(input),
        predicate,
    })
}

/// Binds a PipeOperator::Select into a LogicalPlan::Project
pub async fn bind_select<'a>(
    input: LogicalPlan,
    items: &[SelectItem<'_>],
    context: &mut BinderContext<'a>,
    scopes: &mut ScopeStack,
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
        new_fields.push(std::sync::Arc::new(ResolvedColumn::new(
            context.next_id(),
            name.unwrap_or_else(|| "_col".to_string()),
            None,
            expr.data_type(),
        )));
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
    scopes: &mut ScopeStack,
) -> Result<LogicalPlan> {
    let mut bound_group_by = Vec::new();
    let mut new_fields: Vec<ResolvedColumnRef> = Vec::new();

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
        new_fields.push(std::sync::Arc::new(ResolvedColumn::new(
            context.next_id(),
            name,
            None,
            bound_expr.data_type(),
        )));
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

        new_fields.push(std::sync::Arc::new(ResolvedColumn::new(
            context.next_id(),
            col_name,
            None,
            bound_expr.data_type(),
        )));
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
    input: LogicalPlan,
    join: &JoinClause<'_>,
    context: &mut BinderContext<'a>,
    scopes: &mut ScopeStack,
) -> Result<LogicalPlan> {
    // 1. Bind the right-hand query in an isolated scope stack (no correlation
    //    across the join, mirroring set operations). Its columns carry the
    //    right side's qualifiers (table name / alias / subquery alias).
    let mut right_scopes = ScopeStack::new();
    let right = Box::pin(bind_query_internal(&join.right, context, &mut right_scopes)).await?;

    let left_fields = input.schema().fields.clone();
    let right_fields = right.schema().fields.clone();

    // 2. Merge both sides into a single scope so the ON/USING predicate and
    //    any downstream pipe can resolve columns from either side. The scope
    //    answers to every qualifier present on each side's columns.
    let mut all_columns = left_fields.clone();
    all_columns.extend(right_fields.clone());
    let mut aliases: Vec<String> = Vec::new();
    for field in &all_columns {
        if let Some(q) = &field.qualifier
            && !aliases.contains(q)
        {
            aliases.push(q.clone());
        }
    }
    let mut merged = Scope {
        columns: all_columns,
        aliases,
    };

    // 3. Build the join predicate and output schema.
    let (on, using) = match (&join.predicate, join.using.as_slice()) {
        // ON predicate: bind against the merged scope.
        (Some(predicate), _) => {
            scopes.push(merged.clone());
            let mut expr_binder = ExprBinder::new(context, scopes);
            let on = expr_binder.bind(predicate).await?;
            if expr_contains_window_call(&on) {
                return Err(VeinError::semantic(
                    "window functions are not allowed in JOIN ON",
                    Some(predicate.span),
                ));
            }
            scopes.pop();
            (Some(on), vec![])
        }
        // USING columns: synthesize `left.col = right.col`, merge the output
        // column so it appears exactly once.
        (None, using_cols) if !using_cols.is_empty() => {
            let mut pred_parts = Vec::new();
            let mut using = Vec::new();
            let mut right_out = right_fields.clone();
            for col in using_cols {
                let left_col = left_fields
                    .iter()
                    .find(|c| c.name == *col)
                    .ok_or_else(|| {
                        VeinError::semantic(
                            format!("USING column '{col}' not found on the left side"),
                            None,
                        )
                    })?;
                let right_col = right_fields
                    .iter()
                    .find(|c| c.name == *col)
                    .ok_or_else(|| {
                        VeinError::semantic(
                            format!("USING column '{col}' not found on the right side"),
                            None,
                        )
                    })?;
                pred_parts.push(LogicalExpr::BinaryOp {
                    left: Box::new(LogicalExpr::Column(left_col.clone())),
                    op: BinaryOp::Eq,
                    right: Box::new(LogicalExpr::Column(right_col.clone())),
                    return_type: VeinDataType::Boolean,
                });
                using.push(left_col.clone());
                right_out.retain(|c| c.name != *col);
            }
            let on = match pred_parts.len() {
                1 => pred_parts.pop(),
                _n => Some(pred_parts.into_iter().reduce(|l, r| LogicalExpr::BinaryOp {
                    left: Box::new(l),
                    op: BinaryOp::And,
                    right: Box::new(r),
                    return_type: VeinDataType::Boolean,
                }).expect("at least one USING column (n={n})")),
            };
            // Rebuild the merged schema with the USING columns de-duplicated.
            merged = Scope {
                columns: {
                    let mut c = left_fields.clone();
                    c.extend(right_out.clone());
                    c
                },
                aliases: merged.aliases.clone(),
            };
            (on, using)
        }
        // CROSS join.
        (None, _) => (None, vec![]),
    };

    // 4. Output schema: left ++ right (USING de-duplicates the join columns).
    let output_fields = merged.columns.clone();

    // 5. Push the merged scope so downstream pipes see both sides.
    scopes.push(merged);

    Ok(LogicalPlan::Join {
        join_type: join.join_type,
        left: Box::new(input),
        right: Box::new(right),
        on,
        using,
        schema: Box::new(VeinSchema { fields: output_fields }),
    })
}

/// Binds a PipeOperator::Distinct into a LogicalPlan::Distinct
pub fn bind_distinct(input: LogicalPlan) -> Result<LogicalPlan> {
    let schema = input.schema().clone();
    Ok(LogicalPlan::Distinct {
        input: Box::new(input),
        schema: Box::new(schema),
    })
}
