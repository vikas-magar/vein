use error::{Result, VeinError};
use parser::ast::pipe_op::{SelectItem, AggregateExpr, JoinClause};
use parser::ast::expr::SpannedExpr;
use crate::plan::{LogicalPlan, JoinType};
use crate::schema::VeinSchema;
use crate::binder::context::BinderContext;
use crate::binder::scope::ScopeStack;
use crate::binder::expr::ExprBinder;

/// Binds a PipeOperator::Where into a LogicalPlan::Filter
pub async fn bind_where<'a>(
    input: LogicalPlan,
    predicate: &SpannedExpr,
    context: &mut BinderContext<'a>,
    scopes: &mut ScopeStack<'a>,
) -> Result<LogicalPlan> {
    let mut expr_binder = ExprBinder::new(context, scopes);
    let bound_predicate = expr_binder.bind(predicate).await?;
    
    Ok(LogicalPlan::Filter {
        input: Box::new(input),
        predicate: bound_predicate,
    })
}

/// Binds a PipeOperator::Select into a LogicalPlan::Project
pub async fn bind_select<'a>(
    input: LogicalPlan,
    items: &[SelectItem],
    context: &mut BinderContext<'a>,
    scopes: &mut ScopeStack<'a>,
) -> Result<LogicalPlan> {
    let mut bound_exprs = Vec::new();
    let mut new_fields = Vec::new();
    
    for item in items {
        let bound_expr = ExprBinder::new(context, scopes).bind(&item.expr).await?;
        let col_name = item.alias.clone().unwrap_or_else(|| "unnamed_expr".to_string());
        
        new_fields.push(crate::schema::ResolvedColumn {
            id: scopes.next_id(),
            name: col_name,
            qualifier: None,
            data_type: bound_expr.data_type(),
            is_factorized: false,
        });
        bound_exprs.push(bound_expr);
    }
    
    // Select replaces the current scope with ONLY the selected columns
    scopes.push(crate::binder::scope::Scope {
        columns: new_fields.clone(),
        qualifier: None,
    });
    
    Ok(LogicalPlan::Project {
        input: Box::new(input),
        exprs: bound_exprs,
        schema: Box::new(VeinSchema { fields: new_fields }),
    })
}

/// Binds a PipeOperator::Aggregate into a LogicalPlan::Aggregate
pub async fn bind_aggregate<'a>(
    input: LogicalPlan,
    group_by: &[SpannedExpr],
    aggregates: &[AggregateExpr],
    context: &mut BinderContext<'a>,
    scopes: &mut ScopeStack<'a>,
) -> Result<LogicalPlan> {
    let mut bound_group_by = Vec::new();
    let mut new_fields = Vec::new();
    
    for gb in group_by {
        let bound_expr = ExprBinder::new(context, scopes).bind(gb).await?;
        new_fields.push(crate::schema::ResolvedColumn {
            id: scopes.next_id(),
            name: "group_by_col".to_string(), // In a real engine, try to derive from expr
            qualifier: None,
            data_type: bound_expr.data_type(),
            is_factorized: false,
        });
        bound_group_by.push(bound_expr);
    }

    let mut bound_aggs = Vec::new();
    for agg in aggregates {
        let bound_expr = ExprBinder::new(context, scopes).bind(&agg.expr).await?;
        let col_name = agg.alias.clone().unwrap_or_else(|| "agg_col".to_string());
        
        new_fields.push(crate::schema::ResolvedColumn {
            id: scopes.next_id(),
            name: col_name,
            qualifier: None,
            data_type: bound_expr.data_type(),
            is_factorized: false,
        });
        bound_aggs.push(bound_expr);
    }

    // Aggregate also replaces the current scope
    scopes.push(crate::binder::scope::Scope {
        columns: new_fields.clone(),
        qualifier: None,
    });

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
    join: &JoinClause,
    _context: &mut BinderContext<'a>,
    _scopes: &mut ScopeStack<'a>,
) -> Result<LogicalPlan> {
    // Requires recursive call to QueryBinder for the right side
    Err(VeinError::internal("Join binding not fully implemented yet"))
}

/// Binds a PipeOperator::Distinct into a LogicalPlan::Distinct
pub fn bind_distinct(input: LogicalPlan) -> Result<LogicalPlan> {
    let schema = input.schema().clone();
    Ok(LogicalPlan::Distinct {
        input: Box::new(input),
        schema: Box::new(schema),
    })
}
