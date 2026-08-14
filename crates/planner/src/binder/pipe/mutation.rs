use error::{Result, VeinError};
use parser::ast::pipe_op::SelectItem;
use parser::ast::expr::SpannedExpr;
use crate::plan::LogicalPlan;
use crate::schema::VeinSchema;
use crate::binder::context::BinderContext;
use crate::binder::scope::ScopeStack;
use crate::plan::traversal::PlanTraversal;
use crate::binder::expr::ExprBinder;

/// Binds a PipeOperator::Extend (Adds columns without dropping existing ones)
pub async fn bind_extend<'a>(
    input: LogicalPlan,
    items: &[SelectItem<'_>],
    context: &mut BinderContext<'a>,
    scopes: &mut ScopeStack<'a>,
) -> Result<LogicalPlan> {
    let mut bound_exprs = Vec::new();
    let mut new_fields = input.schema().fields.clone();
    
    // 1. Push all existing columns from the input schema
    for col in &input.schema().fields {
        bound_exprs.push(crate::expr::LogicalExpr::Column(col.clone()));
    }
    
    // 2. Push the new extended columns
    for item in items {
        let bound_expr = ExprBinder::new(context, scopes).bind(&item.expr).await?;
        let col_name = item.alias.map(|a| a.to_string()).unwrap_or_else(|| "unnamed_expr".to_string());
        
        let new_col = crate::schema::ResolvedColumn {
            id: context.next_id(),
            name: col_name,
            qualifier: None,
            data_type: bound_expr.data_type(),
            is_factorized: false,
        };
        
        new_fields.push(new_col);
        bound_exprs.push(bound_expr);
    }
    
    // 3. Make the new extended schema available to downstream pipe operators
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

/// Binds a PipeOperator::Set (Updates existing columns)
pub async fn bind_set<'a>(
    input: LogicalPlan,
    assignments: &[(&str, SpannedExpr<'_>)],
    context: &mut BinderContext<'a>,
    scopes: &mut ScopeStack<'a>,
) -> Result<LogicalPlan> {
    let mut bound_exprs = Vec::new();
    let mut new_fields = input.schema().fields.clone();
    
    // Pass through or overwrite existing columns
    for col in &input.schema().fields {
        if let Some((_, expr)) = assignments.iter().find(|(name, _)| *name == col.name.as_str()) {
            let bound_expr = ExprBinder::new(context, scopes).bind(expr).await?;
            
            let new_col = crate::schema::ResolvedColumn {
                id: context.next_id(),
                name: col.name.clone(),
                qualifier: col.qualifier.clone(),
                data_type: bound_expr.data_type(),
                is_factorized: false,
            };
            
            if let Some(pos) = new_fields.iter().position(|f| f.name == col.name) {
                new_fields[pos] = new_col;
            }
            bound_exprs.push(bound_expr);
        } else {
            bound_exprs.push(crate::expr::LogicalExpr::Column(col.clone()));
        }
    }
    
    // Check if any assignment targeted a non-existent column
    for (name, _) in assignments {
        if !input.schema().fields.iter().any(|f| f.name.as_str() == *name) {
            return Err(VeinError::internal(format!("SET operator cannot find column '{}'", name)));
        }
    }

    scopes.push(crate::binder::scope::Scope {
        columns: new_fields.clone(),
        qualifier: None,
    });
    
    // Translates to a projection that overwrites specific columns
    Ok(LogicalPlan::Project {
        input: Box::new(input),
        exprs: bound_exprs,
        schema: Box::new(VeinSchema { fields: new_fields }),
    })
}

/// Binds a PipeOperator::Drop (Removes columns from the stream)
pub fn bind_drop<'a>(
    input: LogicalPlan,
    columns: &[&str],
    scopes: &mut ScopeStack<'a>,
) -> Result<LogicalPlan> {
    let mut bound_exprs = Vec::new();
    let mut new_fields = Vec::new();
    
    for col in &input.schema().fields {
        if !columns.contains(&col.name.as_str()) {
            bound_exprs.push(crate::expr::LogicalExpr::Column(col.clone()));
            new_fields.push(col.clone());
        }
    }

    scopes.push(crate::binder::scope::Scope {
        columns: new_fields.clone(),
        qualifier: None,
    });

    // Translates to a projection that includes everything EXCEPT the dropped columns
    Ok(LogicalPlan::Project {
        input: Box::new(input),
        exprs: bound_exprs,
        schema: Box::new(VeinSchema { fields: new_fields }),
    })
}

/// Binds a PipeOperator::Rename (Renames columns in the stream)
pub fn bind_rename<'a>(
    input: LogicalPlan,
    renames: &[(&str, &str)],
    scopes: &mut ScopeStack<'a>,
) -> Result<LogicalPlan> {
    let mut bound_exprs = Vec::new();
    let mut new_fields = input.schema().fields.clone();
    
    for col in &mut new_fields {
        if let Some((_, new_name)) = renames.iter().find(|(old, _)| *old == col.name.as_str()) {
            col.name = (*new_name).to_string();
        }
        bound_exprs.push(crate::expr::LogicalExpr::Column(col.clone()));
    }

    scopes.push(crate::binder::scope::Scope {
        columns: new_fields.clone(),
        qualifier: None,
    });

    // Translates to a projection with updated column names
    Ok(LogicalPlan::Project {
        input: Box::new(input),
        exprs: bound_exprs,
        schema: Box::new(VeinSchema { fields: new_fields }),
    })
}

/// Binds a PipeOperator::As (Aliases the entire data stream/subquery)
pub fn bind_as(input: LogicalPlan, alias: &str) -> Result<LogicalPlan> {
    let schema = input.schema().clone();
    Ok(LogicalPlan::SubqueryAlias {
        input: Box::new(input),
        alias: alias.to_string(),
        schema: Box::new(schema),
    })
}
