use crate::context::BinderContext;
use crate::expr::ExprBinder;
use crate::scope::{Scope, ScopeStack};
use crate::star::expand_projection_items;
use crate::window::{expr_contains_window_call, split_window_items, wrap_window};
use plan::LogicalExpr;
use plan::LogicalPlan;
use plan::PlanTraversal;
use plan::{ResolvedColumn, VeinSchema};
use error::{Result, VeinError};
use parser::ast::expr::SpannedExpr;
use parser::ast::pipe_op::SelectItem;

/// Binds a PipeOperator::Extend (Adds columns without dropping existing ones)
pub async fn bind_extend<'a>(
    input: LogicalPlan,
    items: &[SelectItem<'_>],
    context: &mut BinderContext<'a>,
    scopes: &mut ScopeStack,
) -> Result<LogicalPlan> {
    // 1. Expand items like SELECT, keeping the input columns as passthroughs.
    let bound_items = expand_projection_items(items, context, scopes).await?;

    // 2. Lift window calls into a Window node below the projection.
    let (plain_items, window_exprs) = split_window_items(bound_items, context, scopes)?;
    // The passthrough base is the pre-wrap input: the Window node's schema
    // already appends the window outputs, which would duplicate them here.
    let base_fields = input.schema().fields.clone();
    let input = wrap_window(input, window_exprs)?;

    // 3. Project = input columns (passthrough) + the extended columns.
    let mut bound_exprs = base_fields
        .iter()
        .cloned()
        .map(LogicalExpr::Column)
        .collect::<Vec<_>>();
    let mut new_fields = base_fields;

    for (expr, name) in plain_items {
        // Window placeholder columns ARE the Window node's output columns;
        // reuse them so the extended schema references stay consistent. A
        // bare column passthrough keeps its identity; an aliased item keeps
        // its underlying column id (same value) but applies the alias and
        // resets the qualifier, matching projection semantics.
        let field = match &expr {
            LogicalExpr::Column(col) => match name {
                Some(alias) => ResolvedColumn {
                    id: col.id,
                    name: alias.to_string(),
                    qualifier: None,
                    data_type: col.data_type.clone(),
                    is_factorized: col.is_factorized,
                    nullable: col.nullable,
                },
                None => col.clone(),
            },
            _ => ResolvedColumn::new(
                context.next_id(),
                name.unwrap_or_else(|| "_col".to_string()),
                None,
                expr.data_type(),
            ),
        };
        new_fields.push(field);
        bound_exprs.push(expr);
    }

    // 4. Make the extended schema available to downstream pipe operators.
    scopes.push(Scope::unqualified(new_fields.clone()));

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
    scopes: &mut ScopeStack,
) -> Result<LogicalPlan> {
    let mut bound_exprs = Vec::new();
    let mut new_fields = input.schema().fields.clone();

    // Pass through or overwrite existing columns
    for col in &input.schema().fields {
        if let Some((_, expr)) = assignments
            .iter()
            .find(|(name, _)| *name == col.name.as_str())
        {
            let bound_expr = ExprBinder::new(context, scopes).bind(expr).await?;
            // Window functions are only allowed inside projection operators
            // (deferred; Semantic error for now).
            if expr_contains_window_call(&bound_expr) {
                return Err(VeinError::semantic(
                    "window functions are not allowed inside SET",
                    Some(expr.span),
                ));
            }

            let new_col = ResolvedColumn {
                id: context.next_id(),
                name: col.name.clone(),
                qualifier: col.qualifier.clone(),
                data_type: bound_expr.data_type(),
                is_factorized: false,
                nullable: col.nullable,
            };

            if let Some(pos) = new_fields.iter().position(|f| f.name == col.name) {
                new_fields[pos] = new_col;
            }
            bound_exprs.push(bound_expr);
        } else {
            bound_exprs.push(LogicalExpr::Column(col.clone()));
        }
    }

    // Check if any assignment targeted a non-existent column
    for (name, _) in assignments {
        if !input
            .schema()
            .fields
            .iter()
            .any(|f| f.name.as_str() == *name)
        {
            return Err(VeinError::semantic(
                format!("SET operator cannot find column '{name}'"),
                None,
            ));
        }
    }

    scopes.push(crate::scope::Scope::unqualified(new_fields.clone()));

    // Translates to a projection that overwrites specific columns
    Ok(LogicalPlan::Project {
        input: Box::new(input),
        exprs: bound_exprs,
        schema: Box::new(VeinSchema { fields: new_fields }),
    })
}

/// Binds a PipeOperator::Drop (Removes columns from the stream)
pub fn bind_drop(
    input: LogicalPlan,
    columns: &[&str],
    scopes: &mut ScopeStack,
) -> Result<LogicalPlan> {
    // Reject unknown columns, mirroring SET: silently dropping nothing for a
    // misspelled name would mask a user error.
    for name in columns {
        if !input.schema().fields.iter().any(|f| f.name.as_str() == *name) {
            return Err(VeinError::semantic(
                format!("DROP operator cannot find column '{name}'"),
                None,
            ));
        }
    }

    let mut bound_exprs = Vec::new();
    let mut new_fields = Vec::new();

    for col in &input.schema().fields {
        if !columns.contains(&col.name.as_str()) {
            bound_exprs.push(LogicalExpr::Column(col.clone()));
            new_fields.push(col.clone());
        }
    }

    scopes.push(crate::scope::Scope::unqualified(new_fields.clone()));

    // Translates to a projection that includes everything EXCEPT the dropped columns
    Ok(LogicalPlan::Project {
        input: Box::new(input),
        exprs: bound_exprs,
        schema: Box::new(VeinSchema { fields: new_fields }),
    })
}

/// Binds a PipeOperator::Rename (Renames columns in the stream)
pub fn bind_rename(
    input: LogicalPlan,
    renames: &[(&str, &str)],
    scopes: &mut ScopeStack,
) -> Result<LogicalPlan> {
    let mut bound_exprs = Vec::new();
    let mut new_fields = input.schema().fields.clone();

    for col in &mut new_fields {
        if let Some((_, new_name)) = renames.iter().find(|(old, _)| *old == col.name.as_str()) {
            col.name = (*new_name).to_string();
        }
        bound_exprs.push(LogicalExpr::Column(col.clone()));
    }

    scopes.push(crate::scope::Scope::unqualified(new_fields.clone()));

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
