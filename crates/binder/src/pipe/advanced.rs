use crate::context::BinderContext;
use crate::expr::ExprBinder;
use crate::scope::ScopeStack;
use crate::window::expr_contains_window_call;
use plan::LogicalPlan;
use plan::PlanTraversal;
use error::{Result, VeinError};
use parser::ast::expr::SpannedExpr;
use parser::ast::pipe_op::{CommonTableExpr, WriteTarget};

/// Binds a PipeOperator::Pivot
pub fn bind_pivot(_input: LogicalPlan) -> Result<LogicalPlan> {
    Err(VeinError::planning(
        "Pivot operator not yet fully implemented", None,
    ))
}

/// Binds a PipeOperator::Unpivot
pub fn bind_unpivot(_input: LogicalPlan) -> Result<LogicalPlan> {
    Err(VeinError::planning(
        "Unpivot operator not yet fully implemented", None,
    ))
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
    scopes: &mut ScopeStack<'a>,
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
    _input: LogicalPlan,
    _name: &str,
    _args: &[SpannedExpr<'_>],
    _context: &mut BinderContext<'a>,
    _scopes: &mut ScopeStack<'a>,
) -> Result<LogicalPlan> {
    Err(VeinError::planning(
        "Call operator not yet fully implemented", None,
    ))
}

/// Binds a PipeOperator::With (Common Table Expressions)
pub fn bind_with(_input: LogicalPlan, _ctes: &[CommonTableExpr<'_>]) -> Result<LogicalPlan> {
    Err(VeinError::planning(
        "CTE (With) operator not yet fully implemented in pipe scope", None,
    ))
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
