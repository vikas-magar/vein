use error::{Result, VeinError};
use parser::ast::pipe_op::{WriteTarget, CommonTableExpr};
use parser::ast::expr::SpannedExpr;
use crate::plan::LogicalPlan;
use crate::plan::traversal::PlanTraversal;
use crate::binder::context::BinderContext;
use crate::binder::scope::ScopeStack;

/// Binds a PipeOperator::Pivot
pub fn bind_pivot(_input: LogicalPlan) -> Result<LogicalPlan> {
    Err(VeinError::internal("Pivot operator not yet fully implemented"))
}

/// Binds a PipeOperator::Unpivot
pub fn bind_unpivot(_input: LogicalPlan) -> Result<LogicalPlan> {
    Err(VeinError::internal("Unpivot operator not yet fully implemented"))
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
pub fn bind_assert(input: LogicalPlan, condition: &str, message: &Option<&str>) -> Result<LogicalPlan> {
    let schema = input.schema().clone();
    Ok(LogicalPlan::Assert {
        input: Box::new(input),
        condition: condition.to_string(),
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
    Err(VeinError::internal("Call operator not yet fully implemented"))
}

/// Binds a PipeOperator::With (Common Table Expressions)
pub fn bind_with(_input: LogicalPlan, _ctes: &[CommonTableExpr<'_>]) -> Result<LogicalPlan> {
    Err(VeinError::internal("CTE (With) operator not yet fully implemented in pipe scope"))
}

/// Binds a PipeOperator::Write (Outputs stream to storage)
pub fn bind_write(input: LogicalPlan, target: &WriteTarget<'_>) -> Result<LogicalPlan> {
    let schema = input.schema().clone();
    Ok(LogicalPlan::Write {
        input: Box::new(input),
        target: target.table.to_string(),
        options: target.options.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
        schema: Box::new(schema),
    })
}
