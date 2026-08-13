use error::{Result, VeinError};
use parser::ast::pipe_op::OrderByExpr;
use crate::plan::{LogicalPlan, SortKey};
use crate::binder::context::BinderContext;
use crate::binder::scope::ScopeStack;
use crate::binder::expr::ExprBinder;

/// Binds a PipeOperator::OrderBy into a LogicalPlan::Sort
pub async fn bind_order_by<'a>(
    input: LogicalPlan,
    order_bys: &[OrderByExpr],
    context: &mut BinderContext<'a>,
    scopes: &mut ScopeStack<'a>,
) -> Result<LogicalPlan> {
    let mut expr_binder = ExprBinder::new(context, scopes);
    let mut sort_keys = Vec::new();
    
    for ob in order_bys {
        let expr = expr_binder.bind(&ob.expr).await?;
        sort_keys.push(SortKey {
            expr,
            asc: ob.asc,
            nulls_first: ob.nulls_first,
        });
    }

    let schema = input.schema().clone();
    Ok(LogicalPlan::Sort {
        input: Box::new(input),
        sort_keys,
        schema: Box::new(schema),
    })
}

/// Binds a PipeOperator::Limit into a LogicalPlan::Limit
pub fn bind_limit(input: LogicalPlan, limit: u64, offset: u64) -> Result<LogicalPlan> {
    let schema = input.schema().clone();
    Ok(LogicalPlan::Limit {
        input: Box::new(input),
        count: limit,
        offset,
        schema: Box::new(schema),
    })
}
