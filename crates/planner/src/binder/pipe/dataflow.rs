use error::{Result, VeinError};
use parser::ast::query::{Query, SetOpKind};
use crate::plan::{LogicalPlan, SetOpKind as LogicalSetOpKind};

use crate::binder::pipe::query::{QueryBinder, bind_pipeline};
use crate::binder::context::BinderContext;
use crate::binder::scope::ScopeStack;

/// Binds a PipeOperator::SetOp into a LogicalPlan::SetOp
pub async fn bind_set_op<'a>(
    left: LogicalPlan,
    kind: &SetOpKind,
    right: &Query,
    all: bool,
    context: &mut BinderContext<'a>,
    _parent_scopes: &mut ScopeStack<'a>,
) -> Result<LogicalPlan> {
    // 1. Create a completely isolated ScopeStack for the right side (RisingWave approach)
    // We pass None as parent because SetOps shouldn't allow correlated references across the UNION.
    let mut isolated_scopes = ScopeStack::new(None);
    
    // 2. Call bind_pipeline directly instead of instantiating QueryBinder
    let right_plan = Box::pin(bind_pipeline(left.clone(), &right.pipes, context, &mut isolated_scopes)).await?;
    
    // 3. Validate schemas match
    if left.schema().fields.len() != right_plan.schema().fields.len() {
        return Err(VeinError::internal(format!("SetOp branches have different number of columns: left has {}, right has {}", left.schema().fields.len(), right_plan.schema().fields.len())));
    }
    
    let logical_kind = match kind {
        SetOpKind::Union => LogicalSetOpKind::Union,
        SetOpKind::Intersect => LogicalSetOpKind::Intersect,
        SetOpKind::Except => LogicalSetOpKind::Except,
    };
    
    let schema = left.schema().clone();
    
    Ok(LogicalPlan::SetOp {
        kind: logical_kind,
        all,
        left: Box::new(left),
        right: Box::new(right_plan),
        schema: Box::new(schema),
    })
}

/// Binds a PipeOperator::Fork into a LogicalPlan::Fork
pub async fn bind_fork<'a>(
    input: LogicalPlan,
    pipelines: &[Vec<parser::ast::pipe_op::PipeOperator>],
    context: &mut BinderContext<'a>,
    parent_scopes: &mut ScopeStack<'a>,
) -> Result<LogicalPlan> {
    let mut branches = Vec::new();
    let schema = input.schema().clone();
    
    for pipe_chain in pipelines {
        // Each branch of a Fork needs its own scope stack derived from the parent
        // because mutations in one branch shouldn't affect the other.
        let mut branch_scopes = parent_scopes.clone();
        // Use standalone bind_pipeline with Box::pin to resolve async recursion E0733
        let branch_plan = Box::pin(bind_pipeline(input.clone(), pipe_chain, context, &mut branch_scopes)).await?;
        branches.push(Box::new(branch_plan));
    }
    
    Ok(LogicalPlan::Fork {
        branches,
        schema: Box::new(schema),
    })
}

/// Binds a PipeOperator::Tee into a LogicalPlan::Tee
pub async fn bind_tee<'a>(
    input: LogicalPlan,
    pipelines: &[Vec<parser::ast::pipe_op::PipeOperator>],
    context: &mut BinderContext<'a>,
    parent_scopes: &mut ScopeStack<'a>,
) -> Result<LogicalPlan> {
    let mut branches = Vec::new();
    let schema = input.schema().clone();
    
    for pipe_chain in pipelines {
        let mut branch_scopes = parent_scopes.clone();
        let branch_plan = Box::pin(bind_pipeline(input.clone(), pipe_chain, context, &mut branch_scopes)).await?;
        branches.push(Box::new(branch_plan));
    }
    
    Ok(LogicalPlan::Tee {
        branches,
        schema: Box::new(schema),
    })
}
