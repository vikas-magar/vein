use crate::plan::LogicalPlan;
use crate::plan::traversal::PlanTraversal;
use error::{Result, VeinError};
use parser::ast::query::{Query, SetOpKind};

use crate::binder::context::BinderContext;
use crate::binder::pipe::query::{bind_pipeline, bind_query_internal};
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

    // 2. Call bind_query_internal to bind the entire right query (source + pipes)
    let right_plan = Box::pin(bind_query_internal(right, context, &mut isolated_scopes)).await?;

    // 3. Validate schemas match
    if left.schema().fields.len() != right_plan.schema().fields.len() {
        return Err(VeinError::internal(format!(
            "SetOp branches have different number of columns: left has {}, right has {}",
            left.schema().fields.len(),
            right_plan.schema().fields.len()
        )));
    }

    let schema = left.schema().clone();

    Ok(LogicalPlan::SetOp {
        kind: kind.clone(),
        all,
        left: Box::new(left),
        right: Box::new(right_plan),
        schema: Box::new(schema),
    })
}

/// Helper function to bind branch pipelines for Fork and Tee
async fn bind_branches<'a>(
    input: &LogicalPlan,
    pipelines: &[Vec<parser::ast::pipe_op::PipeOperator>],
    context: &mut BinderContext<'a>,
    parent_scopes: &mut ScopeStack<'a>,
) -> Result<Vec<Box<LogicalPlan>>> {
    let mut branches = Vec::new();
    for pipe_chain in pipelines {
        // Each branch needs its own scope stack derived from the parent
        let mut branch_scopes = parent_scopes.clone();
        let branch_plan = Box::pin(bind_pipeline(
            input.clone(),
            pipe_chain,
            context,
            &mut branch_scopes,
        ))
        .await?;
        branches.push(Box::new(branch_plan));
    }
    Ok(branches)
}

/// Binds a PipeOperator::Fork into a LogicalPlan::Fork
pub async fn bind_fork<'a>(
    input: LogicalPlan,
    pipelines: &[Vec<parser::ast::pipe_op::PipeOperator>],
    context: &mut BinderContext<'a>,
    parent_scopes: &mut ScopeStack<'a>,
) -> Result<LogicalPlan> {
    let schema = input.schema().clone();
    let branches = bind_branches(&input, pipelines, context, parent_scopes).await?;

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
    let schema = input.schema().clone();
    let branches = bind_branches(&input, pipelines, context, parent_scopes).await?;

    Ok(LogicalPlan::Tee {
        branches,
        schema: Box::new(schema),
    })
}
