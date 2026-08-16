use plan::LogicalPlan;
use plan::PlanTraversal;
use error::{Result, VeinError};
use parser::ast::query::{Query, SetOpKind};

use crate::coerce::unify_types;
use crate::context::BinderContext;
use crate::pipe::query::{bind_pipeline, bind_query_internal};
use crate::scope::ScopeStack;

/// Binds a PipeOperator::SetOp into a LogicalPlan::SetOp
pub async fn bind_set_op<'a>(
    left: LogicalPlan,
    kind: &SetOpKind,
    right: &Query<'_>,
    all: bool,
    context: &mut BinderContext<'a>,
    _parent_scopes: &mut ScopeStack,
) -> Result<LogicalPlan> {
    // 1. Create a completely isolated ScopeStack for the right side (RisingWave approach)
    // We pass None as parent because SetOps shouldn't allow correlated references across the UNION.
    let mut isolated_scopes = ScopeStack::new();

    // 2. Call bind_query_internal to bind the entire right query (source + pipes)
    let right_plan = Box::pin(bind_query_internal(right, context, &mut isolated_scopes)).await?;

    // 3. Validate schemas match (same column count, each column type unifiable)
    let left_fields = left.schema().fields.clone();
    let right_fields = right_plan.schema().fields.clone();
    if left_fields.len() != right_fields.len() {
        return Err(VeinError::semantic(
            format!(
                "SetOp branches have different number of columns: left has {}, right has {}",
                left_fields.len(),
                right_fields.len()
            ),
            None,
        ));
    }

    // Coerce each column pair to a common type (rejecting incompatible
    // branches) and produce the output schema from the unified types.
    let mut fields = Vec::with_capacity(left_fields.len());
    for (lf, rf) in left_fields.iter().zip(right_fields.iter()) {
        match unify_types(&lf.data_type, &rf.data_type) {
            Ok(common) => {
                let mut col = lf.clone();
                col.data_type = common;
                fields.push(col);
            }
            Err(e) => {
                return Err(VeinError::semantic(
                    format!(
                        "SetOp column '{}' has incompatible types: {}",
                        lf.name, e
                    ),
                    None,
                ));
            }
        }
    }

    let schema = Box::new(plan::VeinSchema { fields });

    Ok(LogicalPlan::SetOp {
        kind: kind.clone(),
        all,
        left: Box::new(left),
        right: Box::new(right_plan),
        schema,
    })
}

/// Helper function to bind branch pipelines for Fork and Tee
async fn bind_branches<'a>(
    input: &LogicalPlan,
    pipelines: &[Vec<parser::ast::pipe_op::PipeOperator<'_>>],
    context: &mut BinderContext<'a>,
    parent_scopes: &mut ScopeStack,
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
    pipelines: &[Vec<parser::ast::pipe_op::PipeOperator<'_>>],
    context: &mut BinderContext<'a>,
    parent_scopes: &mut ScopeStack,
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
    pipelines: &[Vec<parser::ast::pipe_op::PipeOperator<'_>>],
    context: &mut BinderContext<'a>,
    parent_scopes: &mut ScopeStack,
) -> Result<LogicalPlan> {
    let schema = input.schema().clone();
    let branches = bind_branches(&input, pipelines, context, parent_scopes).await?;

    Ok(LogicalPlan::Tee {
        branches,
        schema: Box::new(schema),
    })
}
