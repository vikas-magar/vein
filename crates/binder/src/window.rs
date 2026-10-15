//! Window function lifting for projection operators (SELECT/EXTEND).
//!
//! Binding model (Google parity, paper §4.1.1 footnote): a pipe operator may
//! reference window functions in its item list; each bounds to
//! `LogicalExpr::WindowCall`, then all calls in the operator are lifted into a
//! single `LogicalPlan::Window` below the operator's projection. The
//! projection keeps referencing the window outputs through placeholder
//! `Column` nodes, so downstream consumers (and the schema) stay consistent.

use crate::context::BinderContext;
use crate::scope::ScopeStack;
use plan::LogicalExpr;
use plan::PlanTraversal;
use plan::{LogicalPlan, WindowExpr};
use plan::ResolvedColumn;
use error::{Result, VeinError};
use parser::ast::pipe_op::WindowFrame;

/// A projection item after binding: the logical expression plus its optional
/// output column name (alias or derived), threaded through window lifting.
pub type BoundItem = (LogicalExpr, Option<String>);

/// Splits bound projection items into plain expressions and window calls.
///
/// Window expressions found inside `bound_items` are lifted out: each call is
/// assigned a fresh output column and replaced in the item tree by a
/// placeholder `Column` whose id matches the window node's output column. The
/// returned plain items therefore reference the future `Window` node's
/// outputs and can be projected directly on top of it.
///
/// Each input item carries its optional output name; a top-level window call
/// uses it as its output column name (falls back to the function name).
/// Plain items keep their name alongside the (possibly placeholder) expr, so
/// the caller can build projection fields without re-walking the AST.
pub fn split_window_items(
    bound_items: Vec<BoundItem>,
    context: &mut BinderContext<'_>,
    _scopes: &mut ScopeStack,
) -> Result<(Vec<BoundItem>, Vec<WindowExpr>)> {
    let mut window_exprs = Vec::new();
    let mut partition_by: Option<Vec<LogicalExpr>> = None;
    let mut order_by: Option<Vec<plan::SortKey>> = None;
    let mut frame: Option<WindowFrame> = None;

    let plain_items = bound_items
        .into_iter()
        .map(|(item, name)| {
            let lifted = lift_window_calls(
                item,
                name.as_deref(),
                &mut window_exprs,
                &mut partition_by,
                &mut order_by,
                &mut frame,
                context,
            )?;
            Ok::<_, VeinError>((lifted, name))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok((plain_items, window_exprs))
}

/// Recursively replaces `WindowCall` nodes with placeholder `Column` nodes,
/// collecting each lifted call (with its fresh output column) for the
/// enclosing `LogicalPlan::Window`.
#[allow(clippy::too_many_arguments)]
fn lift_window_calls(
    expr: LogicalExpr,
    alias_hint: Option<&str>,
    window_exprs: &mut Vec<WindowExpr>,
    partition_by: &mut Option<Vec<LogicalExpr>>,
    order_by: &mut Option<Vec<plan::SortKey>>,
    frame: &mut Option<WindowFrame>,
    context: &mut BinderContext<'_>,
) -> Result<LogicalExpr> {
    // Recurse into children first so calls nested in e.g. BinaryOp are lifted too.
    // Nested calls cannot use the item alias, only the top-level call can.
    let mapped = expr.map_children(&mut |child| {
        lift_window_calls(
            child,
            None,
            window_exprs,
            partition_by,
            order_by,
            frame,
            context,
        )
    })?;

    match mapped {
        LogicalExpr::WindowCall {
            ref name,
            partition_by: ref call_partition,
            order_by: ref call_order,
            frame: ref call_frame,
            ref return_type,
            ..
        } => {
            // All calls in one operator must agree on the OVER clause in this
            // milestone; the single Window node partitions once for all.
            if let Some(existing) = partition_by {
                if *existing != *call_partition {
                    return Err(VeinError::semantic(
                        format!(
                            "window functions in one operator must share the same PARTITION BY clause (conflict with '{name}')"
                        ),
                        None,
                    ));
                }
            } else {
                *partition_by = Some(call_partition.clone());
            }
            if let Some(existing) = order_by {
                if *existing != *call_order {
                    return Err(VeinError::semantic(
                        format!(
                            "window functions in one operator must share the same ORDER BY clause (conflict with '{name}')"
                        ),
                        None,
                    ));
                }
            } else {
                *order_by = Some(call_order.clone());
            }
            if *frame != *call_frame {
                return Err(VeinError::semantic(
                    format!(
                        "window functions in one operator must share the same frame (conflict with '{name}')"
                    ),
                    None,
                ));
            }
            if frame.is_none() {
                *frame = (*call_frame).clone();
            }

            let output_column = ResolvedColumn::new(
                context.next_id(),
                alias_hint.unwrap_or(name.as_str()).to_string(),
                None,
                return_type.clone(),
            )
                .with_nullable(mapped.nullable());
            let output_column = std::sync::Arc::new(output_column);
            window_exprs.push(WindowExpr {
                call: mapped.clone(),
                output_column: output_column.clone(),
            });
            Ok(LogicalExpr::Column(output_column))
        }
        other => Ok(other),
    }
}

/// True when the expression tree contains a window function call (used for
/// placement validation in WHERE/AGGREGATE/ASSERT).
pub fn expr_contains_window_call(expr: &LogicalExpr) -> bool {
    if matches!(expr, LogicalExpr::WindowCall { .. }) {
        return true;
    }
    expr.children()
        .iter()
        .any(|child| expr_contains_window_call(child))
}

/// Wraps `input` in a `LogicalPlan::Window` when window expressions were
/// lifted (no-op otherwise). The Window's schema is the input schema plus the
/// window output columns, which its consuming projection references.
pub fn wrap_window(input: LogicalPlan, window_exprs: Vec<WindowExpr>) -> Result<LogicalPlan> {
    if window_exprs.is_empty() {
        return Ok(input);
    }

    let LogicalExpr::WindowCall {
        partition_by,
        order_by,
        frame,
        ..
    } = &window_exprs[0].call
    else {
        unreachable!("window expressions always hold WindowCall calls");
    };

    let mut fields = input.schema().fields.clone();
    for w in &window_exprs {
        fields.push(w.output_column.clone());
    }

    Ok(LogicalPlan::Window {
        input: Box::new(input),
        partition_by: partition_by.clone(),
        order_by: order_by.clone(),
        frame: frame.clone(),
        exprs: window_exprs,
        schema: Box::new(plan::VeinSchema { fields }),
    })
}

#[cfg(test)]
#[allow(missing_docs)]
mod tests;
