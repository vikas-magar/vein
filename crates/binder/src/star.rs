//! Wildcard (`*`, `table.*`) expansion for projection operators.

use crate::context::BinderContext;
use crate::expr::ExprBinder;
use crate::scope::ScopeStack;
use plan::LogicalExpr;
use plan::ResolvedColumn;
use error::{Result, VeinError};
use parser::ast::expr::Expr;
use parser::ast::pipe_op::SelectItem;

/// Expands a `*` / `table.*` reference into the columns visible in the
/// current top scope.
///
/// `table: None` expands every column of the innermost scope; `table: Some(t)`
/// expands only the columns whose scope answers to the qualifier `t`. An
/// unqualified `*` with no visible columns (e.g. after a full DROP) is a
/// `Semantic` error.
pub fn expand_wildcard(
    scopes: &ScopeStack,
    table: Option<&str>,
) -> Result<Vec<ResolvedColumn>> {
    let Some(scope) = scopes.scopes_top() else {
        return Err(VeinError::semantic(
            "no tables in scope for '*' expansion",
            None,
        ));
    };

    match table {
        Some(t) => {
            if !scope.aliases.iter().any(|a| a == t) {
                return Err(VeinError::semantic(
                    format!("unknown table '{t}' in '*' reference"),
                    None,
                ));
            }
            // Only the columns belonging to `t` (its columns carry the table
            // name or alias as their qualifier), not every visible column.
            let matched: Vec<ResolvedColumn> = scope
                .columns
                .iter()
                .filter(|c| c.qualifier.as_deref() == Some(t))
                .cloned()
                .collect();
            if matched.is_empty() {
                return Err(VeinError::semantic(
                    format!("no columns from table '{t}' in scope for '*' reference"),
                    None,
                ));
            }
            Ok(matched)
        }
        None => {
            if scope.columns.is_empty() {
                return Err(VeinError::semantic(
                    "no columns in scope to expand '*'",
                    None,
                ));
            }
            Ok(scope.columns.clone())
        }
    }
}

/// Binds the item list of a projection operator (SELECT/EXTEND) into
/// `(expression, output column name)` pairs.
///
/// Wildcard items expand to a placeholder `Column` expression per visible
/// column (name preserved); every other item is bound via [`ExprBinder`] with
/// the name coming from its alias or a derived fallback. The pairs feed
/// [`crate::window::split_window_items`].
pub async fn expand_projection_items<'a>(
    items: &[SelectItem<'_>],
    context: &mut BinderContext<'a>,
    scopes: &mut ScopeStack,
) -> Result<Vec<(LogicalExpr, Option<String>)>> {
    let mut bound = Vec::new();
    for item in items {
        match &item.expr.node {
            Expr::Wildcard { table } => {
                for column in expand_wildcard(scopes, table.as_deref())? {
                    bound.push((LogicalExpr::Column(column.clone()), Some(column.name)));
                }
            }
            _ => {
                let expr = ExprBinder::new(context, scopes).bind(&item.expr).await?;
                let name = item
                    .alias
                    .map(|a| a.to_string())
                    .unwrap_or_else(|| crate::expr::derive_column_name(&item.expr.node));
                bound.push((expr, Some(name)));
            }
        }
    }
    Ok(bound)
}
