//! Algebraic simplification of logical expressions.
//!
//! This rule complements constant folding. It rewrites expressions containing
//! columns only when the identity is valid for the expression's type and SQL
//! three-valued boolean semantics.

use error::Result;
use parser::ast::expr::BinaryOp;
use plan::{LogicalExpr, LogicalLiteral, LogicalPlan, SortKey, VeinDataType, WindowExpr};

use crate::context::OptimizerContext;
use crate::rule::{OptimizerRule, RuleType};

/// A heuristic rule for semantics-preserving expression identities.
pub struct ExpressionSimplificationRule;

impl OptimizerRule for ExpressionSimplificationRule {
    fn name(&self) -> &str {
        "ExpressionSimplificationRule"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Heuristic
    }

    fn apply(
        &self,
        plan: &LogicalPlan,
        _context: &OptimizerContext,
    ) -> Result<Option<LogicalPlan>> {
        let rewritten = rewrite_node(plan);
        Ok((rewritten != *plan).then_some(rewritten))
    }
}

/// Rewrites every logical expression carried by a plan node.
fn rewrite_node(plan: &LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan {
            table,
            projection,
            filters,
            schema,
        } => LogicalPlan::Scan {
            table: table.clone(),
            projection: projection.clone(),
            filters: simplify_exprs(filters),
            schema: schema.clone(),
        },
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: input.clone(),
            predicate: simplify_expr(predicate),
        },
        LogicalPlan::ExternalScan { .. } => plan.clone(),
        LogicalPlan::Call {
            input,
            name,
            args,
            schema,
        } => LogicalPlan::Call {
            input: input.clone(),
            name: name.clone(),
            args: simplify_exprs(args),
            schema: schema.clone(),
        },
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => LogicalPlan::Project {
            input: input.clone(),
            exprs: simplify_exprs(exprs),
            schema: schema.clone(),
        },
        LogicalPlan::Aggregate {
            input,
            grouping_sets,
            aggs,
            schema,
        } => LogicalPlan::Aggregate {
            input: input.clone(),
            grouping_sets: grouping_sets
                .iter()
                .map(|group| simplify_exprs(group))
                .collect(),
            aggs: simplify_exprs(aggs),
            schema: schema.clone(),
        },
        LogicalPlan::Window {
            input,
            partition_by,
            order_by,
            frame,
            exprs,
            schema,
        } => LogicalPlan::Window {
            input: input.clone(),
            partition_by: simplify_exprs(partition_by),
            order_by: simplify_sort_keys(order_by),
            frame: frame.clone(),
            exprs: exprs
                .iter()
                .map(|window_expr| WindowExpr {
                    call: simplify_expr(&window_expr.call),
                    output_column: window_expr.output_column.clone(),
                })
                .collect(),
            schema: schema.clone(),
        },
        LogicalPlan::Sort {
            input,
            sort_keys,
            schema,
        } => LogicalPlan::Sort {
            input: input.clone(),
            sort_keys: simplify_sort_keys(sort_keys),
            schema: schema.clone(),
        },
        LogicalPlan::Assert {
            input,
            condition,
            message,
            schema,
        } => LogicalPlan::Assert {
            input: input.clone(),
            condition: simplify_expr(condition),
            message: message.clone(),
            schema: schema.clone(),
        },
        LogicalPlan::Pivot {
            input,
            aggregate,
            alias,
            value_column,
            values,
            schema,
        } => LogicalPlan::Pivot {
            input: input.clone(),
            aggregate: simplify_expr(aggregate),
            alias: alias.clone(),
            value_column: value_column.clone(),
            values: simplify_exprs(values),
            schema: schema.clone(),
        },
        LogicalPlan::Unpivot {
            input,
            value_name,
            name_name,
            columns,
            include_nulls,
            schema,
        } => LogicalPlan::Unpivot {
            input: input.clone(),
            value_name: value_name.clone(),
            name_name: name_name.clone(),
            columns: columns.clone(),
            include_nulls: *include_nulls,
            schema: schema.clone(),
        },
        LogicalPlan::Join {
            join_type,
            left,
            right,
            on,
            using,
            schema,
        } => LogicalPlan::Join {
            join_type: *join_type,
            left: left.clone(),
            right: right.clone(),
            on: on.as_ref().map(simplify_expr),
            using: using.clone(),
            schema: schema.clone(),
        },
        other => other.clone(),
    }
}

fn simplify_exprs(exprs: &[LogicalExpr]) -> Vec<LogicalExpr> {
    exprs.iter().map(simplify_expr).collect()
}

fn simplify_sort_keys(keys: &[SortKey]) -> Vec<SortKey> {
    keys.iter()
        .map(|key| SortKey {
            expr: simplify_expr(&key.expr),
            asc: key.asc,
            nulls_first: key.nulls_first,
        })
        .collect()
}

/// Recursively applies identity rewrites after simplifying all children.
fn simplify_expr(expr: &LogicalExpr) -> LogicalExpr {
    let rewritten = expr
        .map_children(&mut |child| Ok(simplify_expr(&child)))
        .expect("expression simplification child traversal is infallible");

    match rewritten {
        LogicalExpr::BinaryOp {
            left,
            op,
            right,
            return_type,
        } => simplify_binary(*left, op, *right, return_type),
        LogicalExpr::Cast { expr, target_type } if expr.data_type() == target_type => *expr,
        LogicalExpr::Not { expr: inner } => match *inner {
            LogicalExpr::Not { expr } => *expr,
            inner => LogicalExpr::Not {
                expr: Box::new(inner),
            },
        },
        other => other,
    }
}

/// Applies arithmetic and boolean identities without changing nullable results.
fn simplify_binary(
    left: LogicalExpr,
    op: BinaryOp,
    right: LogicalExpr,
    return_type: VeinDataType,
) -> LogicalExpr {
    if return_type == VeinDataType::Boolean {
        if let Some(result) = simplify_boolean_comparison(&left, op, &right) {
            return result;
        }
        if let Some(result) = simplify_boolean_identity(&left, op, &right) {
            return result;
        }
    }

    if return_type == VeinDataType::Int64 {
        if let Some(result) = simplify_integer_identity(&left, op, &right) {
            return result;
        }
    }

    LogicalExpr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
        return_type,
    }
}

fn simplify_integer_identity(
    left: &LogicalExpr,
    op: BinaryOp,
    right: &LogicalExpr,
) -> Option<LogicalExpr> {
    let zero = |expr: &LogicalExpr| matches!(expr, LogicalExpr::Literal(LogicalLiteral::Int64(value)) if *value == 0);
    let one = |expr: &LogicalExpr| matches!(expr, LogicalExpr::Literal(LogicalLiteral::Int64(value)) if *value == 1);

    match op {
        BinaryOp::Plus if zero(right) => Some(left.clone()),
        BinaryOp::Plus if zero(left) => Some(right.clone()),
        BinaryOp::Minus if zero(right) => Some(left.clone()),
        BinaryOp::Multiply if one(right) => Some(left.clone()),
        BinaryOp::Multiply if one(left) => Some(right.clone()),
        BinaryOp::Divide if one(right) => Some(left.clone()),
        _ => None,
    }
}

fn simplify_boolean_identity(
    left: &LogicalExpr,
    op: BinaryOp,
    right: &LogicalExpr,
) -> Option<LogicalExpr> {
    let is_true =
        |expr: &LogicalExpr| matches!(expr, LogicalExpr::Literal(LogicalLiteral::Boolean(true)));
    let is_false =
        |expr: &LogicalExpr| matches!(expr, LogicalExpr::Literal(LogicalLiteral::Boolean(false)));

    match op {
        BinaryOp::And if is_true(right) => Some(left.clone()),
        BinaryOp::And if is_true(left) => Some(right.clone()),
        BinaryOp::Or if is_false(right) => Some(left.clone()),
        BinaryOp::Or if is_false(left) => Some(right.clone()),
        BinaryOp::And if is_false(left) || is_false(right) => {
            Some(LogicalExpr::Literal(LogicalLiteral::Boolean(false)))
        }
        BinaryOp::Or if is_true(left) || is_true(right) => {
            Some(LogicalExpr::Literal(LogicalLiteral::Boolean(true)))
        }
        _ => None,
    }
}

fn simplify_boolean_comparison(
    left: &LogicalExpr,
    op: BinaryOp,
    right: &LogicalExpr,
) -> Option<LogicalExpr> {
    let (expr, value) = match (left, right) {
        (expr, LogicalExpr::Literal(LogicalLiteral::Boolean(value)))
            if expr.data_type() == VeinDataType::Boolean =>
        {
            (expr, *value)
        }
        (LogicalExpr::Literal(LogicalLiteral::Boolean(value)), expr)
            if expr.data_type() == VeinDataType::Boolean =>
        {
            (expr, *value)
        }
        _ => return None,
    };

    match (op, value) {
        (BinaryOp::Eq, true) | (BinaryOp::Neq, false) => Some(expr.clone()),
        (BinaryOp::Eq, false) | (BinaryOp::Neq, true) => Some(LogicalExpr::Not {
            expr: Box::new(expr.clone()),
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plan::{ColumnId, ResolvedColumn};

    fn int_column() -> LogicalExpr {
        LogicalExpr::Column(ResolvedColumn {
            id: ColumnId(1),
            name: "id".to_string(),
            qualifier: None,
            data_type: VeinDataType::Int64,
            is_factorized: false,
            nullable: true,
        })
    }

    fn bool_column() -> LogicalExpr {
        LogicalExpr::Column(ResolvedColumn {
            id: ColumnId(2),
            name: "active".to_string(),
            qualifier: None,
            data_type: VeinDataType::Boolean,
            is_factorized: false,
            nullable: true,
        })
    }

    fn binary(
        left: LogicalExpr,
        op: BinaryOp,
        right: LogicalExpr,
        return_type: VeinDataType,
    ) -> LogicalExpr {
        LogicalExpr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
            return_type,
        }
    }

    fn int(value: i64) -> LogicalExpr {
        LogicalExpr::Literal(LogicalLiteral::Int64(value))
    }

    fn boolean(value: bool) -> LogicalExpr {
        LogicalExpr::Literal(LogicalLiteral::Boolean(value))
    }

    #[test]
    fn folds_integer_identities() {
        assert_eq!(
            simplify_expr(&binary(
                int_column(),
                BinaryOp::Plus,
                int(0),
                VeinDataType::Int64,
            )),
            int_column()
        );
        assert_eq!(
            simplify_expr(&binary(
                int(1),
                BinaryOp::Multiply,
                int_column(),
                VeinDataType::Int64,
            )),
            int_column()
        );
    }

    #[test]
    fn folds_nullable_boolean_identities() {
        assert_eq!(
            simplify_expr(&binary(
                bool_column(),
                BinaryOp::And,
                boolean(true),
                VeinDataType::Boolean,
            )),
            bool_column()
        );
        assert_eq!(
            simplify_expr(&binary(
                bool_column(),
                BinaryOp::And,
                boolean(false),
                VeinDataType::Boolean,
            )),
            boolean(false)
        );
        assert_eq!(
            simplify_expr(&binary(
                bool_column(),
                BinaryOp::Or,
                boolean(true),
                VeinDataType::Boolean,
            )),
            boolean(true)
        );
    }

    #[test]
    fn folds_boolean_comparisons_and_double_not() {
        assert_eq!(
            simplify_expr(&binary(
                bool_column(),
                BinaryOp::Eq,
                boolean(false),
                VeinDataType::Boolean,
            )),
            LogicalExpr::Not {
                expr: Box::new(bool_column()),
            }
        );
        assert_eq!(
            simplify_expr(&LogicalExpr::Not {
                expr: Box::new(LogicalExpr::Not {
                    expr: Box::new(bool_column()),
                }),
            }),
            bool_column()
        );
    }

    #[test]
    fn removes_only_redundant_casts() {
        let cast = LogicalExpr::Cast {
            expr: Box::new(int_column()),
            target_type: VeinDataType::Int64,
        };
        assert_eq!(simplify_expr(&cast), int_column());

        let cast = LogicalExpr::Cast {
            expr: Box::new(int_column()),
            target_type: VeinDataType::Float64,
        };
        assert_eq!(simplify_expr(&cast), cast);
    }
}
