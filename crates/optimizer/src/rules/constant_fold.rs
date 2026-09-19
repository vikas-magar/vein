//! Constant folding: evaluates constant expressions at plan time.
//!
//! `WHERE age > 10 + 8` becomes `WHERE age > 18` before execution, saving
//! per-row evaluation. Folding is conservative: only pure arithmetic,
//! boolean and comparison operators on matching literal types are folded,
//! and operations that would panic (e.g. `x / 0`, overflow) are left alone.

use error::Result;
use parser::ast::expr::BinaryOp;
use plan::{
    LogicalExpr, LogicalLiteral, LogicalPlan, SortKey, WindowExpr,
};

use crate::rule::{OptimizerRule, RuleType};

/// A heuristic rule that folds constant sub-expressions into literals.
pub struct ConstantFoldingRule;

impl OptimizerRule for ConstantFoldingRule {
    fn name(&self) -> &str {
        "ConstantFoldingRule"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Heuristic
    }

    fn apply(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
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
            filters: fold_exprs(filters),
            schema: schema.clone(),
        },
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: input.clone(),
            predicate: fold_expr(predicate),
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
            args: fold_exprs(args),
            schema: schema.clone(),
        },
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => LogicalPlan::Project {
            input: input.clone(),
            exprs: fold_exprs(exprs),
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
                .map(|group| fold_exprs(group))
                .collect(),
            aggs: fold_exprs(aggs),
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
            partition_by: fold_exprs(partition_by),
            order_by: fold_sort_keys(order_by),
            frame: frame.clone(),
            exprs: exprs
                .iter()
                .map(|window_expr| WindowExpr {
                    call: fold_expr(&window_expr.call),
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
            sort_keys: fold_sort_keys(sort_keys),
            schema: schema.clone(),
        },
        LogicalPlan::Assert {
            input,
            condition,
            message,
            schema,
        } => LogicalPlan::Assert {
            input: input.clone(),
            condition: fold_expr(condition),
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
            aggregate: fold_expr(aggregate),
            alias: alias.clone(),
            value_column: value_column.clone(),
            values: fold_exprs(values),
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
            on: on.as_ref().map(fold_expr),
            using: using.clone(),
            schema: schema.clone(),
        },
        // Nodes without logical expressions and semantic nodes whose
        // predicates are opaque (`String`) pass through unchanged.
        other => other.clone(),
    }
}

/// Folds every expression in a list, preserving order and length.
fn fold_exprs(exprs: &[LogicalExpr]) -> Vec<LogicalExpr> {
    exprs.iter().map(fold_expr).collect()
}

/// Folds the expression inside every sort key.
fn fold_sort_keys(keys: &[SortKey]) -> Vec<SortKey> {
    keys.iter()
        .map(|key| SortKey {
            expr: fold_expr(&key.expr),
            asc: key.asc,
            nulls_first: key.nulls_first,
        })
        .collect()
}

/// Recursively folds constant sub-expressions inside `expr`.
fn fold_expr(expr: &LogicalExpr) -> LogicalExpr {
    match expr {
        LogicalExpr::BinaryOp {
            left,
            op,
            right,
            return_type,
        } => {
            let folded_left = fold_expr(left);
            let folded_right = fold_expr(right);

            if let (LogicalExpr::Literal(l), LogicalExpr::Literal(r)) =
                (&folded_left, &folded_right)
                && let Some(folded) = fold_literals(*op, l, r)
            {
                return LogicalExpr::Literal(folded);
            }

            LogicalExpr::BinaryOp {
                left: Box::new(folded_left),
                op: *op,
                right: Box::new(folded_right),
                return_type: return_type.clone(),
            }
        }
        other => other.clone(),
    }
}

/// Performs a constant binary operation on two literals.
///
/// Returns `None` for operations that are not foldable: mismatched types,
/// division by zero, integer overflow, or operators outside the supported
/// subset.
fn fold_literals(op: BinaryOp, left: &LogicalLiteral, right: &LogicalLiteral) -> Option<LogicalLiteral> {
    match (left, right) {
        (LogicalLiteral::Int64(l), LogicalLiteral::Int64(r)) => fold_int64(op, *l, *r),
        (LogicalLiteral::Float64(l), LogicalLiteral::Float64(r)) => fold_float64(op, *l, *r),
        (LogicalLiteral::Boolean(l), LogicalLiteral::Boolean(r)) => fold_boolean(op, *l, *r),
        (LogicalLiteral::String(l), LogicalLiteral::String(r)) => match op {
            BinaryOp::Concat => Some(LogicalLiteral::String(format!("{l}{r}"))),
            BinaryOp::Eq => Some(LogicalLiteral::Boolean(l == r)),
            BinaryOp::Neq => Some(LogicalLiteral::Boolean(l != r)),
            _ => None,
        },
        _ => None,
    }
}

/// Folds integer operations, guarding against overflow and division by zero.
fn fold_int64(op: BinaryOp, left: i64, right: i64) -> Option<LogicalLiteral> {
    let result = match op {
        BinaryOp::Plus => left.checked_add(right)?,
        BinaryOp::Minus => left.checked_sub(right)?,
        BinaryOp::Multiply => left.checked_mul(right)?,
        BinaryOp::Divide => left.checked_div(right)?,
        BinaryOp::Modulo => left.checked_rem(right)?,
        BinaryOp::Eq => return Some(LogicalLiteral::Boolean(left == right)),
        BinaryOp::Neq => return Some(LogicalLiteral::Boolean(left != right)),
        BinaryOp::Lt => return Some(LogicalLiteral::Boolean(left < right)),
        BinaryOp::Gt => return Some(LogicalLiteral::Boolean(left > right)),
        BinaryOp::Leq => return Some(LogicalLiteral::Boolean(left <= right)),
        BinaryOp::Geq => return Some(LogicalLiteral::Boolean(left >= right)),
        // Mixed-type and boolean operators are handled per literal kind.
        _ => return None,
    };
    Some(LogicalLiteral::Int64(result))
}

/// Folds float operations, guarding against division by zero.
fn fold_float64(op: BinaryOp, left: f64, right: f64) -> Option<LogicalLiteral> {
    let result = match op {
        BinaryOp::Plus => left + right,
        BinaryOp::Minus => left - right,
        BinaryOp::Multiply => left * right,
        BinaryOp::Divide => {
            if right == 0.0 {
                return None;
            }
            left / right
        }
        BinaryOp::Eq => return Some(LogicalLiteral::Boolean(left == right)),
        BinaryOp::Neq => return Some(LogicalLiteral::Boolean(left != right)),
        BinaryOp::Lt => return Some(LogicalLiteral::Boolean(left < right)),
        BinaryOp::Gt => return Some(LogicalLiteral::Boolean(left > right)),
        BinaryOp::Leq => return Some(LogicalLiteral::Boolean(left <= right)),
        BinaryOp::Geq => return Some(LogicalLiteral::Boolean(left >= right)),
        _ => return None,
    };
    Some(LogicalLiteral::Float64(result))
}

/// Folds boolean `AND`/`OR`.
fn fold_boolean(op: BinaryOp, left: bool, right: bool) -> Option<LogicalLiteral> {
    let result = match op {
        BinaryOp::And => left && right,
        BinaryOp::Or => left || right,
        _ => return None,
    };
    Some(LogicalLiteral::Boolean(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use plan::VeinDataType;

    fn lit(value: i64) -> LogicalExpr {
        LogicalExpr::Literal(LogicalLiteral::Int64(value))
    }

    fn float(value: f64) -> LogicalExpr {
        LogicalExpr::Literal(LogicalLiteral::Float64(value))
    }

    fn bin(op: BinaryOp, left: LogicalExpr, right: LogicalExpr) -> LogicalExpr {
        LogicalExpr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
            return_type: VeinDataType::Int64,
        }
    }

    fn scan() -> LogicalPlan {
        let schema = Box::new(plan::VeinSchema { fields: vec![] });
        let table = std::sync::Arc::new(plan::TableMetadata::new(
            "t",
            std::sync::Arc::new(*schema.clone()),
        ));
        LogicalPlan::Scan {
            table,
            projection: None,
            filters: vec![],
            schema,
        }
    }

    fn project(exprs: Vec<LogicalExpr>) -> LogicalPlan {
        LogicalPlan::Project {
            input: Box::new(scan()),
            exprs,
            schema: Box::new(plan::VeinSchema { fields: vec![] }),
        }
    }

    #[test]
    fn folds_nested_arithmetic() {
        let folded = fold_expr(&bin(
            BinaryOp::Multiply,
            bin(BinaryOp::Plus, lit(10), lit(5)),
            lit(2),
        ));
        assert_eq!(folded, lit(30));
    }

    #[test]
    fn folds_comparisons_and_booleans() {
        assert_eq!(
            fold_expr(&bin(BinaryOp::Lt, lit(1), lit(2))),
            LogicalExpr::Literal(LogicalLiteral::Boolean(true))
        );
        assert_eq!(
            fold_expr(&bin(
                BinaryOp::And,
                LogicalExpr::Literal(LogicalLiteral::Boolean(true)),
                LogicalExpr::Literal(LogicalLiteral::Boolean(false))
            )),
            LogicalExpr::Literal(LogicalLiteral::Boolean(false))
        );
    }

    #[test]
    fn folds_floats() {
        assert_eq!(
            fold_expr(&bin(BinaryOp::Divide, float(9.0), float(3.0))),
            float(3.0)
        );
    }

    #[test]
    fn does_not_fold_division_by_zero() {
        let expr = bin(BinaryOp::Divide, lit(1), lit(0));
        assert_eq!(fold_expr(&expr), expr);
    }

    #[test]
    fn does_not_fold_integer_overflow() {
        let expr = bin(
            BinaryOp::Plus,
            LogicalExpr::Literal(LogicalLiteral::Int64(i64::MAX)),
            lit(1),
        );
        assert_eq!(fold_expr(&expr), expr);
    }

    #[test]
    fn does_not_fold_mixed_types() {
        let expr = bin(BinaryOp::Plus, lit(1), float(2.0));
        assert_eq!(fold_expr(&expr), expr);
    }

    #[test]
    fn folds_nodes_that_carry_expressions() {
        let rule = ConstantFoldingRule;
        let plan = LogicalPlan::Aggregate {
            input: Box::new(scan()),
            grouping_sets: vec![vec![bin(BinaryOp::Plus, lit(1), lit(2))]],
            aggs: vec![bin(BinaryOp::Multiply, lit(3), lit(4))],
            schema: Box::new(plan::VeinSchema { fields: vec![] }),
        };

        let optimized = rule.apply(&plan).unwrap().expect("plan must change");
        let LogicalPlan::Aggregate {
            grouping_sets,
            aggs,
            ..
        } = optimized
        else {
            panic!("expected an Aggregate node");
        };
        assert_eq!(grouping_sets, vec![vec![lit(3)]]);
        assert_eq!(aggs, vec![lit(12)]);
    }

    #[test]
    fn unchanged_plan_returns_none() {
        let rule = ConstantFoldingRule;
        let plan = project(vec![lit(17)]);
        assert!(rule.apply(&plan).unwrap().is_none());
    }

    #[test]
    fn project_exprs_are_folded() {
        let plan = project(vec![bin(BinaryOp::Multiply, lit(3), lit(4))]);
        let optimized = ConstantFoldingRule
            .apply(&plan)
            .unwrap()
            .expect("plan must change");
        let LogicalPlan::Project { exprs, .. } = optimized else {
            panic!("expected a Project node");
        };
        assert_eq!(exprs, vec![lit(12)]);
    }

    #[test]
    fn predicates_are_folded() {
        let predicate = bin(BinaryOp::Gt, bin(BinaryOp::Plus, lit(10), lit(8)), lit(18));
        let plan = LogicalPlan::Filter {
            input: Box::new(scan()),
            predicate,
        };

        let optimized = ConstantFoldingRule
            .apply(&plan)
            .unwrap()
            .expect("plan must change");
        let LogicalPlan::Filter { predicate, .. } = optimized else {
            panic!("expected a Filter node");
        };
        assert_eq!(predicate, LogicalExpr::Literal(LogicalLiteral::Boolean(false)));
    }
}