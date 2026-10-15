//! Constant folding: evaluates constant expressions at plan time.
//!
//! `WHERE age > 10 + 8` becomes `WHERE age > 18` before execution, saving
//! per-row evaluation. Folding is conservative: only pure arithmetic,
//! boolean and comparison operators on matching literal types are folded,
//! and operations that would panic (e.g. `x / 0`, overflow) are left alone.

use error::Result;
use parser::ast::expr::BinaryOp;
use plan::{
    LogicalExpr, LogicalLiteral, LogicalPlan, SortKey, VeinDataType, WindowExpr,
};

use crate::context::OptimizerContext;
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
    fold_expr_owned(expr.clone())
}

/// Owned recursion for [`fold_expr`]. Consuming each node keeps the traversal
/// clone-free; the by-reference entry point above clones once per call.
fn fold_expr_owned(expr: LogicalExpr) -> LogicalExpr {
    // The logical expression owns one canonical child-mapping API. Using it
    // here keeps folding aligned with every expression variant emitted by the
    // pipe binder, including CASE, lists, windows, lambdas, and function args.
    let rewritten = expr
        .map_children(&mut |child| Ok(fold_expr_owned(child)))
        .expect("constant folding child traversal is infallible");

    match rewritten {
        LogicalExpr::BinaryOp {
            left,
            op,
            right,
            return_type,
        } => {
            if let (LogicalExpr::Literal(left), LogicalExpr::Literal(right)) = (&*left, &*right)
                && let Some(folded) = fold_literals(op, left, right, &return_type)
            {
                return LogicalExpr::Literal(folded);
            }

            LogicalExpr::BinaryOp {
                left,
                op,
                right,
                return_type,
            }
        }
        LogicalExpr::Cast { expr, target_type } => fold_cast(expr, target_type),
        LogicalExpr::Like {
            expr,
            pattern,
            negated,
        } => fold_like(expr, pattern, negated),
        LogicalExpr::Not { expr } => fold_unary(expr, UnaryKind::Not),
        LogicalExpr::Negate { expr } => fold_unary(expr, UnaryKind::Negate),
        LogicalExpr::IsNull { expr } => fold_null_check(expr, false),
        LogicalExpr::IsNotNull { expr } => fold_null_check(expr, true),
        LogicalExpr::InList {
            expr,
            list,
            negated,
        } => fold_in_list(expr, list, negated),
        LogicalExpr::Case {
            when,
            then,
            else_,
            return_type,
        } => fold_case(when, then, else_, return_type),
        other => other,
    }
}

/// Folds a literal cast only when the representation and conversion are safe.
fn fold_cast(expr: Box<LogicalExpr>, target_type: VeinDataType) -> LogicalExpr {
    let LogicalExpr::Literal(literal) = &*expr else {
        return LogicalExpr::Cast {
            expr,
            target_type,
        };
    };

    let source_type = plan::expr::inference::literal_type(literal);
    let converted = if matches!(literal, LogicalLiteral::Null(_)) {
        Some(LogicalLiteral::Null(target_type.clone()))
    } else if source_type == target_type {
        Some(literal.clone())
    } else {
        match (literal, &target_type) {
            (LogicalLiteral::Int64(value), VeinDataType::Float64) => {
                Some(LogicalLiteral::Float64(*value as f64))
            }
            (LogicalLiteral::Float64(value), VeinDataType::Int64)
            if value.is_finite()
                && *value >= i64::MIN as f64
                && *value < (i64::MAX as f64) + 1.0 =>
                {
                    Some(LogicalLiteral::Int64(value.trunc() as i64))
                }
            (LogicalLiteral::String(value), VeinDataType::Int64) => {
                value.parse().ok().map(LogicalLiteral::Int64)
            }
            (LogicalLiteral::String(value), VeinDataType::Float64) => {
                value.parse().ok().map(LogicalLiteral::Float64)
            }
            (LogicalLiteral::String(value), VeinDataType::Boolean) => match value.to_ascii_lowercase().as_str() {
                "true" => Some(LogicalLiteral::Boolean(true)),
                "false" => Some(LogicalLiteral::Boolean(false)),
                _ => None,
            },
            (LogicalLiteral::Boolean(value), VeinDataType::Utf8) => {
                Some(LogicalLiteral::String(value.to_string()))
            }
            _ => None,
        }
    };

    converted
        .map(LogicalExpr::Literal)
        .unwrap_or(LogicalExpr::Cast {
            expr,
            target_type,
        })
}

/// Folds literal LIKE patterns, including %, _, and backslash escapes.
fn fold_like(expr: Box<LogicalExpr>, pattern: Box<LogicalExpr>, negated: bool) -> LogicalExpr {
    let (LogicalExpr::Literal(LogicalLiteral::String(value)),
        LogicalExpr::Literal(LogicalLiteral::String(pattern))) = (&*expr, &*pattern)
    else {
        return LogicalExpr::Like {
            expr,
            pattern,
            negated,
        };
    };

    let matches = like_matches(value, pattern);
    LogicalExpr::Literal(LogicalLiteral::Boolean(if negated { !matches } else { matches }))
}

/// Matches a SQL LIKE pattern without allocating a transformed string.
fn like_matches(value: &str, pattern: &str) -> bool {
    let value: Vec<char> = value.chars().collect();
    let pattern: Vec<char> = pattern.chars().collect();
    let mut memo = std::collections::HashMap::new();

    fn visit(
        value: &[char],
        pattern: &[char],
        value_index: usize,
        pattern_index: usize,
        memo: &mut std::collections::HashMap<(usize, usize), bool>,
    ) -> bool {
        if let Some(result) = memo.get(&(value_index, pattern_index)) {
            return *result;
        }
        let result = if pattern_index == pattern.len() {
            value_index == value.len()
        } else {
            match pattern[pattern_index] {
                '%' => {
                    visit(value, pattern, value_index, pattern_index + 1, memo)
                        || (value_index < value.len()
                        && visit(value, pattern, value_index + 1, pattern_index, memo))
                }
                '_' => {
                    value_index < value.len()
                        && visit(value, pattern, value_index + 1, pattern_index + 1, memo)
                }
                '\\' if pattern_index + 1 < pattern.len() => {
                    value_index < value.len()
                        && value[value_index] == pattern[pattern_index + 1]
                        && visit(value, pattern, value_index + 1, pattern_index + 2, memo)
                }
                literal => {
                    value_index < value.len()
                        && value[value_index] == literal
                        && visit(value, pattern, value_index + 1, pattern_index + 1, memo)
                }
            }
        };
        memo.insert((value_index, pattern_index), result);
        result
    }

    visit(&value, &pattern, 0, 0, &mut memo)
}

/// Unary operators supported by the pipe expression language.
#[derive(Debug, Clone, Copy)]
enum UnaryKind {
    /// Logical NOT.
    Not,
    /// Numeric negation.
    Negate,
}

/// Folds a literal unary expression when its semantics are unambiguous.
fn fold_unary(expr: Box<LogicalExpr>, kind: UnaryKind) -> LogicalExpr {
    let LogicalExpr::Literal(literal) = &*expr else {
        return match kind {
            UnaryKind::Not => LogicalExpr::Not { expr },
            UnaryKind::Negate => LogicalExpr::Negate { expr },
        };
    };

    let folded = match (kind, literal) {
        (UnaryKind::Not, LogicalLiteral::Boolean(value)) => {
            Some(LogicalLiteral::Boolean(!value))
        }
        (UnaryKind::Negate, LogicalLiteral::Int64(value)) => {
            value.checked_neg().map(LogicalLiteral::Int64)
        }
        (UnaryKind::Negate, LogicalLiteral::Float64(value)) => {
            Some(LogicalLiteral::Float64(-value))
        }
        _ => None,
    };

    folded
        .map(LogicalExpr::Literal)
        .unwrap_or_else(|| match kind {
            UnaryKind::Not => LogicalExpr::Not { expr },
            UnaryKind::Negate => LogicalExpr::Negate { expr },
        })
}

/// Folds IS NULL and IS NOT NULL for literal operands.
fn fold_null_check(expr: Box<LogicalExpr>, negated: bool) -> LogicalExpr {
    let LogicalExpr::Literal(literal) = &*expr else {
        return if negated {
            LogicalExpr::IsNotNull { expr }
        } else {
            LogicalExpr::IsNull { expr }
        };
    };

    let is_null = matches!(literal, LogicalLiteral::Null(_));
    LogicalExpr::Literal(LogicalLiteral::Boolean(if negated {
        !is_null
    } else {
        is_null
    }))
}

/// Folds an IN/NOT IN expression when all operands are literals.
fn fold_in_list(expr: Box<LogicalExpr>, list: Vec<LogicalExpr>, negated: bool) -> LogicalExpr {
    let LogicalExpr::Literal(value) = &*expr else {
        return LogicalExpr::InList {
            expr,
            list,
            negated,
        };
    };
    let literals: Option<Vec<&LogicalLiteral>> = list
        .iter()
        .map(|item| match item {
            LogicalExpr::Literal(literal) => Some(literal),
            _ => None,
        })
        .collect();
    let Some(literals) = literals else {
        return LogicalExpr::InList {
            expr,
            list,
            negated,
        };
    };

    let mut has_null = false;
    for candidate in literals {
        match fold_literals(
            BinaryOp::Eq,
            value,
            candidate,
            &VeinDataType::Boolean,
        ) {
            Some(LogicalLiteral::Boolean(true)) => {
                return LogicalExpr::Literal(LogicalLiteral::Boolean(!negated));
            }
            Some(LogicalLiteral::Null(_)) => has_null = true,
            _ => {}
        }
    }

    if has_null {
        LogicalExpr::Literal(LogicalLiteral::Null(VeinDataType::Boolean))
    } else {
        LogicalExpr::Literal(LogicalLiteral::Boolean(negated))
    }
}

/// Folds a CASE expression when a literal WHEN condition selects a branch.
fn fold_case(
    when: Vec<LogicalExpr>,
    then: Vec<LogicalExpr>,
    else_: Option<Box<LogicalExpr>>,
    return_type: VeinDataType,
) -> LogicalExpr {
    for (condition, result) in when.iter().zip(&then) {
        match condition {
            LogicalExpr::Literal(LogicalLiteral::Boolean(true)) => return result.clone(),
            LogicalExpr::Literal(LogicalLiteral::Boolean(false))
            | LogicalExpr::Literal(LogicalLiteral::Null(_)) => continue,
            _ => {
                return LogicalExpr::Case {
                    when,
                    then,
                    else_,
                    return_type,
                };
            }
        }
    }

    else_
        .map(|expr| *expr)
        .unwrap_or(LogicalExpr::Literal(LogicalLiteral::Null(return_type)))
}

/// Performs a constant binary operation on two literals.
///
/// Returns `None` for mismatched types, invalid arithmetic, or operators
/// outside the pipe language's supported foldable subset.
fn fold_literals(
    op: BinaryOp,
    left: &LogicalLiteral,
    right: &LogicalLiteral,
    return_type: &VeinDataType,
) -> Option<LogicalLiteral> {
    if matches!(op, BinaryOp::And | BinaryOp::Or)
        && let Some(result) = fold_nullable_boolean(op, left, right)
    {
        return Some(result);
    }

    if matches!(left, LogicalLiteral::Null(_)) || matches!(right, LogicalLiteral::Null(_)) {
        return Some(LogicalLiteral::Null(match op {
            BinaryOp::Eq
            | BinaryOp::Neq
            | BinaryOp::Lt
            | BinaryOp::Gt
            | BinaryOp::Leq
            | BinaryOp::Geq
            | BinaryOp::And
            | BinaryOp::Or => VeinDataType::Boolean,
            _ => return_type.clone(),
        }));
    }

    match (left, right) {
        (LogicalLiteral::Int64(l), LogicalLiteral::Int64(r)) => fold_int64(op, *l, *r),
        (LogicalLiteral::Float64(l), LogicalLiteral::Float64(r)) => fold_float64(op, *l, *r),
        (LogicalLiteral::Boolean(l), LogicalLiteral::Boolean(r)) => fold_boolean(op, *l, *r),
        (LogicalLiteral::String(l), LogicalLiteral::String(r)) => match op {
            BinaryOp::Concat => Some(LogicalLiteral::String(format!("{l}{r}"))),
            BinaryOp::Eq => Some(LogicalLiteral::Boolean(l == r)),
            BinaryOp::Neq => Some(LogicalLiteral::Boolean(l != r)),
            BinaryOp::Lt => Some(LogicalLiteral::Boolean(l < r)),
            BinaryOp::Gt => Some(LogicalLiteral::Boolean(l > r)),
            BinaryOp::Leq => Some(LogicalLiteral::Boolean(l <= r)),
            BinaryOp::Geq => Some(LogicalLiteral::Boolean(l >= r)),
            _ => None,
        },
        (LogicalLiteral::Date(l), LogicalLiteral::Date(r)) => {
            fold_ordered_scalars(op, *l, *r)
        }
        (LogicalLiteral::Timestamp(l), LogicalLiteral::Timestamp(r))
        | (LogicalLiteral::Time(l), LogicalLiteral::Time(r)) => {
            fold_ordered_scalars(op, *l, *r)
        }
        (LogicalLiteral::Decimal(l, lp, ls), LogicalLiteral::Decimal(r, rp, rs))
        if lp == rp && ls == rs =>
            {
                fold_ordered_scalars(op, *l, *r)
            }
        _ => None,
    }
}

/// Folds SQL three-valued boolean combinations involving NULL.
fn fold_nullable_boolean(
    op: BinaryOp,
    left: &LogicalLiteral,
    right: &LogicalLiteral,
) -> Option<LogicalLiteral> {
    let left_null = matches!(left, LogicalLiteral::Null(_));
    let right_null = matches!(right, LogicalLiteral::Null(_));

    match (op, left, right) {
        (BinaryOp::And, LogicalLiteral::Boolean(false), _)
        | (BinaryOp::And, _, LogicalLiteral::Boolean(false)) => {
            Some(LogicalLiteral::Boolean(false))
        }
        (BinaryOp::Or, LogicalLiteral::Boolean(true), _)
        | (BinaryOp::Or, _, LogicalLiteral::Boolean(true)) => {
            Some(LogicalLiteral::Boolean(true))
        }
        (BinaryOp::And | BinaryOp::Or, _, _)
        if left_null || right_null =>
            {
                Some(LogicalLiteral::Null(VeinDataType::Boolean))
            }
        _ => None,
    }
}

/// Folds equality and ordering for scalar values with a shared representation.
fn fold_ordered_scalars<T: PartialOrd + PartialEq>(op: BinaryOp, left: T, right: T) -> Option<LogicalLiteral> {
    let result = match op {
        BinaryOp::Eq => left == right,
        BinaryOp::Neq => left != right,
        BinaryOp::Lt => left < right,
        BinaryOp::Gt => left > right,
        BinaryOp::Leq => left <= right,
        BinaryOp::Geq => left >= right,
        _ => return None,
    };
    Some(LogicalLiteral::Boolean(result))
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

        let optimized = rule.apply(&plan, &OptimizerContext::default()).unwrap().expect("plan must change");
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
        assert!(rule.apply(&plan, &OptimizerContext::default()).unwrap().is_none());
    }

    #[test]
    fn project_exprs_are_folded() {
        let plan = project(vec![bin(BinaryOp::Multiply, lit(3), lit(4))]);
        let optimized = ConstantFoldingRule
            .apply(&plan, &OptimizerContext::default())
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
            .apply(&plan, &OptimizerContext::default())
            .unwrap()
            .expect("plan must change");
        let LogicalPlan::Filter { predicate, .. } = optimized else {
            panic!("expected a Filter node");
        };
        assert_eq!(predicate, LogicalExpr::Literal(LogicalLiteral::Boolean(false)));
    }

    #[test]
    fn folds_unary_and_null_checks() {
        assert_eq!(
            fold_expr(&LogicalExpr::Not {
                expr: Box::new(LogicalExpr::Literal(LogicalLiteral::Boolean(true))),
            }),
            LogicalExpr::Literal(LogicalLiteral::Boolean(false))
        );
        assert_eq!(
            fold_expr(&LogicalExpr::Negate {
                expr: Box::new(lit(7)),
            }),
            lit(-7)
        );
        assert_eq!(
            fold_expr(&LogicalExpr::IsNull {
                expr: Box::new(LogicalExpr::Literal(LogicalLiteral::Null(
                    VeinDataType::Int64,
                ))),
            }),
            LogicalExpr::Literal(LogicalLiteral::Boolean(true))
        );
        assert_eq!(
            fold_expr(&LogicalExpr::IsNotNull {
                expr: Box::new(lit(7)),
            }),
            LogicalExpr::Literal(LogicalLiteral::Boolean(true))
        );
    }

    #[test]
    fn folds_in_list_and_case() {
        let in_list = LogicalExpr::InList {
            expr: Box::new(lit(2)),
            list: vec![lit(1), lit(2), lit(3)],
            negated: false,
        };
        assert_eq!(
            fold_expr(&in_list),
            LogicalExpr::Literal(LogicalLiteral::Boolean(true))
        );

        let case = LogicalExpr::Case {
            when: vec![LogicalExpr::Literal(LogicalLiteral::Boolean(false))],
            then: vec![lit(1)],
            else_: Some(Box::new(lit(2))),
            return_type: VeinDataType::Int64,
        };
        assert_eq!(fold_expr(&case), lit(2));
    }

    #[test]
    fn folds_children_inside_function_and_preserves_call() {
        let call = LogicalExpr::FunctionCall {
            name: "abs".to_string(),
            args: vec![bin(BinaryOp::Plus, lit(2), lit(3))],
            is_aggregate: false,
            distinct: false,
            return_type: VeinDataType::Int64,
        };
        let folded = fold_expr(&call);
        assert!(matches!(
            folded,
            LogicalExpr::FunctionCall { ref args, .. } if args == &vec![lit(5)]
        ));
    }

    #[test]
    fn folds_safe_literal_casts() {
        assert_eq!(
            fold_expr(&LogicalExpr::Cast {
                expr: Box::new(LogicalExpr::Literal(LogicalLiteral::String("42".into()))),
                target_type: VeinDataType::Int64,
            }),
            lit(42)
        );
        assert_eq!(
            fold_expr(&LogicalExpr::Cast {
                expr: Box::new(lit(7)),
                target_type: VeinDataType::Float64,
            }),
            float(7.0)
        );
        assert_eq!(
            fold_expr(&LogicalExpr::Cast {
                expr: Box::new(LogicalExpr::Literal(LogicalLiteral::Null(
                    VeinDataType::Int64,
                ))),
                target_type: VeinDataType::Float64,
            }),
            LogicalExpr::Literal(LogicalLiteral::Null(VeinDataType::Float64))
        );
    }

    #[test]
    fn preserves_unsafe_or_unsupported_casts() {
        let expr = LogicalExpr::Cast {
            expr: Box::new(LogicalExpr::Literal(LogicalLiteral::String("not-a-number".into()))),
            target_type: VeinDataType::Int64,
        };
        assert_eq!(fold_expr(&expr), expr);
    }

    #[test]
    fn folds_like_temporal_and_decimal_literals() {
        let like = LogicalExpr::Like {
            expr: Box::new(LogicalExpr::Literal(LogicalLiteral::String(
                "vein optimizer".into(),
            ))),
            pattern: Box::new(LogicalExpr::Literal(LogicalLiteral::String(
                "vein%".into(),
            ))),
            negated: false,
        };
        assert_eq!(
            fold_expr(&like),
            LogicalExpr::Literal(LogicalLiteral::Boolean(true))
        );

        assert_eq!(
            fold_expr(&bin(
                BinaryOp::Geq,
                LogicalExpr::Literal(LogicalLiteral::Date(10)),
                LogicalExpr::Literal(LogicalLiteral::Date(9)),
            )),
            LogicalExpr::Literal(LogicalLiteral::Boolean(true))
        );
        assert_eq!(
            fold_expr(&bin(
                BinaryOp::Eq,
                LogicalExpr::Literal(LogicalLiteral::Decimal(1234, 10, 2)),
                LogicalExpr::Literal(LogicalLiteral::Decimal(1234, 10, 2)),
            )),
            LogicalExpr::Literal(LogicalLiteral::Boolean(true))
        );
    }

    #[test]
    fn preserves_sql_null_comparison() {
        let expr = bin(
            BinaryOp::Eq,
            LogicalExpr::Literal(LogicalLiteral::Null(VeinDataType::Int64)),
            lit(1),
        );
        assert_eq!(
            fold_expr(&expr),
            LogicalExpr::Literal(LogicalLiteral::Null(VeinDataType::Boolean))
        );

        assert_eq!(
            fold_expr(&bin(
                BinaryOp::And,
                LogicalExpr::Literal(LogicalLiteral::Boolean(false)),
                LogicalExpr::Literal(LogicalLiteral::Null(VeinDataType::Boolean)),
            )),
            LogicalExpr::Literal(LogicalLiteral::Boolean(false))
        );
        assert_eq!(
            fold_expr(&bin(
                BinaryOp::Or,
                LogicalExpr::Literal(LogicalLiteral::Boolean(true)),
                LogicalExpr::Literal(LogicalLiteral::Null(VeinDataType::Boolean)),
            )),
            LogicalExpr::Literal(LogicalLiteral::Boolean(true))
        );
    }
}
