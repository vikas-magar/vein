//! Normalize aggregate expressions that need scalar evaluation after grouping.

use plan::{ColumnId, LogicalExpr, ResolvedColumn, ResolvedColumnRef, VeinSchema};
use std::sync::Arc;

/// Raw aggregate calls and the vector projection that restores visible outputs.
pub(super) struct AggregateProjection {
    pub(super) aggregates: Vec<LogicalExpr>,
    pub(super) intermediate_schema: VeinSchema,
    pub(super) expressions: Vec<LogicalExpr>,
}

/// Extract aggregate calls when an aggregate output contains scalar operators.
/// Direct aggregate-only plans stay on their existing path without an extra
/// projection step.
pub(super) fn normalize<F>(
    grouping_sets: &[Vec<LogicalExpr>],
    aggregates: &[LogicalExpr],
    visible_schema: &VeinSchema,
    mut next_id: F,
) -> Option<AggregateProjection>
where
    F: FnMut() -> ColumnId,
{
    if aggregates.iter().all(is_aggregate_call) {
        return None;
    }

    let mut grouping_expressions = Vec::new();
    for expression in grouping_sets.iter().flatten() {
        if !grouping_expressions.contains(&expression) {
            grouping_expressions.push(expression);
        }
    }
    let group_count = grouping_expressions.len();
    if visible_schema.fields.len() != group_count + aggregates.len() {
        return None;
    }
    let group_fields = visible_schema.fields.get(..group_count)?;
    let mut intermediate_fields = group_fields.to_vec();
    let mut raw_aggregates = Vec::new();
    let mut expressions: Vec<_> = group_fields
        .iter()
        .cloned()
        .map(LogicalExpr::Column)
        .collect();

    for aggregate in aggregates.iter().cloned() {
        expressions.push(extract_calls(
            aggregate,
            &mut raw_aggregates,
            &mut intermediate_fields,
            &mut next_id,
        ));
    }

    if raw_aggregates.is_empty() {
        return None;
    }

    Some(AggregateProjection {
        aggregates: raw_aggregates,
        intermediate_schema: VeinSchema {
            fields: intermediate_fields,
        },
        expressions,
    })
}

fn is_aggregate_call(expression: &LogicalExpr) -> bool {
    matches!(expression, LogicalExpr::FunctionCall { is_aggregate: true, .. })
}

fn extract_calls<F>(
    expression: LogicalExpr,
    aggregates: &mut Vec<LogicalExpr>,
    fields: &mut Vec<ResolvedColumnRef>,
    next_id: &mut F,
) -> LogicalExpr
where
    F: FnMut() -> ColumnId,
{
    let nullable = expression.nullable();
    match expression {
        LogicalExpr::FunctionCall {
            name,
            args,
            is_aggregate: true,
            distinct,
            return_type,
        } => {
            let column = Arc::new(
                ResolvedColumn::new(
                    next_id(),
                    format!("__aggregate_result_{}", fields.len()),
                    None,
                    return_type.clone(),
                )
                    .with_nullable(nullable),
            );
            aggregates.push(LogicalExpr::FunctionCall {
                name,
                args,
                is_aggregate: true,
                distinct,
                return_type,
            });
            fields.push(Arc::clone(&column));
            LogicalExpr::Column(column)
        }
        LogicalExpr::BinaryOp {
            left,
            op,
            right,
            return_type,
        } => LogicalExpr::BinaryOp {
            left: Box::new(extract_calls(*left, aggregates, fields, next_id)),
            op,
            right: Box::new(extract_calls(*right, aggregates, fields, next_id)),
            return_type,
        },
        LogicalExpr::Cast { expr, target_type } => LogicalExpr::Cast {
            expr: Box::new(extract_calls(*expr, aggregates, fields, next_id)),
            target_type,
        },
        LogicalExpr::Not { expr } => LogicalExpr::Not {
            expr: Box::new(extract_calls(*expr, aggregates, fields, next_id)),
        },
        LogicalExpr::Negate { expr } => LogicalExpr::Negate {
            expr: Box::new(extract_calls(*expr, aggregates, fields, next_id)),
        },
        LogicalExpr::IsNull { expr } => LogicalExpr::IsNull {
            expr: Box::new(extract_calls(*expr, aggregates, fields, next_id)),
        },
        LogicalExpr::IsNotNull { expr } => LogicalExpr::IsNotNull {
            expr: Box::new(extract_calls(*expr, aggregates, fields, next_id)),
        },
        LogicalExpr::InList { expr, list, negated } => LogicalExpr::InList {
            expr: Box::new(extract_calls(*expr, aggregates, fields, next_id)),
            list: list
                .into_iter()
                .map(|item| extract_calls(item, aggregates, fields, next_id))
                .collect(),
            negated,
        },
        LogicalExpr::Like { expr, pattern, negated } => LogicalExpr::Like {
            expr: Box::new(extract_calls(*expr, aggregates, fields, next_id)),
            pattern: Box::new(extract_calls(*pattern, aggregates, fields, next_id)),
            negated,
        },
        LogicalExpr::Case { when, then, else_, return_type } => LogicalExpr::Case {
            when: when.into_iter().map(|e| extract_calls(e, aggregates, fields, next_id)).collect(),
            then: then.into_iter().map(|e| extract_calls(e, aggregates, fields, next_id)).collect(),
            else_: else_.map(|e| Box::new(extract_calls(*e, aggregates, fields, next_id))),
            return_type,
        },
        LogicalExpr::FunctionCall { name, args, is_aggregate, distinct, return_type } => {
            LogicalExpr::FunctionCall {
                name,
                args: args.into_iter().map(|e| extract_calls(e, aggregates, fields, next_id)).collect(),
                is_aggregate,
                distinct,
                return_type,
            }
        }
        LogicalExpr::AiOperator { name, args, return_type } => LogicalExpr::AiOperator {
            name,
            args: args.into_iter().map(|e| extract_calls(e, aggregates, fields, next_id)).collect(),
            return_type,
        },
        LogicalExpr::WasmUdf { bytecode_ref, args, return_type } => LogicalExpr::WasmUdf {
            bytecode_ref,
            args: args.into_iter().map(|e| extract_calls(e, aggregates, fields, next_id)).collect(),
            return_type,
        },
        LogicalExpr::EbpfUdf { bytecode_ref, args, return_type } => LogicalExpr::EbpfUdf {
            bytecode_ref,
            args: args.into_iter().map(|e| extract_calls(e, aggregates, fields, next_id)).collect(),
            return_type,
        },
        LogicalExpr::PythonUdf { bytecode_ref, args, return_type } => LogicalExpr::PythonUdf {
            bytecode_ref,
            args: args.into_iter().map(|e| extract_calls(e, aggregates, fields, next_id)).collect(),
            return_type,
        },
        leaf => leaf,
    }
}
