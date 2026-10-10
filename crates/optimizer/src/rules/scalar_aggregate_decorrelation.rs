//! Decorrelation for scalar aggregate subqueries.

use error::Result;
use parser::ast::expr::BinaryOp;
use plan::{
    LogicalExpr, LogicalPlan, PlanTraversal, ResolvedColumn, ResolvedColumnRef, VeinDataType,
    VeinSchema,
};
use std::sync::Arc;
use crate::context::{OptimizerContext, OptimizerPhase};
use crate::rule::{OptimizerRule, RuleType};

/// Rewrites a supported correlated scalar aggregate into a grouped aggregate
/// and a left join. This keeps evaluation set based and avoids per-row
/// subquery execution.
pub struct ScalarAggregateDecorrelationRule;

struct Decorrelation {
    right: LogicalPlan,
    correlation: Option<(ResolvedColumnRef, ResolvedColumnRef)>,
    value: ResolvedColumnRef,
    aggregate_name: String,
}

impl OptimizerRule for ScalarAggregateDecorrelationRule {
    fn name(&self) -> &str {
        "ScalarAggregateDecorrelationRule"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Heuristic
    }

    fn phase(&self) -> OptimizerPhase {
        OptimizerPhase::Heuristic
    }

    fn apply(&self, plan: &LogicalPlan, _context: &OptimizerContext) -> Result<Option<LogicalPlan>> {
        let LogicalPlan::Filter { input, predicate } = plan else {
            return Ok(None);
        };

        let mut decorrelations = Vec::new();
        let (predicate, changed) =
            rewrite_expression(predicate.clone(), input.schema(), &mut decorrelations)?;
        if !changed {
            return Ok(None);
        }

        let outer_schema = input.schema().clone();
        let mut input = (**input).clone();
        let needs_outer_projection = decorrelations
            .iter()
            .any(|item| item.correlation.is_none());
        for decorrelation in decorrelations {
            let mut fields = input.schema().fields.clone();
            fields.extend(decorrelation.right.schema().fields.iter().map(|field| {
                if decorrelation.correlation.is_some() {
                    nullable_column(field)
                } else {
                    ResolvedColumnRef::clone(field)
                }
            }));
            let schema = VeinSchema::new(fields);
            let (join_type, on) = match decorrelation.correlation {
                Some((outer_key, inner_key)) => (
                    parser::ast::pipe_op::JoinType::Left,
                    Some(LogicalExpr::BinaryOp {
                        left: Box::new(LogicalExpr::Column(outer_key)),
                        op: BinaryOp::Eq,
                        right: Box::new(LogicalExpr::Column(inner_key)),
                        return_type: VeinDataType::Boolean,
                    }),
                ),
                None => (parser::ast::pipe_op::JoinType::Cross, None),
            };
            tracing::debug!(
                target: "vein.audit",
                event = "scalar_aggregate_decorrelated",
                aggregate = %decorrelation.aggregate_name,
                "scalar_aggregate_decorrelated"
            );
            input = LogicalPlan::Join {
                join_type,
                left: Box::new(input),
                right: Box::new(decorrelation.right),
                on,
                using: Vec::new(),
                schema: Box::new(schema),
            };
        }

        let filtered = LogicalPlan::Filter {
            input: Box::new(input),
            predicate,
        };
        if needs_outer_projection {
            let exprs = outer_schema
                .fields
                .iter()
                .cloned()
                .map(LogicalExpr::Column)
                .collect();
            return Ok(Some(LogicalPlan::Project {
                input: Box::new(filtered),
                exprs,
                schema: Box::new(outer_schema),
            }));
        }
        Ok(Some(filtered))
    }
}

fn rewrite_expression(
    expr: LogicalExpr,
    outer_schema: &VeinSchema,
    decorrelations: &mut Vec<Decorrelation>,
) -> Result<(LogicalExpr, bool)> {
    if let LogicalExpr::ScalarSubquery(subquery) = expr {
        if let Some(decorrelation) = decorrelate_scalar_aggregate(&subquery, outer_schema) {
            let value = LogicalExpr::Column(ResolvedColumnRef::clone(&decorrelation.value));
            decorrelations.push(decorrelation);
            return Ok((value, true));
        }
        return Ok((LogicalExpr::ScalarSubquery(subquery), false));
    }

    let mut changed = false;
    let rewritten = expr.map_children(&mut |child| {
        let (child, child_changed) = rewrite_expression(child, outer_schema, decorrelations)?;
        changed |= child_changed;
        Ok(child)
    })?;
    Ok((rewritten, changed))
}

fn decorrelate_scalar_aggregate(
    subquery: &LogicalPlan,
    outer_schema: &VeinSchema,
) -> Option<Decorrelation> {
    let LogicalPlan::Aggregate {
        input,
        grouping_sets,
        aggs,
        schema,
    } = subquery
    else {
        return None;
    };
    if grouping_sets.len() != 1
        || !grouping_sets[0].is_empty()
        || aggs.len() != 1
        || schema.fields.len() != 1
    {
        return None;
    }
    let mut aggregate_calls = Vec::new();
    collect_aggregate_calls(&aggs[0], &mut aggregate_calls);
    let [LogicalExpr::FunctionCall {
        name,
        is_aggregate: true,
        distinct: false,
        return_type,
        ..
    }] = aggregate_calls.as_slice()
    else {
        return None;
    };
    if !matches!(name.to_ascii_uppercase().as_str(), "COUNT" | "MIN" | "MAX" | "SUM" | "AVG") {
        return None;
    }
    let value = ResolvedColumnRef::clone(schema.fields.first()?);

    let mut base = input.as_ref();
    let mut terms = Vec::new();
    while let LogicalPlan::Filter { input, predicate } = base {
        split_conjunction(predicate, &mut terms);
        base = input;
    }

    let mut correlation = None;
    let mut residual = Vec::new();
    for term in terms {
        if let Some((inner, outer)) = equality_correlation(&term) {
            if correlation.is_some()
                || base.schema().position_of_id(inner.id).is_none()
                || outer_schema.position_of_id(outer.id).is_none()
            {
                return None;
            }
            correlation = Some((inner, outer));
        } else if contains_outer_column(&term) {
            return None;
        } else {
            residual.push(term);
        }
    }
    if name.eq_ignore_ascii_case("count") && correlation.is_some() {
        // A missing grouped COUNT is zero, but a LEFT JOIN represents it as
        // NULL. Keep correlated COUNT unchanged until that default is modeled.
        return None;
    }
    if contains_outer_column(&aggs[0]) {
        return None;
    }

    let mut grouped_input = base.clone();
    if let Some(predicate) = conjunction(residual) {
        grouped_input = LogicalPlan::Filter {
            input: Box::new(grouped_input),
            predicate,
        };
    }

    let aggregate_value = Arc::new(
        ResolvedColumn::new(value.id, value.name.clone(), value.qualifier.clone(), return_type.clone())
            .with_nullable(!name.eq_ignore_ascii_case("count")),
    );
    let mut grouped_aggs = Vec::with_capacity(1);
    let mut replacement_count = 0;
    let rewritten_value = replace_aggregate_calls(
        aggs[0].clone(),
        &aggregate_value,
        &mut replacement_count,
        &mut grouped_aggs,
    )
    .ok()?;
    if replacement_count != 1 || grouped_aggs.len() != 1 {
        return None;
    }

    let grouping_sets = match &correlation {
        Some((inner_key, _)) => vec![vec![LogicalExpr::Column(ResolvedColumnRef::clone(inner_key))]],
        None => vec![Vec::new()],
    };
    let mut aggregate_fields = Vec::with_capacity(2);
    if let Some((inner_key, _)) = &correlation {
        aggregate_fields.push(ResolvedColumnRef::clone(inner_key));
    }
    aggregate_fields.push(Arc::clone(&aggregate_value));
    let aggregate = LogicalPlan::Aggregate {
        input: Box::new(grouped_input),
        grouping_sets,
        aggs: grouped_aggs,
        schema: Box::new(VeinSchema::new(aggregate_fields)),
    };
    let (right, projected_value) = if &rewritten_value == &LogicalExpr::Column(Arc::clone(&aggregate_value)) {
        (aggregate, aggregate_value)
    } else {
        let mut exprs = Vec::with_capacity(2);
        let mut fields = Vec::with_capacity(2);
        if let Some((inner_key, _)) = &correlation {
            exprs.push(LogicalExpr::Column(ResolvedColumnRef::clone(inner_key)));
            fields.push(ResolvedColumnRef::clone(inner_key));
        }
        exprs.push(rewritten_value);
        fields.push(ResolvedColumnRef::clone(&value));
        let projected = LogicalPlan::Project {
            input: Box::new(aggregate),
            exprs,
            schema: Box::new(VeinSchema::new(fields)),
        };
        (projected, value)
    };

    Some(Decorrelation {
        right,
        correlation,
        value: projected_value,
        aggregate_name: name.clone(),
    })
}

fn collect_aggregate_calls(expr: &LogicalExpr, output: &mut Vec<LogicalExpr>) {
    if matches!(expr, LogicalExpr::FunctionCall { is_aggregate: true, .. }) {
        output.push(expr.clone());
        return;
    }
    for child in expr.children() {
        collect_aggregate_calls(child, output);
    }
}

fn replace_aggregate_calls(
    expr: LogicalExpr,
    value: &ResolvedColumnRef,
    replacements: &mut usize,
    aggregate_calls: &mut Vec<LogicalExpr>,
) -> Result<LogicalExpr> {
    if matches!(expr, LogicalExpr::FunctionCall { is_aggregate: true, .. }) {
        *replacements += 1;
        aggregate_calls.push(expr);
        return Ok(LogicalExpr::Column(ResolvedColumnRef::clone(value)));
    }
    expr.map_children(&mut |child| {
        replace_aggregate_calls(child, value, replacements, aggregate_calls)
    })
}

fn equality_correlation(term: &LogicalExpr) -> Option<(ResolvedColumnRef, ResolvedColumnRef)> {
    let LogicalExpr::BinaryOp {
        left,
        op: BinaryOp::Eq,
        right,
        ..
    } = term
    else {
        return None;
    };
    match (left.as_ref(), right.as_ref()) {
        (LogicalExpr::Column(inner), LogicalExpr::OuterColumn(outer))
        | (LogicalExpr::OuterColumn(outer), LogicalExpr::Column(inner)) => {
            Some((ResolvedColumnRef::clone(inner), ResolvedColumnRef::clone(outer)))
        }
        _ => None,
    }
}

fn split_conjunction(expr: &LogicalExpr, terms: &mut Vec<LogicalExpr>) {
    if let LogicalExpr::BinaryOp {
        left,
        op: BinaryOp::And,
        right,
        ..
    } = expr
    {
        split_conjunction(left, terms);
        split_conjunction(right, terms);
    } else {
        terms.push(expr.clone());
    }
}

fn conjunction(terms: Vec<LogicalExpr>) -> Option<LogicalExpr> {
    let mut terms = terms.into_iter();
    let mut expr = terms.next()?;
    for right in terms {
        expr = LogicalExpr::BinaryOp {
            left: Box::new(expr),
            op: BinaryOp::And,
            right: Box::new(right),
            return_type: VeinDataType::Boolean,
        };
    }
    Some(expr)
}

fn contains_outer_column(expr: &LogicalExpr) -> bool {
    matches!(expr, LogicalExpr::OuterColumn(_))
        || expr.children().into_iter().any(contains_outer_column)
}

fn nullable_column(column: &ResolvedColumnRef) -> ResolvedColumnRef {
    Arc::new(ResolvedColumn {
        nullable: true,
        ..column.as_ref().clone()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use plan::{ColumnId, LogicalLiteral, ResolvedColumn, TableMetadata};
    use std::sync::Arc;

    fn field(id: usize, name: &str, qualifier: &str) -> ResolvedColumnRef {
        Arc::new(ResolvedColumn::new(
            ColumnId(id),
            name,
            Some(qualifier.to_string()),
            VeinDataType::Int64,
        ))
    }

    fn scan(name: &str, fields: Vec<ResolvedColumnRef>) -> LogicalPlan {
        let schema = VeinSchema::new(fields);
        LogicalPlan::Scan {
            table: Arc::new(TableMetadata::new(name, Arc::new(schema.clone()))),
            projection: None,
            filters: Vec::new(),
            schema: Box::new(schema),
        }
    }

    fn correlated_min_subquery(
        inner_key: &ResolvedColumnRef,
        measure: &ResolvedColumnRef,
        outer_key: &ResolvedColumnRef,
        output: &ResolvedColumnRef,
    ) -> LogicalPlan {
        let input = scan(
            "inner_fixture",
            vec![Arc::clone(inner_key), Arc::clone(measure)],
        );
        let filtered = LogicalPlan::Filter {
            input: Box::new(input),
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(Arc::clone(inner_key))),
                op: BinaryOp::Eq,
                right: Box::new(LogicalExpr::OuterColumn(Arc::clone(outer_key))),
                return_type: VeinDataType::Boolean,
            },
        };
        LogicalPlan::Aggregate {
            input: Box::new(filtered),
            grouping_sets: vec![Vec::new()],
            aggs: vec![LogicalExpr::FunctionCall {
                name: "min".to_string(),
                args: vec![LogicalExpr::Column(Arc::clone(measure))],
                is_aggregate: true,
                distinct: false,
                return_type: VeinDataType::Int64,
            }],
            schema: Box::new(VeinSchema::new(vec![Arc::clone(output)])),
        }
    }

    #[test]
    fn decorrelates_scalar_min_to_grouped_left_join() {
        let outer_key = field(1, "outer_key", "outer_table");
        let outer_value = field(2, "outer_value", "outer_table");
        let inner_key = field(3, "inner_key", "inner_table");
        let measure = field(4, "measure", "inner_table");
        let min_value = field(5, "min_value", "inner_table");
        let input = scan(
            "outer_fixture",
            vec![Arc::clone(&outer_key), Arc::clone(&outer_value)],
        );
        let subquery = correlated_min_subquery(&inner_key, &measure, &outer_key, &min_value);
        let filter = LogicalPlan::Filter {
            input: Box::new(input),
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(Arc::clone(&outer_value))),
                op: BinaryOp::Eq,
                right: Box::new(LogicalExpr::ScalarSubquery(Box::new(subquery))),
                return_type: VeinDataType::Boolean,
            },
        };

        let rewritten = ScalarAggregateDecorrelationRule
            .apply(&filter, &OptimizerContext::default())
            .expect("decorrelation should not fail")
            .expect("supported scalar aggregate should decorrelate");
        let LogicalPlan::Filter { input, predicate } = rewritten else {
            panic!("rewrite should preserve the filter");
        };
        let LogicalPlan::Join {
            join_type,
            schema,
            right,
            on,
            ..
        } = input.as_ref()
        else {
            panic!("correlated aggregate should become a join");
        };
        assert_eq!(*join_type, parser::ast::pipe_op::JoinType::Left);
        assert!(matches!(right.as_ref(), LogicalPlan::Aggregate { grouping_sets, schema, .. }
            if grouping_sets == &vec![vec![LogicalExpr::Column(Arc::clone(&inner_key))]]
                && schema.fields[1].nullable));
        assert!(schema.fields[2].nullable);
        assert!(matches!(on, Some(LogicalExpr::BinaryOp { op: BinaryOp::Eq, .. })));
        assert!(matches!(predicate, LogicalExpr::BinaryOp { right, .. }
            if matches!(right.as_ref(), LogicalExpr::Column(column) if column.id == min_value.id)));
    }

    #[test]
    fn decorrelates_uncorrelated_global_sum_arithmetic_to_cross_join() {
        let outer = field(21, "outer_value", "outer_fixture");
        let measure = field(22, "measure", "inner_fixture");
        let scalar = field(23, "scalar_value", "inner_fixture");
        let input = scan("outer_fixture", vec![Arc::clone(&outer)]);
        let sum = LogicalExpr::FunctionCall {
            name: "sum".to_string(),
            args: vec![LogicalExpr::Column(Arc::clone(&measure))],
            is_aggregate: true,
            distinct: false,
            return_type: VeinDataType::Int64,
        };
        let expression = LogicalExpr::BinaryOp {
            left: Box::new(sum),
            op: BinaryOp::Multiply,
            right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(10))),
            return_type: VeinDataType::Int64,
        };
        let subquery = LogicalPlan::Aggregate {
            input: Box::new(scan("inner_fixture", vec![Arc::clone(&measure)])),
            grouping_sets: vec![Vec::new()],
            aggs: vec![expression],
            schema: Box::new(VeinSchema::new(vec![Arc::clone(&scalar)])),
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(input),
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(Arc::clone(&outer))),
                op: BinaryOp::Gt,
                right: Box::new(LogicalExpr::ScalarSubquery(Box::new(subquery))),
                return_type: VeinDataType::Boolean,
            },
        };

        let rewritten = ScalarAggregateDecorrelationRule
            .apply(&filter, &OptimizerContext::default())
            .expect("uncorrelated aggregate should rewrite")
            .expect("global scalar aggregate should decorrelate");
        let LogicalPlan::Project { input, exprs, schema } = rewritten else {
            panic!("scalar join column must be removed after filtering");
        };
        assert_eq!(schema.fields, vec![Arc::clone(&outer)]);
        assert_eq!(exprs, vec![LogicalExpr::Column(Arc::clone(&outer))]);
        let LogicalPlan::Filter { input, predicate } = input.as_ref() else {
            panic!("filter must remain above the scalar join");
        };
        assert!(matches!(predicate, LogicalExpr::BinaryOp { right, .. }
            if matches!(right.as_ref(), LogicalExpr::Column(column) if column.id == scalar.id)));
        let LogicalPlan::Join { join_type, on, right, .. } = input.as_ref() else {
            panic!("uncorrelated scalar aggregate must become a join");
        };
        assert_eq!(*join_type, parser::ast::pipe_op::JoinType::Cross);
        assert!(on.is_none(), "CROSS JOIN carries no predicate");
        let LogicalPlan::Project { input, exprs, .. } = right.as_ref() else {
            panic!("aggregate arithmetic must be projected after reduction");
        };
        assert!(matches!(input.as_ref(), LogicalPlan::Aggregate { grouping_sets, aggs, .. }
            if grouping_sets == &vec![Vec::new()]
                && matches!(aggs.as_slice(), [LogicalExpr::FunctionCall { name, .. }] if name.eq_ignore_ascii_case("sum"))));
        assert!(matches!(exprs.as_slice(), [LogicalExpr::BinaryOp { left, op: BinaryOp::Multiply, .. }]
            if matches!(left.as_ref(), LogicalExpr::Column(column) if column.id == scalar.id)));
    }

    #[test]
    fn leaves_potentially_multirow_scalar_aggregate_unchanged() {
        let outer = field(31, "outer_value", "outer_fixture");
        let measure = field(32, "measure", "inner_fixture");
        let output = field(33, "group_value", "inner_fixture");
        let subquery = LogicalPlan::Aggregate {
            input: Box::new(scan("inner_fixture", vec![Arc::clone(&measure)])),
            grouping_sets: vec![vec![LogicalExpr::Column(Arc::clone(&measure))]],
            aggs: vec![LogicalExpr::FunctionCall {
                name: "sum".to_string(),
                args: vec![LogicalExpr::Column(Arc::clone(&measure))],
                is_aggregate: true,
                distinct: false,
                return_type: VeinDataType::Int64,
            }],
            schema: Box::new(VeinSchema::new(vec![Arc::clone(&measure), output])),
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(scan("outer_fixture", vec![Arc::clone(&outer)])),
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(outer)),
                op: BinaryOp::Eq,
                right: Box::new(LogicalExpr::ScalarSubquery(Box::new(subquery))),
                return_type: VeinDataType::Boolean,
            },
        };

        assert!(ScalarAggregateDecorrelationRule
            .apply(&filter, &OptimizerContext::default())
            .expect("unsupported scalar cardinality must not be rewritten")
            .is_none());
    }

    #[test]
    fn leaves_count_scalar_subqueries_unchanged_to_preserve_empty_input_semantics() {
        let outer_key = field(11, "outer_key", "outer_table");
        let inner_key = field(12, "inner_key", "inner_table");
        let count_value = field(13, "count_value", "inner_table");
        let input = scan("outer_fixture", vec![Arc::clone(&outer_key)]);
        let correlated = correlated_min_subquery(
            &inner_key,
            &field(14, "measure", "inner_table"),
            &outer_key,
            &count_value,
        );
        let LogicalPlan::Aggregate { input: correlated_input, schema, .. } = correlated else {
            unreachable!();
        };
        let count = LogicalPlan::Aggregate {
            input: correlated_input,
            grouping_sets: vec![Vec::new()],
            aggs: vec![LogicalExpr::FunctionCall {
                name: "count".to_string(),
                args: vec![LogicalExpr::Literal(LogicalLiteral::Int64(1))],
                is_aggregate: true,
                distinct: false,
                return_type: VeinDataType::Int64,
            }],
            schema,
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(input),
            predicate: LogicalExpr::ScalarSubquery(Box::new(count)),
        };

        assert!(ScalarAggregateDecorrelationRule
            .apply(&filter, &OptimizerContext::default())
            .expect("unsupported forms should be left to another implementation")
            .is_none());
    }
}
