//! Decorrelation for scalar aggregates with equality correlation predicates.

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
    outer_key: ResolvedColumnRef,
    inner_key: ResolvedColumnRef,
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

        let mut input = (**input).clone();
        for decorrelation in decorrelations {
            let mut fields = input.schema().fields.clone();
            fields.extend(
                decorrelation
                    .right
                    .schema()
                    .fields
                    .iter()
                    .map(|field| nullable_column(field)),
            );
            let schema = VeinSchema::new(fields);
            let on = LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(decorrelation.outer_key)),
                op: BinaryOp::Eq,
                right: Box::new(LogicalExpr::Column(decorrelation.inner_key)),
                return_type: VeinDataType::Boolean,
            };
            tracing::debug!(
                target: "vein.audit",
                event = "scalar_aggregate_decorrelated",
                aggregate = %decorrelation.aggregate_name,
                "scalar_aggregate_decorrelated"
            );
            input = LogicalPlan::Join {
                join_type: parser::ast::pipe_op::JoinType::Left,
                left: Box::new(input),
                right: Box::new(decorrelation.right),
                on: Some(on),
                using: Vec::new(),
                schema: Box::new(schema),
            };
        }

        Ok(Some(LogicalPlan::Filter {
            input: Box::new(input),
            predicate,
        }))
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
    let LogicalExpr::FunctionCall {
        name,
        is_aggregate: true,
        distinct: false,
        ..
    } = &aggs[0]
    else {
        return None;
    };
    // These aggregate results are NULL for an empty group. A left join then
    // preserves scalar-subquery behavior when no correlated rows match.
    if !matches!(name.to_ascii_uppercase().as_str(), "MIN" | "MAX" | "SUM" | "AVG") {
        return None;
    }
    // An aggregate can return NULL for a non-empty input (for example when all
    // input values are NULL), and the left join also yields NULL when no group
    // exists. Keep this visible in the rewritten column metadata.
    let value = nullable_column(schema.fields.first()?);

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
    let (inner_key, outer_key) = correlation?;
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

    let grouped_schema = Box::new(VeinSchema::new(vec![
        ResolvedColumnRef::clone(&inner_key),
        ResolvedColumnRef::clone(&value),
    ]));
    let right = LogicalPlan::Aggregate {
        input: Box::new(grouped_input),
        grouping_sets: vec![vec![LogicalExpr::Column(ResolvedColumnRef::clone(
            &inner_key,
        ))]],
        aggs: aggs.clone(),
        schema: grouped_schema,
    };

    Some(Decorrelation {
        right,
        outer_key,
        inner_key,
        value,
        aggregate_name: name.clone(),
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
