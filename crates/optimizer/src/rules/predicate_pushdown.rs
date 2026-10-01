//! Predicate pushdown through safe relational operators.

use std::collections::{HashMap, HashSet};

use error::Result;
use parser::ast::pipe_op::JoinType;
use parser::ast::query::SetOpKind;
use plan::{LogicalExpr, LogicalPlan, PlanTraversal, VeinSchema};

use crate::context::OptimizerContext;
use crate::rule::{OptimizerRule, RuleType};

use super::predicate_utils::{referenced_columns, substitute_columns};

/// A heuristic rule that moves deterministic predicates closer to their source.
pub struct PredicatePushdownRule;

impl OptimizerRule for PredicatePushdownRule {
    fn name(&self) -> &str {
        "PredicatePushdownRule"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Heuristic
    }

    fn apply(
        &self,
        plan: &LogicalPlan,
        _context: &OptimizerContext,
    ) -> Result<Option<LogicalPlan>> {
        let LogicalPlan::Filter { input, predicate } = plan else {
            return Ok(None);
        };
        if predicate.is_volatile() {
            return Ok(None);
        }

        let rewritten = match input.as_ref() {
            LogicalPlan::Project {
                input: project_input,
                exprs,
                schema,
            } => push_through_project(project_input, exprs, schema, predicate).map(|pushed| {
                LogicalPlan::Project {
                    input: Box::new(pushed),
                    exprs: exprs.clone(),
                    schema: schema.clone(),
                }
            }),
            LogicalPlan::Aggregate {
                input: aggregate_input,
                grouping_sets,
                aggs,
                schema,
            } => push_through_aggregate(aggregate_input, grouping_sets, aggs, schema, predicate)
                .map(|pushed| LogicalPlan::Aggregate {
                    input: Box::new(pushed),
                    grouping_sets: grouping_sets.clone(),
                    aggs: aggs.clone(),
                    schema: schema.clone(),
                }),
            LogicalPlan::Join {
                join_type: JoinType::Inner,
                left,
                right,
                on,
                using,
                schema,
            } => push_through_inner_join(left, right, on, using, schema, predicate),
            LogicalPlan::SetOp {
                kind,
                all,
                left,
                right,
                schema,
            } => push_through_set_op(kind.clone(), *all, left, right, schema, predicate),
            // A predicate above LIMIT cannot generally move below it:
            // Filter(Limit(input)) is not equivalent to Limit(Filter(input)).
            LogicalPlan::Limit { .. } => None,
            _ => None,
        };

        let Some(rewritten) = rewritten else {
            return Ok(None);
        };
        Ok((rewritten != *plan).then_some(rewritten))
    }
}

fn push_through_project(
    input: &LogicalPlan,
    exprs: &[LogicalExpr],
    schema: &VeinSchema,
    predicate: &LogicalExpr,
) -> Option<LogicalPlan> {
    if exprs.len() != schema.fields.len() {
        return None;
    }

    let mut mapping = HashMap::new();
    for (expr, field) in exprs.iter().zip(&schema.fields) {
        if expr.is_volatile() {
            continue;
        }
        mapping.insert(field.id, expr.clone());
    }

    let referenced = referenced_columns(predicate)?;
    if !referenced.iter().all(|column| mapping.contains_key(column)) {
        return None;
    }

    let substituted = substitute_columns(predicate, &mapping)?;
    Some(LogicalPlan::Filter {
        input: Box::new(input.clone()),
        predicate: substituted,
    })
}

fn push_through_aggregate(
    input: &LogicalPlan,
    grouping_sets: &[Vec<LogicalExpr>],
    _aggs: &[LogicalExpr],
    schema: &VeinSchema,
    predicate: &LogicalExpr,
) -> Option<LogicalPlan> {
    // A predicate below multiple grouping sets can change subtotal rows.
    // Restrict this rule to ordinary GROUP BY until grouping-set semantics are
    // modeled explicitly.
    if grouping_sets.len() != 1 {
        return None;
    }
    let grouping = grouping_sets.first()?;
    if grouping.is_empty() {
        return None;
    }

    let grouping_count = grouping.len();
    if schema.fields.len() < grouping_count {
        return None;
    }

    let referenced = referenced_columns(predicate)?;
    if referenced.is_empty() {
        return None;
    }

    let mut mapping = HashMap::new();
    for (expr, field) in grouping
        .iter()
        .zip(schema.fields.iter().take(grouping_count))
    {
        if expr.is_volatile() {
            return None;
        }
        mapping.insert(field.id, expr.clone());
    }

    if !referenced.iter().all(|column| mapping.contains_key(column)) {
        return None;
    }

    let substituted = substitute_columns(predicate, &mapping)?;
    Some(LogicalPlan::Filter {
        input: Box::new(input.clone()),
        predicate: substituted,
    })
}

fn push_through_inner_join(
    left: &LogicalPlan,
    right: &LogicalPlan,
    on: &Option<LogicalExpr>,
    using: &[plan::ResolvedColumnRef],
    schema: &VeinSchema,
    predicate: &LogicalExpr,
) -> Option<LogicalPlan> {
    let referenced = referenced_columns(predicate)?;
    let left_ids = schema_ids(left.schema());
    let right_ids = schema_ids(right.schema());

    if referenced.is_empty() {
        return None;
    }

    if referenced.is_subset(&left_ids) {
        return Some(LogicalPlan::Join {
            join_type: JoinType::Inner,
            left: Box::new(LogicalPlan::Filter {
                input: Box::new(left.clone()),
                predicate: predicate.clone(),
            }),
            right: Box::new(right.clone()),
            on: on.clone(),
            using: using.to_vec(),
            schema: Box::new(schema.clone()),
        });
    }

    if referenced.is_subset(&right_ids) {
        return Some(LogicalPlan::Join {
            join_type: JoinType::Inner,
            left: Box::new(left.clone()),
            right: Box::new(LogicalPlan::Filter {
                input: Box::new(right.clone()),
                predicate: predicate.clone(),
            }),
            on: on.clone(),
            using: using.to_vec(),
            schema: Box::new(schema.clone()),
        });
    }

    None
}

fn push_through_set_op(
    kind: SetOpKind,
    all: bool,
    left: &LogicalPlan,
    right: &LogicalPlan,
    schema: &VeinSchema,
    predicate: &LogicalExpr,
) -> Option<LogicalPlan> {
    let left_predicate = map_set_predicate(predicate, schema, left.schema())?;
    let right_predicate = map_set_predicate(predicate, schema, right.schema())?;

    Some(LogicalPlan::SetOp {
        kind,
        all,
        left: Box::new(LogicalPlan::Filter {
            input: Box::new(left.clone()),
            predicate: left_predicate,
        }),
        right: Box::new(LogicalPlan::Filter {
            input: Box::new(right.clone()),
            predicate: right_predicate,
        }),
        schema: Box::new(schema.clone()),
    })
}

fn map_set_predicate(
    predicate: &LogicalExpr,
    output: &VeinSchema,
    branch: &VeinSchema,
) -> Option<LogicalExpr> {
    if output.fields.len() != branch.fields.len() {
        return None;
    }

    let mapping = output
        .fields
        .iter()
        .zip(&branch.fields)
        .map(|(output, branch)| (output.id, LogicalExpr::Column(branch.clone())))
        .collect::<HashMap<_, _>>();
    substitute_columns(predicate, &mapping)
}

fn schema_ids(schema: &VeinSchema) -> HashSet<plan::ColumnId> {
    schema.fields.iter().map(|field| field.id).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use parser::ast::expr::BinaryOp;
    use plan::{ColumnId, LogicalLiteral, ResolvedColumn, ResolvedColumnRef, TableMetadata};

    fn field(id: usize, name: &str, data_type: plan::VeinDataType) -> ResolvedColumnRef {
        std::sync::Arc::new(ResolvedColumn::new(
            ColumnId(id),
            name,
            Some("t".to_string()),
            data_type,
        ))
    }

    fn scan(fields: Vec<ResolvedColumnRef>) -> LogicalPlan {
        let schema = std::sync::Arc::new(VeinSchema {
            fields: fields.clone(),
        });
        LogicalPlan::Scan {
            table: std::sync::Arc::new(TableMetadata::new("t", schema)),
            projection: None,
            filters: vec![],
            schema: Box::new(VeinSchema { fields }),
        }
    }

    fn column(field: &ResolvedColumnRef) -> LogicalExpr {
        LogicalExpr::Column(field.clone())
    }

    fn bool_predicate(field: &ResolvedColumnRef) -> LogicalExpr {
        LogicalExpr::BinaryOp {
            left: Box::new(column(field)),
            op: BinaryOp::Gt,
            right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(0))),
            return_type: plan::VeinDataType::Boolean,
        }
    }

    #[test]
    fn pushes_through_project_with_substitution() {
        let id = field(1, "id", plan::VeinDataType::Int64);
        let projected = field(10, "projected_id", plan::VeinDataType::Int64);
        let plan = LogicalPlan::Project {
            input: Box::new(scan(vec![id.clone()])),
            exprs: vec![column(&id)],
            schema: Box::new(VeinSchema {
                fields: vec![projected.clone()],
            }),
        };
        let predicate = bool_predicate(&projected);
        let pushed = push_through_project(
            match &plan {
                LogicalPlan::Project { input, .. } => input,
                _ => unreachable!(),
            },
            match &plan {
                LogicalPlan::Project { exprs, .. } => exprs,
                _ => unreachable!(),
            },
            match &plan {
                LogicalPlan::Project { schema, .. } => schema,
                _ => unreachable!(),
            },
            &predicate,
        )
        .expect("predicate should push");

        assert!(matches!(pushed, LogicalPlan::Filter { .. }));
    }

    #[test]
    fn does_not_push_through_limit() {
        let id = field(1, "id", plan::VeinDataType::Int64);
        let input = LogicalPlan::Limit {
            input: Box::new(scan(vec![id.clone()])),
            offset: 0,
            count: 1,
            schema: Box::new(VeinSchema {
                fields: vec![id.clone()],
            }),
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(input),
            predicate: bool_predicate(&id),
        };
        let rule = PredicatePushdownRule;
        assert!(
            rule.apply(&plan, &OptimizerContext::default())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn pushes_only_side_local_inner_join_predicates() {
        let left_id = field(1, "left_id", plan::VeinDataType::Int64);
        let right_id = field(2, "right_id", plan::VeinDataType::Int64);
        let left = scan(vec![left_id.clone()]);
        let right = scan(vec![right_id.clone()]);
        let schema = VeinSchema {
            fields: vec![left_id.clone(), right_id.clone()],
        };
        let join = LogicalPlan::Join {
            join_type: JoinType::Inner,
            left: Box::new(left),
            right: Box::new(right),
            on: None,
            using: vec![],
            schema: Box::new(schema.clone()),
        };
        let predicate = bool_predicate(&left_id);
        let rule = PredicatePushdownRule;
        let plan = LogicalPlan::Filter {
            input: Box::new(join),
            predicate,
        };
        let rewritten = rule
            .apply(&plan, &OptimizerContext::default())
            .unwrap()
            .expect("join predicate should push");
        let LogicalPlan::Join { left, .. } = rewritten else {
            panic!("expected an inner join");
        };
        assert!(matches!(left.as_ref(), LogicalPlan::Filter { .. }));
    }

    #[test]
    fn keeps_cross_side_inner_join_predicates_above_join() {
        let left_id = field(1, "left_id", plan::VeinDataType::Int64);
        let right_id = field(2, "right_id", plan::VeinDataType::Int64);
        let join = LogicalPlan::Join {
            join_type: JoinType::Inner,
            left: Box::new(scan(vec![left_id.clone()])),
            right: Box::new(scan(vec![right_id.clone()])),
            on: None,
            using: vec![],
            schema: Box::new(VeinSchema {
                fields: vec![left_id.clone(), right_id.clone()],
            }),
        };
        let predicate = LogicalExpr::BinaryOp {
            left: Box::new(column(&left_id)),
            op: BinaryOp::Eq,
            right: Box::new(column(&right_id)),
            return_type: plan::VeinDataType::Boolean,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(join),
            predicate,
        };
        assert!(
            PredicatePushdownRule
                .apply(&plan, &OptimizerContext::default())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn pushes_through_single_group_aggregate() {
        let input_id = field(1, "id", plan::VeinDataType::Int64);
        let group_id = field(2, "group_id", plan::VeinDataType::Int64);
        let count = field(3, "count", plan::VeinDataType::Int64);
        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(scan(vec![input_id.clone()])),
            grouping_sets: vec![vec![column(&input_id)]],
            aggs: vec![LogicalExpr::Literal(LogicalLiteral::Int64(1))],
            schema: Box::new(VeinSchema {
                fields: vec![group_id.clone(), count],
            }),
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(aggregate),
            predicate: bool_predicate(&group_id),
        };
        let rewritten = PredicatePushdownRule
            .apply(&plan, &OptimizerContext::default())
            .unwrap()
            .expect("grouping-key predicate should push");
        let LogicalPlan::Aggregate { input, .. } = rewritten else {
            panic!("expected an aggregate");
        };
        assert!(matches!(input.as_ref(), LogicalPlan::Filter { .. }));
    }

    #[test]
    fn does_not_push_through_multiple_grouping_sets() {
        let input_id = field(1, "id", plan::VeinDataType::Int64);
        let group_id = field(2, "group_id", plan::VeinDataType::Int64);
        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(scan(vec![input_id.clone()])),
            grouping_sets: vec![vec![column(&input_id)], vec![]],
            aggs: vec![],
            schema: Box::new(VeinSchema {
                fields: vec![group_id.clone()],
            }),
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(aggregate),
            predicate: bool_predicate(&group_id),
        };
        assert!(
            PredicatePushdownRule
                .apply(&plan, &OptimizerContext::default())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn pushes_through_all_set_operation_kinds_by_position() {
        for kind in [SetOpKind::Union, SetOpKind::Intersect, SetOpKind::Except] {
            let output = field(10, "value", plan::VeinDataType::Int64);
            let left_field = field(11, "left_value", plan::VeinDataType::Int64);
            let right_field = field(12, "right_value", plan::VeinDataType::Int64);
            let set_op = LogicalPlan::SetOp {
                kind,
                all: true,
                left: Box::new(scan(vec![left_field.clone()])),
                right: Box::new(scan(vec![right_field.clone()])),
                schema: Box::new(VeinSchema {
                    fields: vec![output.clone()],
                }),
            };
            let plan = LogicalPlan::Filter {
                input: Box::new(set_op),
                predicate: bool_predicate(&output),
            };
            let rewritten = PredicatePushdownRule
                .apply(&plan, &OptimizerContext::default())
                .unwrap()
                .expect("set-operation predicate should push");
            let LogicalPlan::SetOp { left, right, .. } = rewritten else {
                panic!("expected a set operation");
            };
            assert!(matches!(left.as_ref(), LogicalPlan::Filter { .. }));
            assert!(matches!(right.as_ref(), LogicalPlan::Filter { .. }));
        }
    }
}
