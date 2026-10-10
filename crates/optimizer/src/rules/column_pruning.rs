//! Top-down required-column analysis and scan projection pruning.

use std::collections::HashSet;

use error::Result;
use plan::{LogicalExpr, LogicalPlan, PlanTraversal, SortKey, VeinSchema};

use crate::context::OptimizerContext;
use crate::rule::{OptimizerRule, RuleType};

type RequiredColumns = HashSet<plan::ColumnId>;

/// A plan-scoped heuristic that removes columns not needed by the query.
pub struct ColumnPruningRule;

impl OptimizerRule for ColumnPruningRule {
    fn name(&self) -> &str {
        "ColumnPruningRule"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Heuristic
    }

    fn is_plan_rule(&self) -> bool {
        true
    }

    fn apply_plan(
        &self,
        plan: &LogicalPlan,
        _context: &OptimizerContext,
    ) -> Result<Option<LogicalPlan>> {
        let required = all_schema_columns(plan.schema());
        let rewritten = prune_plan(plan.clone(), &required);
        Ok((rewritten != *plan).then_some(rewritten))
    }

    fn apply(
        &self,
        _plan: &LogicalPlan,
        _context: &OptimizerContext,
    ) -> Result<Option<LogicalPlan>> {
        Ok(None)
    }
}

/// Propagates required output columns from a plan node to its children.
fn prune_plan(plan: LogicalPlan, required: &RequiredColumns) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan {
            table,
            projection,
            filters,
            schema,
        } => {
            let mut needed = required.clone();
            for filter in &filters {
                collect_columns(filter, &mut needed);
            }
            let ordered = ordered_schema_columns(&schema, &needed);
            let projection = if ordered.len() == schema.fields.len() && projection.is_none() {
                None
            } else {
                Some(ordered)
            };
            LogicalPlan::Scan {
                table,
                projection,
                filters,
                schema,
            }
        }
        LogicalPlan::ExternalScan {
            url,
            options,
            files,
            schema,
        } => LogicalPlan::ExternalScan {
            url,
            options,
            files,
            schema,
        },
        LogicalPlan::Filter { input, predicate } => {
            let mut child_required = required.clone();
            collect_columns(&predicate, &mut child_required);
            LogicalPlan::Filter {
                input: Box::new(prune_plan(*input, &child_required)),
                predicate,
            }
        }
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => {
            if exprs.len() != schema.fields.len() {
                let child_required = all_schema_columns(input.schema());
                let input = prune_plan(*input, &child_required);
                return LogicalPlan::Project {
                    input: Box::new(input),
                    exprs,
                    schema,
                };
            }

            let mut kept_exprs = Vec::new();
            let mut kept_fields = Vec::new();
            let mut child_required = RequiredColumns::new();
            for (expr, field) in exprs.into_iter().zip(schema.fields.iter()) {
                if required.contains(&field.id) {
                    collect_columns(&expr, &mut child_required);
                    kept_exprs.push(expr);
                    kept_fields.push(field.clone());
                }
            }

            LogicalPlan::Project {
                input: Box::new(prune_plan(*input, &child_required)),
                exprs: kept_exprs,
                schema: Box::new(VeinSchema {
                    fields: kept_fields,
                }),
            }
        }
        LogicalPlan::Aggregate {
            input,
            grouping_sets,
            aggs,
            schema,
        } => {
            let mut child_required = RequiredColumns::new();
            for grouping_set in &grouping_sets {
                for expr in grouping_set {
                    collect_columns(expr, &mut child_required);
                }
            }

            let grouping_count = grouping_sets.first().map_or(0, Vec::len);
            let mut kept_aggs = Vec::new();
            let mut output_fields = schema
                .fields
                .iter()
                .take(grouping_count)
                .cloned()
                .collect::<Vec<_>>();
            for (index, agg) in aggs.into_iter().enumerate() {
                let output_index = grouping_count + index;
                let keep = schema
                    .fields
                    .get(output_index)
                    .is_none_or(|field| required.contains(&field.id));
                if keep {
                    collect_columns(&agg, &mut child_required);
                    kept_aggs.push(agg);
                    if let Some(field) = schema.fields.get(output_index) {
                        output_fields.push(field.clone());
                    }
                }
            }

            LogicalPlan::Aggregate {
                input: Box::new(prune_plan(*input, &child_required)),
                grouping_sets,
                aggs: kept_aggs,
                schema: Box::new(VeinSchema {
                    fields: output_fields,
                }),
            }
        }
        LogicalPlan::Window {
            input,
            partition_by,
            order_by,
            frame,
            exprs,
            schema,
        } => {
            let mut child_required = required.clone();
            collect_exprs(&partition_by, &mut child_required);
            collect_sort_keys(&order_by, &mut child_required);
            for expr in &exprs {
                collect_columns(&expr.call, &mut child_required);
            }
            LogicalPlan::Window {
                input: Box::new(prune_plan(*input, &child_required)),
                partition_by,
                order_by,
                frame,
                exprs,
                schema,
            }
        }
        LogicalPlan::Join {
            join_type,
            left,
            right,
            on,
            using,
            schema,
        } => {
            let left_schema = left.schema().clone();
            let right_schema = right.schema().clone();
            let mut left_required = columns_in_schema(required, &left_schema);
            let mut right_required = columns_in_schema(required, &right_schema);

            if let Some(on) = &on {
                let mut join_columns = RequiredColumns::new();
                collect_columns(on, &mut join_columns);
                left_required.extend(columns_in_schema(&join_columns, &left_schema));
                right_required.extend(columns_in_schema(&join_columns, &right_schema));
            }
            for column in &using {
                if left_schema.fields.iter().any(|field| field.id == column.id) {
                    left_required.insert(column.id);
                }
                if right_schema
                    .fields
                    .iter()
                    .any(|field| field.id == column.id)
                {
                    right_required.insert(column.id);
                }
            }

            // Make the pruned output contract explicit. Join keys and residual
            // predicate inputs remain required from their children above, but
            // only parent-visible columns belong in this join's output schema.
            let schema = Box::new(VeinSchema {
                fields: schema
                    .fields
                    .iter()
                    .filter(|field| required.contains(&field.id))
                    .cloned()
                    .collect(),
            });

            LogicalPlan::Join {
                join_type,
                left: Box::new(prune_plan(*left, &left_required)),
                right: Box::new(prune_plan(*right, &right_required)),
                on,
                using,
                schema,
            }
        }
        LogicalPlan::Sort {
            input,
            sort_keys,
            schema,
        } => {
            let mut child_required = required.clone();
            collect_sort_keys(&sort_keys, &mut child_required);
            LogicalPlan::Sort {
                input: Box::new(prune_plan(*input, &child_required)),
                sort_keys,
                schema,
            }
        }
        LogicalPlan::Limit {
            input,
            offset,
            count,
            schema,
        } => LogicalPlan::Limit {
            input: Box::new(prune_plan(*input, required)),
            offset,
            count,
            schema,
        },
        LogicalPlan::Distinct { input, schema } => {
            let child_required = all_schema_columns(input.schema());
            LogicalPlan::Distinct {
                input: Box::new(prune_plan(*input, &child_required)),
                schema,
            }
        }
        LogicalPlan::SubqueryAlias {
            input,
            alias,
            schema,
        } => LogicalPlan::SubqueryAlias {
            input: Box::new(prune_plan(*input, required)),
            alias,
            schema,
        },
        LogicalPlan::Explain { input, schema } => LogicalPlan::Explain {
            input: Box::new(prune_plan(*input, required)),
            schema,
        },
        LogicalPlan::SetOp {
            kind,
            all,
            left,
            right,
            schema,
        } => {
            // Set operations require both branches to retain matching arity
            // and positional types. Prune within each branch, but preserve
            // every branch output column at this boundary.
            let left_required = all_schema_columns(left.schema());
            let right_required = all_schema_columns(right.schema());
            LogicalPlan::SetOp {
                kind,
                all,
                left: Box::new(prune_plan(*left, &left_required)),
                right: Box::new(prune_plan(*right, &right_required)),
                schema,
            }
        }
        LogicalPlan::Sample {
            input,
            method,
            percent,
            schema,
        } => LogicalPlan::Sample {
            input: Box::new(prune_plan(*input, required)),
            method,
            percent,
            schema,
        },
        LogicalPlan::Assert {
            input,
            condition,
            message,
            schema,
        } => {
            let mut child_required = required.clone();
            collect_columns(&condition, &mut child_required);
            LogicalPlan::Assert {
                input: Box::new(prune_plan(*input, &child_required)),
                condition,
                message,
                schema,
            }
        }
        LogicalPlan::Pivot {
            input,
            aggregate,
            alias,
            value_column,
            values,
            schema,
        } => {
            let mut child_required = required.clone();
            collect_columns(&aggregate, &mut child_required);
            collect_exprs(&values, &mut child_required);
            child_required.insert(value_column.id);
            LogicalPlan::Pivot {
                input: Box::new(prune_plan(*input, &child_required)),
                aggregate,
                alias,
                value_column,
                values,
                schema,
            }
        }
        LogicalPlan::Unpivot {
            input,
            value_name,
            name_name,
            columns,
            include_nulls,
            schema,
        } => {
            let mut child_required = required.clone();
            child_required.extend(columns.iter().map(|column| column.id));
            LogicalPlan::Unpivot {
                input: Box::new(prune_plan(*input, &child_required)),
                value_name,
                name_name,
                columns,
                include_nulls,
                schema,
            }
        }
        LogicalPlan::Fork { branches, schema } => LogicalPlan::Fork {
            branches: branches
                .into_iter()
                .map(|branch| Box::new(prune_plan(*branch, required)))
                .collect(),
            schema,
        },
        LogicalPlan::Tee { branches, schema } => LogicalPlan::Tee {
            branches: branches
                .into_iter()
                .map(|branch| Box::new(prune_plan(*branch, required)))
                .collect(),
            schema,
        },
        LogicalPlan::SemanticFilter {
            input,
            predicate,
            targets,
            alpha,
            schema,
        } => {
            let mut child_required = all_schema_columns(input.schema());
            child_required.extend(targets.iter().map(|column| column.id));
            LogicalPlan::SemanticFilter {
                input: Box::new(prune_plan(*input, &child_required)),
                predicate,
                targets,
                alpha,
                schema,
            }
        }
        LogicalPlan::SemanticJoin {
            left,
            right,
            condition,
            bounding_equi,
            depends_on,
            alpha,
            schema,
        } => {
            let left_required = all_schema_columns(left.schema());
            let right_required = all_schema_columns(right.schema());
            LogicalPlan::SemanticJoin {
                left: Box::new(prune_plan(*left, &left_required)),
                right: Box::new(prune_plan(*right, &right_required)),
                condition,
                bounding_equi,
                depends_on,
                alpha,
                schema,
            }
        }
        LogicalPlan::SemanticCalibrate {
            input,
            target_features,
            ground_truth_label,
            alpha,
            schema,
        } => {
            let child_required = all_schema_columns(input.schema());
            LogicalPlan::SemanticCalibrate {
                input: Box::new(prune_plan(*input, &child_required)),
                target_features,
                ground_truth_label,
                alpha,
                schema,
            }
        }
        LogicalPlan::Write {
            input,
            target,
            options,
            schema,
        } => {
            let child_required = all_schema_columns(input.schema());
            LogicalPlan::Write {
                input: Box::new(prune_plan(*input, &child_required)),
                target,
                options,
                schema,
            }
        }
        LogicalPlan::Call {
            input,
            name,
            args,
            schema,
        } => {
            let mut child_required = all_schema_columns(input.schema());
            collect_exprs(&args, &mut child_required);
            LogicalPlan::Call {
                input: Box::new(prune_plan(*input, &child_required)),
                name,
                args,
                schema,
            }
        }
    }
}

fn all_schema_columns(schema: &VeinSchema) -> RequiredColumns {
    schema.fields.iter().map(|field| field.id).collect()
}

fn ordered_schema_columns(schema: &VeinSchema, required: &RequiredColumns) -> Vec<plan::ColumnId> {
    schema
        .fields
        .iter()
        .filter(|field| required.contains(&field.id))
        .map(|field| field.id)
        .collect()
}

fn columns_in_schema(required: &RequiredColumns, schema: &VeinSchema) -> RequiredColumns {
    schema
        .fields
        .iter()
        .filter(|field| required.contains(&field.id))
        .map(|field| field.id)
        .collect()
}

fn collect_exprs(exprs: &[LogicalExpr], output: &mut RequiredColumns) {
    for expr in exprs {
        collect_columns(expr, output);
    }
}

fn collect_sort_keys(keys: &[SortKey], output: &mut RequiredColumns) {
    for key in keys {
        collect_columns(&key.expr, output);
    }
}

fn collect_columns(expr: &LogicalExpr, output: &mut RequiredColumns) {
    match expr {
        LogicalExpr::Column(column) | LogicalExpr::OuterColumn(column) => {
            output.insert(column.id);
        }
        _ => {
            for child in expr.children() {
                collect_columns(child, output);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plan::{ColumnId, ResolvedColumn, ResolvedColumnRef, TableMetadata, VeinDataType};
    type Arc<T> = std::sync::Arc<T>;

    fn field(id: usize, name: &str) -> ResolvedColumnRef {
        std::sync::Arc::new(ResolvedColumn::new(
            ColumnId(id),
            name,
            Some("t".to_string()),
            VeinDataType::Int64,
        ))
    }

    fn scan() -> LogicalPlan {
        let fields = vec![field(1, "id"), field(2, "unused")];
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

    fn column(id: usize, name: &str) -> LogicalExpr {
        LogicalExpr::Column(field(id, name))
    }

    #[test]
    fn prunes_scan_to_projected_columns() {
        let schema = Box::new(VeinSchema {
            fields: vec![field(1, "id")],
        });
        let plan = LogicalPlan::Project {
            input: Box::new(scan()),
            exprs: vec![column(1, "id")],
            schema,
        };
        let rewritten = prune_plan(
            plan,
            &all_schema_columns(&VeinSchema {
                fields: vec![field(1, "id")],
            }),
        );
        let LogicalPlan::Project { input, .. } = rewritten else {
            panic!("expected project");
        };
        let LogicalPlan::Scan { projection, .. } = *input else {
            panic!("expected scan");
        };
        assert_eq!(projection, Some(vec![ColumnId(1)]));
    }

    #[test]
    fn prunes_join_outputs_while_retaining_columns_needed_for_join_keys() {
        let left_value = field(1, "left_value");
        let left_key = field(2, "left_key");
        let right_key = field(3, "right_key");
        let right_value = field(4, "right_value");
        let scan = |name: &str, fields: Vec<ResolvedColumnRef>| {
            let schema = Box::new(VeinSchema { fields });
            LogicalPlan::Scan {
                table: std::sync::Arc::new(TableMetadata::new(
                    name,
                    std::sync::Arc::new(*schema.clone()),
                )),
                projection: None,
                filters: Vec::new(),
                schema,
            }
        };
        let join = LogicalPlan::Join {
            join_type: parser::ast::pipe_op::JoinType::Inner,
            left: Box::new(scan(
                "left",
                vec![Arc::clone(&left_value), Arc::clone(&left_key)],
            )),
            right: Box::new(scan(
                "right",
                vec![Arc::clone(&right_key), Arc::clone(&right_value)],
            )),
            on: Some(LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(Arc::clone(&left_key))),
                op: parser::ast::expr::BinaryOp::Eq,
                right: Box::new(LogicalExpr::Column(Arc::clone(&right_key))),
                return_type: VeinDataType::Boolean,
            }),
            using: Vec::new(),
            schema: Box::new(VeinSchema::new(vec![
                Arc::clone(&left_value),
                Arc::clone(&right_value),
                Arc::clone(&left_key),
                Arc::clone(&right_key),
            ])),
        };
        let plan = LogicalPlan::Project {
            input: Box::new(join),
            exprs: vec![LogicalExpr::Column(Arc::clone(&left_value))],
            schema: Box::new(VeinSchema::new(vec![field(10, "result")])),
        };

        let pruned = prune_plan(plan, &[ColumnId(10)].into_iter().collect());

        let LogicalPlan::Project { input, .. } = pruned else {
            panic!("expected output projection");
        };
        let LogicalPlan::Join {
            left,
            right,
            schema,
            ..
        } = *input
        else {
            panic!("expected join under output projection");
        };
        assert_eq!(
            schema.fields.iter().map(|field| field.id).collect::<Vec<_>>(),
            vec![left_value.id],
            "join schema should explicitly expose only parent-required outputs"
        );

        let LogicalPlan::Scan { projection: left_projection, .. } = *left else {
            panic!("expected left scan");
        };
        let LogicalPlan::Scan { projection: right_projection, .. } = *right else {
            panic!("expected right scan");
        };
        assert!(
            left_projection.is_none(),
            "all left scan columns are required, so an explicit projection is unnecessary"
        );
        assert_eq!(right_projection, Some(vec![right_key.id]));
    }

    #[test]
    fn filter_dependencies_survive_projection_pruning_across_join_inputs() {
        let column = |id, name: &str, qualifier: &str| {
            std::sync::Arc::new(ResolvedColumn::new(
                ColumnId(id),
                name,
                Some(qualifier.to_string()),
                VeinDataType::Int64,
            ))
        };
        let scan = |table: &str, fields: Vec<ResolvedColumnRef>| {
            let schema = Box::new(VeinSchema { fields });
            LogicalPlan::Scan {
                table: std::sync::Arc::new(TableMetadata::new(
                    table,
                    std::sync::Arc::new(*schema.clone()),
                )),
                projection: None,
                filters: vec![],
                schema,
            }
        };

        let part_key = column(10, "p_partkey", "part");
        let supply_part_key = column(11, "ps_partkey", "partsupp");
        let supply_cost = column(12, "ps_supplycost", "partsupp");
        let supplier_key = column(13, "ps_suppkey", "partsupp");
        let supplier_supp_key = column(14, "s_suppkey", "supplier");
        let supplier_name = column(15, "s_name", "supplier");
        let min_part_key = column(16, "group_partkey", "partsupp_min");
        let min_cost = column(17, "min_supplycost", "partsupp_min");

        let part_scan = scan("part", vec![Arc::clone(&part_key)]);
        let partsupp_scan = scan(
            "partsupp",
            vec![
                Arc::clone(&supply_part_key),
                Arc::clone(&supply_cost),
                Arc::clone(&supplier_key),
            ],
        );
        let first_join_schema = Box::new(VeinSchema::new(vec![
            Arc::clone(&part_key),
            Arc::clone(&supply_part_key),
            Arc::clone(&supply_cost),
            Arc::clone(&supplier_key),
        ]));
        let first_join = LogicalPlan::Join {
            join_type: parser::ast::pipe_op::JoinType::Inner,
            left: Box::new(part_scan),
            right: Box::new(partsupp_scan),
            on: Some(LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(Arc::clone(&part_key))),
                op: parser::ast::expr::BinaryOp::Eq,
                right: Box::new(LogicalExpr::Column(Arc::clone(&supply_part_key))),
                return_type: VeinDataType::Boolean,
            }),
            using: vec![],
            schema: first_join_schema,
        };

        let supplier_scan = scan(
            "supplier",
            vec![Arc::clone(&supplier_supp_key), Arc::clone(&supplier_name)],
        );
        let chained_schema = Box::new(VeinSchema::new(vec![
            Arc::clone(&part_key),
            Arc::clone(&supply_part_key),
            Arc::clone(&supply_cost),
            Arc::clone(&supplier_key),
            Arc::clone(&supplier_supp_key),
            Arc::clone(&supplier_name),
        ]));
        let chained_join = LogicalPlan::Join {
            join_type: parser::ast::pipe_op::JoinType::Inner,
            left: Box::new(first_join),
            right: Box::new(supplier_scan),
            on: Some(LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(Arc::clone(&supplier_key))),
                op: parser::ast::expr::BinaryOp::Eq,
                right: Box::new(LogicalExpr::Column(Arc::clone(&supplier_supp_key))),
                return_type: VeinDataType::Boolean,
            }),
            using: vec![],
            schema: chained_schema,
        };

        let min_scan = scan(
            "partsupp_min",
            vec![Arc::clone(&min_part_key), Arc::clone(&min_cost)],
        );
        let correlated_join_schema = Box::new(VeinSchema::new(vec![
            Arc::clone(&part_key),
            Arc::clone(&supply_part_key),
            Arc::clone(&supply_cost),
            Arc::clone(&supplier_key),
            Arc::clone(&supplier_supp_key),
            Arc::clone(&supplier_name),
            Arc::clone(&min_part_key),
            Arc::clone(&min_cost),
        ]));
        let correlated_join = LogicalPlan::Join {
            join_type: parser::ast::pipe_op::JoinType::Left,
            left: Box::new(chained_join),
            right: Box::new(min_scan),
            on: Some(LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(Arc::clone(&part_key))),
                op: parser::ast::expr::BinaryOp::Eq,
                right: Box::new(LogicalExpr::Column(Arc::clone(&min_part_key))),
                return_type: VeinDataType::Boolean,
            }),
            using: vec![],
            schema: correlated_join_schema,
        };
        let filtered = LogicalPlan::Filter {
            input: Box::new(correlated_join),
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(Arc::clone(&supply_cost))),
                op: parser::ast::expr::BinaryOp::Eq,
                right: Box::new(LogicalExpr::Column(Arc::clone(&min_cost))),
                return_type: VeinDataType::Boolean,
            },
        };
        let output = column(18, "s_name", "result");
        let plan = LogicalPlan::Project {
            input: Box::new(filtered),
            exprs: vec![LogicalExpr::Column(Arc::clone(&supplier_name))],
            schema: Box::new(VeinSchema::new(vec![output])),
        };

        let pruned = prune_plan(plan, &[ColumnId(18)].into_iter().collect());
        let LogicalPlan::Project { input, .. } = pruned else {
            panic!("expected output projection");
        };
        let LogicalPlan::Filter { input, .. } = *input else {
            panic!("filter must stay below the output projection");
        };
        let LogicalPlan::Join { left, .. } = *input else {
            panic!("expected decorrelated aggregate join");
        };
        let LogicalPlan::Join { left, .. } = *left else {
            panic!("expected chained supplier join");
        };
        let LogicalPlan::Join { right, .. } = *left else {
            panic!("expected part-to-partsupp join");
        };
        let LogicalPlan::Scan { projection, .. } = *right else {
            panic!("expected partsupp scan");
        };
        assert!(
            projection
                .as_ref()
                .is_none_or(|columns| columns.contains(&ColumnId(12))),
            "filter-only ps_supplycost must survive scan pruning: {projection:?}"
        );
    }
}
