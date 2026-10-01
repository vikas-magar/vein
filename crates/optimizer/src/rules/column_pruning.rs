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
}
