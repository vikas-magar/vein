use super::logical::LogicalPlan;
use crate::expr::LogicalExpr;
use crate::schema::VeinSchema;
use error::VeinError;

/// Defines traversal methods for logical plans
pub trait PlanTraversal {
    /// Returns the schema of the plan node
    fn schema(&self) -> &VeinSchema;
    /// Visits every expression stored directly on this node and identifies
    /// the field that owns it. Keep the implementation exhaustive so new
    /// expression-bearing operators join the optimizer's support inventory.
    fn for_each_expression<'a>(
        &'a self,
        visitor: &mut dyn FnMut(&'static str, &'a LogicalExpr),
    );
    /// Returns a list of child plans for traversal
    fn children(&self) -> Vec<&LogicalPlan>;
    /// Maps children using a mutable function, consuming the node.
    ///
    /// Taking `self` by value is what keeps recursion clone-free: a rewriter
    /// that owns each node moves it into the mapper instead of deep-cloning the
    /// whole subtree at every level.
    fn map_children<F>(self, f: &mut F) -> Result<LogicalPlan, VeinError>
    where
        F: FnMut(Box<LogicalPlan>) -> Result<Box<LogicalPlan>, VeinError>;
    /// Checks if the plan contains semantic nodes (which blocks relational sharing rules)
    fn contains_semantic(&self) -> bool;
    /// Collects the `LogicalPlan`s embedded in subquery expressions
    /// (`ScalarSubquery`/`Exists`) carried directly by this plan node.
    ///
    /// Subquery bodies are plans, not expressions, so they are opaque to
    /// [`children`](PlanTraversal::children). This is the narrow accessor used
    /// to descend into them without widening the generic traversal contract.
    fn subquery_plans(&self) -> Vec<&LogicalPlan>;
}

impl PlanTraversal for LogicalPlan {
    fn for_each_expression<'a>(
        &'a self,
        visitor: &mut dyn FnMut(&'static str, &'a LogicalExpr),
    ) {
        use crate::plan::types::SortKey;

        fn expressions<'a>(
            exprs: &'a [LogicalExpr],
            owner: &'static str,
            visitor: &mut dyn FnMut(&'static str, &'a LogicalExpr),
        ) {
            for expr in exprs {
                visitor(owner, expr);
            }
        }

        fn sort_keys<'a>(
            keys: &'a [SortKey],
            owner: &'static str,
            visitor: &mut dyn FnMut(&'static str, &'a LogicalExpr),
        ) {
            for key in keys {
                visitor(owner, &key.expr);
            }
        }

        match self {
            LogicalPlan::Scan { filters, .. } => expressions(filters, "Scan.filters", visitor),
            LogicalPlan::Filter { predicate, .. } => visitor("Filter.predicate", predicate),
            LogicalPlan::Project { exprs, .. } => expressions(exprs, "Project.exprs", visitor),
            LogicalPlan::Aggregate { grouping_sets, aggs, .. } => {
                for grouping_set in grouping_sets {
                    expressions(grouping_set, "Aggregate.grouping_sets", visitor);
                }
                expressions(aggs, "Aggregate.aggs", visitor);
            }
            LogicalPlan::Window { partition_by, order_by, exprs, .. } => {
                expressions(partition_by, "Window.partition_by", visitor);
                sort_keys(order_by, "Window.order_by", visitor);
                for expr in exprs {
                    visitor("Window.exprs.call", &expr.call);
                }
            }
            LogicalPlan::Join { on: Some(on), .. } => visitor("Join.on", on),
            LogicalPlan::Sort { sort_keys: keys, .. } => sort_keys(keys, "Sort.sort_keys", visitor),
            LogicalPlan::Assert { condition, .. } => visitor("Assert.condition", condition),
            LogicalPlan::Pivot { aggregate, values, .. } => {
                visitor("Pivot.aggregate", aggregate);
                expressions(values, "Pivot.values", visitor);
            }
            LogicalPlan::Call { args, .. } => expressions(args, "Call.args", visitor),
            LogicalPlan::ExternalScan { .. }
            | LogicalPlan::Intermediate { .. }
            | LogicalPlan::Join { on: None, .. }
            | LogicalPlan::Limit { .. }
            | LogicalPlan::Distinct { .. }
            | LogicalPlan::SubqueryAlias { .. }
            | LogicalPlan::Explain { .. }
            | LogicalPlan::SetOp { .. }
            | LogicalPlan::Sample { .. }
            | LogicalPlan::Unpivot { .. }
            | LogicalPlan::Fork { .. }
            | LogicalPlan::Tee { .. }
            | LogicalPlan::SemanticFilter { .. }
            | LogicalPlan::SemanticJoin { .. }
            | LogicalPlan::SemanticCalibrate { .. }
            | LogicalPlan::Write { .. } => {}
        }
    }

    fn schema(&self) -> &VeinSchema {
        match self {
            LogicalPlan::Scan { schema, .. }
            | LogicalPlan::ExternalScan { schema, .. }
            | LogicalPlan::Intermediate { schema, .. }
            | LogicalPlan::Project { schema, .. }
            | LogicalPlan::Aggregate { schema, .. }
            | LogicalPlan::Window { schema, .. }
            | LogicalPlan::Sort { schema, .. }
            | LogicalPlan::Limit { schema, .. }
            | LogicalPlan::Distinct { schema, .. }
            | LogicalPlan::SubqueryAlias { schema, .. }
            | LogicalPlan::Explain { schema, .. }
            | LogicalPlan::SetOp { schema, .. }
            | LogicalPlan::Join { schema, .. }
            | LogicalPlan::Sample { schema, .. }
            | LogicalPlan::Assert { schema, .. }
            | LogicalPlan::Pivot { schema, .. }
            | LogicalPlan::Unpivot { schema, .. }
            | LogicalPlan::Fork { schema, .. }
            | LogicalPlan::Tee { schema, .. }
            | LogicalPlan::SemanticFilter { schema, .. }
            | LogicalPlan::SemanticJoin { schema, .. }
            | LogicalPlan::SemanticCalibrate { schema, .. }
            | LogicalPlan::Write { schema, .. }
            | LogicalPlan::Call { schema, .. } => schema,
            LogicalPlan::Filter { input, .. } => input.schema(),
        }
    }

    fn children(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::Scan { .. } => vec![],
            LogicalPlan::ExternalScan { .. } => vec![],
            LogicalPlan::Intermediate { .. } => vec![],
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Window { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Distinct { input, .. }
            | LogicalPlan::SubqueryAlias { input, .. }
            | LogicalPlan::Explain { input, .. }
            | LogicalPlan::Sample { input, .. }
            | LogicalPlan::Assert { input, .. }
            | LogicalPlan::Pivot { input, .. }
            | LogicalPlan::Unpivot { input, .. }
            | LogicalPlan::SemanticFilter { input, .. }
            | LogicalPlan::SemanticCalibrate { input, .. }
            | LogicalPlan::Write { input, .. }
            | LogicalPlan::Call { input, .. } => vec![input.as_ref()],
            LogicalPlan::SetOp { left, right, .. }
            | LogicalPlan::Join { left, right, .. }
            | LogicalPlan::SemanticJoin { left, right, .. } => vec![left.as_ref(), right.as_ref()],
            LogicalPlan::Fork { branches, .. } | LogicalPlan::Tee { branches, .. } => {
                branches.iter().map(|b| b.as_ref()).collect()
            }
        }
    }

    fn map_children<F>(self, f: &mut F) -> Result<LogicalPlan, VeinError>
    where
        F: FnMut(Box<LogicalPlan>) -> Result<Box<LogicalPlan>, VeinError>,
    {
        match self {
            node @ (LogicalPlan::Scan { .. } | LogicalPlan::ExternalScan { .. }
            | LogicalPlan::Intermediate { .. }) => Ok(node),
            LogicalPlan::Filter { input, predicate } => Ok(LogicalPlan::Filter {
                input: f(input)?,
                predicate,
            }),
            LogicalPlan::Project {
                input,
                exprs,
                schema,
            } => Ok(LogicalPlan::Project {
                input: f(input)?,
                exprs,
                schema,
            }),

            LogicalPlan::Aggregate {
                input,
                grouping_sets,
                aggs,
                schema,
            } => Ok(LogicalPlan::Aggregate {
                input: f(input)?,
                grouping_sets,
                aggs,
                schema,
            }),
            LogicalPlan::Window {
                input,
                partition_by,
                order_by,
                frame,
                exprs,
                schema,
            } => Ok(LogicalPlan::Window {
                input: f(input)?,
                partition_by,
                order_by,
                frame,
                exprs,
                schema,
            }),
            LogicalPlan::Sort {
                input,
                sort_keys,
                schema,
            } => Ok(LogicalPlan::Sort {
                input: f(input)?,
                sort_keys,
                schema,
            }),
            LogicalPlan::Limit {
                input,
                offset,
                count,
                schema,
            } => Ok(LogicalPlan::Limit {
                input: f(input)?,
                offset,
                count,
                schema,
            }),
            LogicalPlan::Distinct { input, schema } => Ok(LogicalPlan::Distinct {
                input: f(input)?,
                schema,
            }),

            LogicalPlan::SubqueryAlias {
                input,
                alias,
                schema,
            } => Ok(LogicalPlan::SubqueryAlias {
                input: f(input)?,
                alias,
                schema,
            }),
            LogicalPlan::Explain { input, schema } => Ok(LogicalPlan::Explain {
                input: f(input)?,
                schema,
            }),
            LogicalPlan::SetOp {
                kind,
                all,
                left,
                right,
                schema,
            } => Ok(LogicalPlan::SetOp {
                kind,
                all,
                left: f(left)?,
                right: f(right)?,
                schema,
            }),
            LogicalPlan::Join {
                join_type,
                left,
                right,
                on,
                using,
                schema,
            } => Ok(LogicalPlan::Join {
                join_type,
                left: f(left)?,
                right: f(right)?,
                on,
                using,
                schema,
            }),
            LogicalPlan::Sample {
                input,
                method,
                percent,
                schema,
            } => Ok(LogicalPlan::Sample {
                input: f(input)?,
                method,
                percent,
                schema,
            }),
            LogicalPlan::Assert {
                input,
                condition,
                message,
                schema,
            } => Ok(LogicalPlan::Assert {
                input: f(input)?,
                condition,
                message,
                schema,
            }),
            LogicalPlan::Pivot {
                input,
                aggregate,
                alias,
                value_column,
                values,
                schema,
            } => Ok(LogicalPlan::Pivot {
                input: f(input)?,
                aggregate,
                alias,
                value_column,
                values,
                schema,
            }),
            LogicalPlan::Unpivot {
                input,
                value_name,
                name_name,
                columns,
                include_nulls,
                schema,
            } => Ok(LogicalPlan::Unpivot {
                input: f(input)?,
                value_name,
                name_name,
                columns,
                include_nulls,
                schema,
            }),
            LogicalPlan::Fork { branches, schema } => {
                let mut new_branches = Vec::with_capacity(branches.len());
                for b in branches {
                    new_branches.push(f(b)?);
                }
                Ok(LogicalPlan::Fork {
                    branches: new_branches,
                    schema,
                })
            }
            LogicalPlan::Tee { branches, schema } => {
                let mut new_branches = Vec::with_capacity(branches.len());
                for b in branches {
                    new_branches.push(f(b)?);
                }
                Ok(LogicalPlan::Tee {
                    branches: new_branches,
                    schema,
                })
            }
            LogicalPlan::SemanticFilter {
                input,
                predicate,
                targets,
                alpha,
                schema,
            } => Ok(LogicalPlan::SemanticFilter {
                input: f(input)?,
                predicate,
                targets,
                alpha,
                schema,
            }),
            LogicalPlan::SemanticJoin {
                left,
                right,
                condition,
                bounding_equi,
                depends_on,
                alpha,
                schema,
            } => Ok(LogicalPlan::SemanticJoin {
                left: f(left)?,
                right: f(right)?,
                condition,
                bounding_equi,
                depends_on,
                alpha,
                schema,
            }),
            LogicalPlan::SemanticCalibrate {
                input,
                target_features,
                ground_truth_label,
                alpha,
                schema,
            } => Ok(LogicalPlan::SemanticCalibrate {
                input: f(input)?,
                target_features,
                ground_truth_label,
                alpha,
                schema,
            }),
            LogicalPlan::Write {
                input,
                target,
                options,
                schema,
            } => Ok(LogicalPlan::Write {
                input: f(input)?,
                target,
                options,
                schema,
            }),
            LogicalPlan::Call {
                input,
                name,
                args,
                schema,
            } => Ok(LogicalPlan::Call {
                input: f(input)?,
                name,
                args,
                schema,
            }),
        }
    }

    fn contains_semantic(&self) -> bool {
        match self {
            LogicalPlan::SemanticFilter { .. }
            | LogicalPlan::SemanticJoin { .. }
            | LogicalPlan::SemanticCalibrate { .. } => true,
            _ => {
                self.children().iter().any(|c| c.contains_semantic())
                    || self
                    .subquery_plans()
                    .iter()
                    .any(|p| p.contains_semantic())
            }
        }
    }

    fn subquery_plans(&self) -> Vec<&LogicalPlan> {
        let mut subqueries = Vec::new();
        self.for_each_expression(&mut |_, expression| {
            subqueries.extend(expression.subquery_plans());
        });
        subqueries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::logical::LogicalExpr;
    use crate::expr::LogicalLiteral;
    use crate::schema::{ColumnId, ResolvedColumn, ResolvedColumnRef, VeinSchema};
    use crate::VeinDataType;
    use parser::ast::expr::BinaryOp;

    fn col(id: u64, name: &str) -> ResolvedColumnRef {
        std::sync::Arc::new(ResolvedColumn {
            id: ColumnId(id as usize),
            name: name.to_string(),
            qualifier: Some("users".to_string()),
            data_type: VeinDataType::Int64,
            is_factorized: false,
            nullable: false,
        })
    }

    fn scan_plan() -> LogicalPlan {
        let schema = std::sync::Arc::new(VeinSchema {
            fields: vec![col(0, "id"), col(1, "name")],
        });
        LogicalPlan::Scan {
            table: std::sync::Arc::new(crate::TableMetadata::new("users", schema.clone())),
            projection: None,
            filters: vec![],
            schema: Box::new((*schema).clone()),
        }
    }

    /// A plan node known to be "semantic" (blocks relational sharing rules).
    fn semantic_filter() -> LogicalPlan {
        LogicalPlan::SemanticFilter {
            input: Box::new(scan_plan()),
            predicate: "is a returning customer".to_string(),
            targets: vec![col(1, "name")],
            alpha: 0.1,
            schema: Box::new(VeinSchema { fields: vec![] }),
        }
    }

    fn scalar_subquery(plan: LogicalPlan) -> LogicalExpr {
        LogicalExpr::ScalarSubquery(Box::new(plan))
    }

    #[test]
    fn expr_subquery_plans_finds_nested_subqueries() {
        // A subquery buried several levels deep inside a binary expression.
        let e = LogicalExpr::BinaryOp {
            left: Box::new(scalar_subquery(scan_plan())),
            op: BinaryOp::Eq,
            right: Box::new(LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(1))),
                op: BinaryOp::And,
                right: Box::new(scalar_subquery(scan_plan())),
                return_type: VeinDataType::Boolean,
            }),
            return_type: VeinDataType::Boolean,
        };
        assert_eq!(e.subquery_plans().len(), 2);
    }

    #[test]
    fn expr_subquery_plans_is_empty_without_subqueries() {
        let e = LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(1))),
            op: BinaryOp::Plus,
            right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(2))),
            return_type: VeinDataType::Int64,
        };
        assert!(e.subquery_plans().is_empty());
    }

    #[test]
    fn plan_subquery_plans_extracts_embedded_subqueries() {
        // Filter whose predicate wraps a scalar subquery.
        let sub = scalar_subquery(semantic_filter());
        let filter = LogicalPlan::Filter {
            input: Box::new(scan_plan()),
            predicate: sub.clone(),
        };
        assert_eq!(filter.subquery_plans().len(), 1);
        assert!(matches!(
            filter.subquery_plans()[0],
            LogicalPlan::SemanticFilter { .. }
        ));
    }

    #[test]
    fn expression_inventory_covers_every_expression_owner() {
        use crate::plan::logical::WindowExpr;
        use crate::plan::types::SortKey;

        let scalar = || scalar_subquery(scan_plan());
        let base = scan_plan();
        let schema = Box::new(base.schema().clone());
        let LogicalPlan::Scan { table, .. } = base else {
            unreachable!();
        };

        let scan = LogicalPlan::Scan {
            table: table.clone(),
            projection: None,
            filters: vec![scalar()],
            schema: schema.clone(),
        };
        let input = || Box::new(scan_plan());
        let plans = [
            scan,
            LogicalPlan::Filter {
                input: input(),
                predicate: scalar(),
            },
            LogicalPlan::Project {
                input: input(),
                exprs: vec![scalar()],
                schema: schema.clone(),
            },
            LogicalPlan::Aggregate {
                input: input(),
                grouping_sets: vec![vec![scalar()]],
                aggs: vec![scalar()],
                schema: schema.clone(),
            },
            LogicalPlan::Window {
                input: input(),
                partition_by: vec![scalar()],
                order_by: vec![SortKey {
                    expr: scalar(),
                    asc: true,
                    nulls_first: false,
                }],
                frame: None,
                exprs: vec![WindowExpr {
                    call: scalar(),
                    output_column: col(2, "window_value"),
                }],
                schema: schema.clone(),
            },
            LogicalPlan::Join {
                join_type: parser::ast::pipe_op::JoinType::Inner,
                left: input(),
                right: input(),
                on: Some(scalar()),
                using: vec![],
                schema: schema.clone(),
            },
            LogicalPlan::Sort {
                input: input(),
                sort_keys: vec![SortKey {
                    expr: scalar(),
                    asc: true,
                    nulls_first: false,
                }],
                schema: schema.clone(),
            },
            LogicalPlan::Assert {
                input: input(),
                condition: scalar(),
                message: None,
                schema: schema.clone(),
            },
            LogicalPlan::Pivot {
                input: input(),
                aggregate: scalar(),
                alias: None,
                value_column: col(1, "value"),
                values: vec![scalar()],
                schema: schema.clone(),
            },
            LogicalPlan::Call {
                input: input(),
                name: "fixture".to_string(),
                args: vec![scalar()],
                schema,
            },
        ];

        let mut owners = Vec::new();
        for plan in &plans {
            plan.for_each_expression(&mut |owner, expression| {
                if !expression.subquery_plans().is_empty() {
                    owners.push(owner);
                }
            });
        }

        assert_eq!(
            owners,
            [
                "Scan.filters",
                "Filter.predicate",
                "Project.exprs",
                "Aggregate.grouping_sets",
                "Aggregate.aggs",
                "Window.partition_by",
                "Window.order_by",
                "Window.exprs.call",
                "Join.on",
                "Sort.sort_keys",
                "Assert.condition",
                "Pivot.aggregate",
                "Pivot.values",
                "Call.args",
            ]
        );
    }

    #[test]
    fn contains_semantic_descends_into_subquery_bodies() {
        // A semantic node hidden inside a scalar subquery within the predicate.
        let predicate = LogicalExpr::BinaryOp {
            left: Box::new(scalar_subquery(semantic_filter())),
            op: BinaryOp::Eq,
            right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(1))),
            return_type: VeinDataType::Boolean,
        };
        let outer = LogicalPlan::Filter {
            input: Box::new(scan_plan()),
            predicate,
        };
        assert!(outer.contains_semantic());
    }

    #[test]
    fn contains_semantic_without_subqueries_is_false() {
        let predicate = LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(col(0, "id"))),
            op: BinaryOp::Eq,
            right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(1))),
            return_type: VeinDataType::Boolean,
        };
        let outer = LogicalPlan::Filter {
            input: Box::new(scan_plan()),
            predicate,
        };
        assert!(!outer.contains_semantic());
    }
}
