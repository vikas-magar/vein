use super::logical::LogicalPlan;
use crate::schema::VeinSchema;
use error::VeinError;

/// Defines traversal methods for logical plans
pub trait PlanTraversal {
    /// Returns the schema of the plan node
    fn schema(&self) -> &VeinSchema;
    /// Returns a list of child plans for traversal
    fn children(&self) -> Vec<&LogicalPlan>;
    /// Maps children using a mutable function
    fn map_children<F>(&self, f: &mut F) -> Result<LogicalPlan, VeinError>
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
    fn schema(&self) -> &VeinSchema {
        match self {
            LogicalPlan::Scan { schema, .. }
            | LogicalPlan::ExternalScan { schema, .. }
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

    fn map_children<F>(&self, f: &mut F) -> Result<LogicalPlan, VeinError>
    where
        F: FnMut(Box<LogicalPlan>) -> Result<Box<LogicalPlan>, VeinError>,
    {
        match self.clone() {
            LogicalPlan::Scan { .. } | LogicalPlan::ExternalScan { .. } => Ok(self.clone()),
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
        use crate::expr::LogicalExpr;
        use crate::plan::types::SortKey;

        fn from_sort_keys(keys: &[SortKey]) -> Vec<&LogicalPlan> {
            keys.iter()
                .flat_map(|k| k.expr.subquery_plans())
                .collect()
        }

        fn from_exprs(exprs: &[LogicalExpr]) -> Vec<&LogicalPlan> {
            exprs.iter().flat_map(|e| e.subquery_plans()).collect()
        }

        match self {
            LogicalPlan::Scan { filters, .. } => from_exprs(filters),
            LogicalPlan::Filter { predicate, .. } => predicate.subquery_plans(),
            LogicalPlan::Project { exprs, .. } => from_exprs(exprs),
            LogicalPlan::Aggregate {
                grouping_sets,
                aggs,
                ..
            } => grouping_sets
                .iter()
                .flat_map(|g| from_exprs(g))
                .chain(from_exprs(aggs))
                .collect(),
            LogicalPlan::Window {
                partition_by,
                order_by,
                exprs,
                ..
            } => from_exprs(partition_by)
                .into_iter()
                .chain(from_sort_keys(order_by))
                .chain(exprs.iter().flat_map(|w| w.call.subquery_plans()))
                .collect(),
            LogicalPlan::Sort { sort_keys, .. } => from_sort_keys(sort_keys),
            LogicalPlan::Join { on: Some(on), .. } => on.subquery_plans(),
            LogicalPlan::Join { on: None, .. } => vec![],
            LogicalPlan::Assert { condition, .. } => condition.subquery_plans(),
            LogicalPlan::Pivot {
                aggregate,
                values,
                ..
            } => aggregate
                .subquery_plans()
                .into_iter()
                .chain(from_exprs(values))
                .collect(),
            LogicalPlan::Call { args, .. } => from_exprs(args),
            _ => vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::logical::LogicalExpr;
    use crate::expr::LogicalLiteral;
    use crate::schema::{ColumnId, ResolvedColumn, VeinSchema};
    use crate::VeinDataType;
    use parser::ast::expr::BinaryOp;

    fn col(id: u64, name: &str) -> ResolvedColumn {
        ResolvedColumn {
            id: ColumnId(id as usize),
            name: name.to_string(),
            qualifier: Some("users".to_string()),
            data_type: VeinDataType::Int64,
            is_factorized: false,
            nullable: false,
        }
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
