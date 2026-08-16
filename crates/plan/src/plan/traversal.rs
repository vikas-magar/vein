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
}

impl PlanTraversal for LogicalPlan {
    fn schema(&self) -> &VeinSchema {
        match self {
            LogicalPlan::Scan { schema, .. }
            | LogicalPlan::Project { schema, .. }
            | LogicalPlan::Aggregate { schema, .. }
            | LogicalPlan::Window { schema, .. }
            | LogicalPlan::Sort { schema, .. }
            | LogicalPlan::Limit { schema, .. }
            | LogicalPlan::Distinct { schema, .. }
            | LogicalPlan::SubqueryAlias { schema, .. }
            | LogicalPlan::Explain { schema, .. }
            | LogicalPlan::SetOp { schema, .. }
            | LogicalPlan::Sample { schema, .. }
            | LogicalPlan::Assert { schema, .. }
            | LogicalPlan::Fork { schema, .. }
            | LogicalPlan::Tee { schema, .. }
            | LogicalPlan::SemanticFilter { schema, .. }
            | LogicalPlan::SemanticJoin { schema, .. }
            | LogicalPlan::SemanticCalibrate { schema, .. }
            | LogicalPlan::Write { schema, .. } => schema,
            LogicalPlan::Filter { input, .. } => input.schema(),
        }
    }

    fn children(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::Scan { .. } => vec![],
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
            | LogicalPlan::SemanticFilter { input, .. }
            | LogicalPlan::SemanticCalibrate { input, .. }
            | LogicalPlan::Write { input, .. } => vec![input.as_ref()],
            // Join removed
            LogicalPlan::SetOp { left, right, .. }
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
            LogicalPlan::Scan { .. } => Ok(self.clone()),
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
        }
    }

    fn contains_semantic(&self) -> bool {
        match self {
            LogicalPlan::SemanticFilter { .. }
            | LogicalPlan::SemanticJoin { .. }
            | LogicalPlan::SemanticCalibrate { .. } => true,
            _ => self.children().iter().any(|c| c.contains_semantic()),
        }
    }
}
