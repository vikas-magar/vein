use crate::ast::pipe_op::PipeOperator;
use crate::ast::query::Query;
use crate::logical_plan::{LogicalPlan, PlanError};

pub struct PlanBuilder;

impl PlanBuilder {
    pub fn build(query: &Query) -> Result<LogicalPlan, PlanError> {
        let scan = Self::build_scan(&query.source, &query.joins)?;
        let mut plan = scan;
        for pipe in &query.pipes {
            plan = Self::apply(plan, pipe)?;
        }
        Self::validate(&plan)?;
        Ok(plan)
    }

    pub fn apply(plan: LogicalPlan, op: &PipeOperator) -> Result<LogicalPlan, PlanError> {
        match op {
            PipeOperator::Where { predicate } => {
                Ok(LogicalPlan::Filter { predicate: predicate.clone(), input: Box::new(plan) })
            }
            PipeOperator::Select { items } => {
                let exprs: Vec<_> = items.iter().map(|i| i.expr.clone()).collect();
                Ok(LogicalPlan::Projection { expr: exprs, input: Box::new(plan) })
            }
            PipeOperator::Limit { limit, offset } => {
                Ok(LogicalPlan::Limit { limit: *limit as usize, offset: *offset as usize, input: Box::new(plan) })
            }
            PipeOperator::OrderBy { order_bys } => {
                Ok(LogicalPlan::Sort { order_by: order_bys.clone(), input: Box::new(plan) })
            }
            PipeOperator::Aggregate { group_by, aggregates } => {
                let agg_exprs: Vec<_> = aggregates.iter().map(|a| a.expr.clone()).collect();
                Ok(LogicalPlan::Aggregate {
                    group_expr: group_by.clone(),
                    aggregate_expr: agg_exprs,
                    input: Box::new(plan),
                })
            }
            PipeOperator::Distinct => {
                Ok(LogicalPlan::Distinct { input: Box::new(plan) })
            }
            PipeOperator::As { alias } => {
                Ok(LogicalPlan::TableAlias { alias: alias.clone(), input: Box::new(plan) })
            }
            PipeOperator::Extend { items } => {
                let exprs: Vec<_> = items.iter().map(|i| i.expr.clone()).collect();
                Ok(LogicalPlan::Extend { items: exprs, input: Box::new(plan) })
            }
            PipeOperator::Drop { columns } => {
                Ok(LogicalPlan::Drop { columns: columns.clone(), input: Box::new(plan) })
            }
            PipeOperator::Assert { condition, message } => {
                Ok(LogicalPlan::Assert { condition: condition.clone(), message: message.clone(), input: Box::new(plan) })
            }
            PipeOperator::Call { name, args } => {
                Ok(LogicalPlan::Call { name: name.clone(), args: args.clone() })
            }
            PipeOperator::Write { target } => {
                Ok(LogicalPlan::Write { target: target.clone(), input: Box::new(plan) })
            }
            PipeOperator::Join { join } => {
                let right = Self::build(&join.right)?;
                Ok(LogicalPlan::Join {
                    join_type: join.join_type,
                    left: Box::new(plan),
                    right: Box::new(right),
                    predicate: join.predicate.clone(),
                    using: join.using.clone(),
                })
            }
            PipeOperator::With { ctes } => {
                Ok(LogicalPlan::With { ctes: ctes.clone(), input: Box::new(plan) })
            }
            PipeOperator::SetOp { kind, right, all } => {
                let right = Self::build(right)?;
                Ok(LogicalPlan::SetOp {
                    kind: match kind {
                        crate::ast::query::SetOpKind::Union => crate::logical_plan::SetOpType::Union,
                        crate::ast::query::SetOpKind::Intersect => crate::logical_plan::SetOpType::Intersect,
                        crate::ast::query::SetOpKind::Except => crate::logical_plan::SetOpType::Except,
                    },
                    all: *all,
                    left: Box::new(plan),
                    right: Box::new(right),
                })
            }
            _ => Err(PlanError::UnsupportedFeature(format!("pipe operator not yet supported: {:?}", op))),
        }
    }

    fn build_scan(source: &crate::ast::query::TableSource, _joins: &[crate::ast::pipe_op::JoinClause]) -> Result<LogicalPlan, PlanError> {
        match source {
            crate::ast::query::TableSource::Table { name, alias: _ } => {
                Ok(LogicalPlan::Scan {
                    source: crate::logical_plan::ScanSource::Table(name.clone()),
                    projection: None,
                    filters: vec![],
                    limit: None,
                })
            }
            crate::ast::query::TableSource::ObjectStore { url, inline_options, alias: _ } => {
                Ok(LogicalPlan::Scan {
                    source: crate::logical_plan::ScanSource::ObjectStore { url: url.clone(), options: inline_options.clone() },
                    projection: None,
                    filters: vec![],
                    limit: None,
                })
            }
            crate::ast::query::TableSource::Subquery { query, alias: _ } => {
                Self::build(query)
            }
        }
    }

    fn validate(_plan: &LogicalPlan) -> Result<(), PlanError> {
        Ok(())
    }
}
