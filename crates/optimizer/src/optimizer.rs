//! The optimizer pipeline: applies rules bottom-up until a fixpoint.
//!
//! # Algorithm
//!
//! One *sweep* applies every heuristic rule to the whole plan:
//!
//! 1. Rewrite the tree bottom-up with a single rule (children are rewritten
//!    before the parent, so a rule always sees folded subtrees).
//! 2. A rule reports whether it changed anything.
//!
//! Sweeps repeat while any rule changed the plan, up to
//! [`MAX_SWEEPS`]. Convergence is structural: two plans that are
//! [`PartialEq`]-equal are considered identical.

use std::cell::Cell;
use std::sync::Arc;
use std::time::Instant;

use error::{Result, VeinError};
use plan::{LogicalPlan, PlanTraversal};

use crate::context::{
    OptimizationDiagnostics, OptimizerConfig, OptimizerContext, RuleExecution,
};
use crate::rule::{OptimizerRule, RuleType};
use crate::rules::{ConstantFoldingRule, ExpressionSimplificationRule};

/// The engine-facing optimizer seam.
///
/// The binder produces a [`LogicalPlan`]; an implementation of this trait
/// rewrites it into a cheaper equivalent plan. The reference implementation
/// is [`OptimizerPipeline`], but the seam allows swapping in rule pipelines
/// or, in the future, cost-based optimizers without touching the engine.
pub trait Optimizer: Send + Sync {
    /// Optimizes the given logical plan.
    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan>;
}

/// An ordered, extensible collection of rules driven to a fixpoint.
pub struct OptimizerPipeline {
    rules: Vec<Arc<dyn OptimizerRule>>,
    config: OptimizerConfig,
}

impl OptimizerPipeline {
    /// Creates a pipeline from the given rules.
    pub fn new(rules: Vec<Arc<dyn OptimizerRule>>) -> Self {
        Self {
            rules,
            config: OptimizerConfig::default(),
        }
    }

    /// Sets the optimizer configuration.
    pub fn with_config(mut self, config: OptimizerConfig) -> Self {
        self.config = config;
        self
    }

    /// Appends a rule to the pipeline.
    ///
    /// Rules run in insertion order within a sweep; later rules observe the
    /// output of earlier ones.
    pub fn with_rule(mut self, rule: Arc<dyn OptimizerRule>) -> Self {
        self.rules.push(rule);
        self
    }

    /// The default production pipeline: heuristic rules only.
    ///
    /// Installs constant folding followed by expression simplification.
    pub fn production() -> Self {
        Self::new(vec![
            Arc::new(ConstantFoldingRule),
            Arc::new(ExpressionSimplificationRule),
        ])
    }
}

impl Default for OptimizerPipeline {
    /// [`production`](Self::production) as the default pipeline.
    fn default() -> Self {
        Self::production()
    }
}

impl OptimizerPipeline {
    /// Optimizes a plan and returns rule execution diagnostics.
    pub fn optimize_with_diagnostics(
        &self,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, OptimizationDiagnostics)> {
        let context = OptimizerContext::new(self.config.clone());
        let rules: Vec<_> = self
            .rules
            .iter()
            .filter(|rule| rule.rule_type() == RuleType::Heuristic)
            .collect();

        let mut diagnostics = OptimizationDiagnostics::default();
        let mut current = plan;

        for sweep in 1..=context.config().max_sweeps {
            let mut next = current;
            let mut changed = false;

            for rule in &rules {
                let started = Instant::now();
                let (result, any_changed) = apply_rule(rule.as_ref(), next, &context)?;
                next = result;
                changed |= any_changed;
                diagnostics.rule_executions.push(RuleExecution {
                    rule_name: rule.name().to_string(),
                    phase: rule.phase(),
                    sweep,
                    changed: any_changed,
                    elapsed: started.elapsed(),
                });
            }

            diagnostics.sweeps = sweep;
            if !changed {
                return Ok((next, diagnostics));
            }
            current = next;
        }

        Err(VeinError::planning(
            format!(
                "optimizer did not converge within {} sweeps",
                context.config().max_sweeps
            ),
            None,
        ))
    }
}

impl Optimizer for OptimizerPipeline {
    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        self.optimize_with_diagnostics(plan)
            .map(|(optimized, _)| optimized)
    }
}

/// Applies a single rule bottom-up to the whole plan.
///
/// Returns the rewritten plan and whether the rule changed anything.
fn apply_rule(
    rule: &dyn OptimizerRule,
    plan: LogicalPlan,
    context: &OptimizerContext,
) -> Result<(LogicalPlan, bool)> {
    let changed = Cell::new(false);
    let result = rewrite_bottom_up(plan, &mut |node| {
        Ok(match rule.apply(&node, context)? {
            Some(rewritten) => {
                changed.set(true);
                rewritten
            }
            None => node,
        })
    })?;
    Ok((result, changed.get()))
}

/// Rewrites every node of the plan with `f`, children before parents.
fn rewrite_bottom_up<F>(plan: LogicalPlan, f: &mut F) -> Result<LogicalPlan>
where
    F: FnMut(LogicalPlan) -> Result<LogicalPlan>,
{
    let mapped = plan.map_children(&mut |child| Ok(Box::new(rewrite_bottom_up(*child, f)?)))?;
    f(mapped)
}

#[cfg(test)]
mod tests {
    use super::*;
    use plan::{LogicalExpr, LogicalLiteral, VeinDataType};

    fn lit(value: i64) -> LogicalExpr {
        LogicalExpr::Literal(LogicalLiteral::Int64(value))
    }

    fn project(exprs: Vec<LogicalExpr>) -> LogicalPlan {
        let schema = Box::new(plan::VeinSchema { fields: vec![] });
        let table = std::sync::Arc::new(plan::TableMetadata::new(
            "t",
            std::sync::Arc::new(*schema.clone()),
        ));
        LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                table,
                projection: None,
                filters: vec![],
                schema: schema.clone(),
            }),
            exprs,
            schema,
        }
    }

    fn plus(left: LogicalExpr, right: LogicalExpr) -> LogicalExpr {
        LogicalExpr::BinaryOp {
            left: Box::new(left),
            op: parser::ast::expr::BinaryOp::Plus,
            right: Box::new(right),
            return_type: VeinDataType::Int64,
        }
    }

    #[test]
    fn default_pipeline_folds_constants() {
        let pipeline = OptimizerPipeline::default();
        let plan = project(vec![plus(plus(lit(10), lit(5)), lit(2))]);

        let optimized = pipeline.optimize(plan).unwrap();
        let LogicalPlan::Project { exprs, .. } = optimized else {
            panic!("expected a Project node");
        };
        assert_eq!(exprs, vec![lit(17)]);
    }

    #[test]
    fn already_optimized_plan_converges_unchanged() {
        let pipeline = OptimizerPipeline::default();
        let plan = project(vec![lit(17)]);

        let optimized = pipeline.optimize(plan).unwrap();
        let LogicalPlan::Project { exprs, .. } = optimized else {
            panic!("expected a Project node");
        };
        assert_eq!(exprs, vec![lit(17)]);
    }

    #[test]
    fn custom_pipeline_appends_rules_in_order() {
        let pipeline = OptimizerPipeline::new(vec![]).with_rule(Arc::new(ConstantFoldingRule));
        let plan = project(vec![plus(plus(lit(1), lit(2)), lit(3))]);

        let optimized = pipeline.optimize(plan).unwrap();
        let LogicalPlan::Project { exprs, .. } = optimized else {
            panic!("expected a Project node");
        };
        assert_eq!(exprs, vec![lit(6)]);
    }
}