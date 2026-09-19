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

use error::Result;
use plan::{LogicalPlan, PlanTraversal};

use crate::rule::{OptimizerRule, RuleType};
use crate::rules::ConstantFoldingRule;

/// Hard cap on the number of full rule sweeps before giving up.
///
/// Correct rules converge in one sweep (once a node is folded, re-folding it
/// is a no-op); the cap exists to bound misbehaving or interacting rules.
const MAX_SWEEPS: usize = 8;

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
}

impl OptimizerPipeline {
    /// Creates a pipeline from the given rules.
    pub fn new(rules: Vec<Arc<dyn OptimizerRule>>) -> Self {
        Self { rules }
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
    /// Currently installs [`ConstantFoldingRule`]; new universally-beneficial
    /// rules are added here as they land.
    pub fn production() -> Self {
        Self::new(vec![Arc::new(ConstantFoldingRule)])
    }
}

impl Default for OptimizerPipeline {
    /// [`production`](Self::production) as the default pipeline.
    fn default() -> Self {
        Self::production()
    }
}

impl Optimizer for OptimizerPipeline {
    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let heuristic_rules: Vec<_> = self
            .rules
            .iter()
            .filter(|rule| rule.rule_type() == RuleType::Heuristic)
            .collect();

        let mut current = plan;
        for _ in 0..MAX_SWEEPS {
            let mut next = current;
            let mut changed = false;
            for rule in &heuristic_rules {
                let (result, any_changed) = apply_rule(rule.as_ref(), next)?;
                next = result;
                changed |= any_changed;
            }
            if !changed {
                return Ok(next);
            }
            current = next;
        }
        Ok(current)
    }
}

/// Applies a single rule bottom-up to the whole plan.
///
/// Returns the rewritten plan and whether the rule changed anything.
fn apply_rule(rule: &dyn OptimizerRule, plan: LogicalPlan) -> Result<(LogicalPlan, bool)> {
    let changed = Cell::new(false);
    let result = rewrite_bottom_up(plan, &mut |node| {
        Ok(match rule.apply(&node)? {
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