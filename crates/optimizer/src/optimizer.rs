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
use common::statistics::QueryStatisticsContext;

use crate::context::{CatalogStatisticsProvider, OptimizationDiagnostics, OptimizerConfig,
                     OptimizerContext, QueryStatisticsProvider, RuleExecution, StatisticsProvider};
use crate::rule::{OptimizerRule, RuleType};
use crate::rules::{
    ColumnPruningRule, ConstantFoldingRule, ExistsDecorrelationRule, ExpressionSimplificationRule, FilterNormalizationRule,
    FilterTransitivityRule,
    JoinPredicateTransitivityRule, PredicatePushdownRule, ScanFilterPushdownRule,
    ScanStatisticsPruningRule, ScalarAggregateDecorrelationRule,
};
use crate::rules::join_order::JoinOrderRule;

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
    statistics: Arc<dyn StatisticsProvider>,
}

impl OptimizerPipeline {
    /// Creates a pipeline from the given rules.
    pub fn new(rules: Vec<Arc<dyn OptimizerRule>>) -> Self {
        Self {
            rules,
            config: OptimizerConfig::default(),
            statistics: Arc::new(CatalogStatisticsProvider),
        }
    }

    /// Sets the optimizer configuration.
    pub fn with_config(mut self, config: OptimizerConfig) -> Self {
        self.config = config;
        self
    }

    /// Installs a statistics source shared by all planning estimates.
    pub fn with_statistics_provider(mut self, provider: Arc<dyn StatisticsProvider>) -> Self {
        self.statistics = provider;
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

    /// The default production pipeline: heuristic rewrites followed by
    /// bounded statistics-driven join-order selection.
    ///
    /// Installs constant folding followed by expression simplification.
    pub fn production() -> Self {
        Self::new(vec![
            Arc::new(ConstantFoldingRule),
            Arc::new(ExpressionSimplificationRule),
            Arc::new(FilterNormalizationRule),
            Arc::new(FilterTransitivityRule),
            Arc::new(ScanStatisticsPruningRule),
            Arc::new(JoinPredicateTransitivityRule),
            Arc::new(ExistsDecorrelationRule),
            Arc::new(ScalarAggregateDecorrelationRule),
            Arc::new(PredicatePushdownRule),
            Arc::new(ScanFilterPushdownRule),
            Arc::new(ColumnPruningRule),
            Arc::new(JoinOrderRule::default()),
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
        self.optimize_with_statistics_provider(plan, Arc::clone(&self.statistics))
    }

    /// Optimizes using the query-scoped metadata and runtime statistics context.
    pub fn optimize_with_statistics_context(
        &self,
        plan: LogicalPlan,
        statistics: Arc<QueryStatisticsContext>,
    ) -> Result<(LogicalPlan, OptimizationDiagnostics)> {
        self.optimize_with_statistics_provider(
            plan,
            Arc::new(QueryStatisticsProvider::new(statistics)),
        )
    }

    fn optimize_with_statistics_provider(
        &self,
        plan: LogicalPlan,
        statistics: Arc<dyn StatisticsProvider>,
    ) -> Result<(LogicalPlan, OptimizationDiagnostics)> {
        let context = OptimizerContext::with_statistics_provider(
            self.config.clone(), statistics);
        let rules: Vec<_> = self
            .rules
            .iter()
            .filter(|rule| rule.rule_type() == RuleType::Heuristic)
            .collect();
        let cost_rules: Vec<_> = self
            .rules
            .iter()
            .filter(|rule| rule.rule_type() == RuleType::CostBased)
            .collect();

        let mut diagnostics = OptimizationDiagnostics::default();
        let mut current = plan;
        tracing::info!(
            target: "vein.audit",
            event = "optimizer_started",
            rule_count = rules.len(),
            cost_rule_count = cost_rules.len(),
            max_sweeps = context.config().max_sweeps,
            "optimizer_started"
        );

        for sweep in 1..=context.config().max_sweeps {
            let mut next = current;
            let mut changed = false;

            for rule in &rules {
                let started = Instant::now();
                let (result, any_changed, _) = apply_rule(rule.as_ref(), next, &context)?;
                next = result;
                changed |= any_changed;
                let elapsed = started.elapsed();
                tracing::debug!(
                    target: "vein.optimizer",
                    event = "optimizer_rule_applied",
                    rule = rule.name(),
                    phase = ?rule.phase(),
                    sweep,
                    changed = any_changed,
                    duration_us = elapsed.as_micros() as u64,
                    "optimizer_rule_applied"
                );
                diagnostics.rule_executions.push(RuleExecution {
                    rule_name: rule.name().to_string(),
                    phase: rule.phase(),
                    sweep,
                    changed: any_changed,
                    elapsed,
                });
                if any_changed {
                    tracing::trace!(
                        target: "vein.optimizer",
                        event = "optimizer_plan_after_rule",
                        rule = rule.name(),
                        phase = ?rule.phase(),
                        sweep,
                        plan = %plan::display::format_plan(&next),
                        "optimizer_plan_after_rule"
                    );
                }
            }

            diagnostics.sweeps = sweep;
            if !changed {
                let mut optimized = next;
                for rule in &cost_rules {
                    let started = Instant::now();
                    let (result, any_changed, join_decisions) =
                        apply_rule(rule.as_ref(), optimized, &context)?;
                    optimized = result;
                    diagnostics.join_order_decisions.extend(join_decisions);
                    let elapsed = started.elapsed();
                    tracing::debug!(
                        target: "vein.optimizer",
                        event = "optimizer_rule_applied",
                        rule = rule.name(),
                        phase = ?rule.phase(),
                        sweep,
                        changed = any_changed,
                        duration_us = elapsed.as_micros() as u64,
                        "optimizer_rule_applied"
                    );
                    diagnostics.rule_executions.push(RuleExecution {
                        rule_name: rule.name().to_string(),
                        phase: rule.phase(),
                        sweep,
                        changed: any_changed,
                        elapsed,
                    });
                }
                validate_lowered_subqueries(&optimized)?;
                diagnostics.cardinalities = context.estimate_plan(&optimized);
                let filter_visibility = FilterPlacementVisibility::from_plan(&optimized);
                tracing::info!(
                    target: "vein.audit",
                    event = "optimizer_filter_placement",
                    scan_count = filter_visibility.scan_count,
                    scan_filter_count = filter_visibility.scan_filter_count,
                    residual_filter_count = filter_visibility.residual_filter_count,
                    "optimizer_filter_placement"
                );
                tracing::info!(
                    target: "vein.audit",
                    event = "optimizer_converged",
                    sweeps = sweep,
                    rule_executions = diagnostics.rule_executions.len(),
                    "optimizer_converged"
                );
                return Ok((optimized, diagnostics));
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

/// Prevents unsupported subqueries from reaching physical expression
/// compilation, where the resulting error lacks the owning operator context.
/// Supported scalar and EXISTS rewrites remove their subquery expressions
/// before this final invariant check.
fn validate_lowered_subqueries(plan: &LogicalPlan) -> Result<()> {
    let mut unsupported_owner = None;
    plan.for_each_expression(&mut |owner, expression| {
        if unsupported_owner.is_none() && !expression.subquery_plans().is_empty() {
            unsupported_owner = Some(owner);
        }
    });

    if let Some(owner) = unsupported_owner {
        return Err(VeinError::planning(
            format!("subquery expression in {owner} was not lowered by the optimizer"),
            None,
        ));
    }

    for child in plan.children() {
        validate_lowered_subqueries(child)?;
    }
    Ok(())
}

impl Optimizer for OptimizerPipeline {
    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        self.optimize_with_diagnostics(plan)
            .map(|(optimized, _)| optimized)
    }
}

/// Counts filters attached to scans and filters that remain above relational
/// operators after optimization. The latter are usually residual filters whose
/// semantics do not permit moving them into a source.
#[derive(Debug, Default, PartialEq, Eq)]
struct FilterPlacementVisibility {
    scan_count: usize,
    scan_filter_count: usize,
    residual_filter_count: usize,
}

impl FilterPlacementVisibility {
    fn from_plan(plan: &LogicalPlan) -> Self {
        fn visit(plan: &LogicalPlan, summary: &mut FilterPlacementVisibility) {
            match plan {
                LogicalPlan::Scan { filters, .. } => {
                    summary.scan_count += 1;
                    summary.scan_filter_count += filters.len();
                }
                LogicalPlan::Filter { .. } => summary.residual_filter_count += 1,
                _ => {}
            }
            for child in plan.children() {
                visit(child, summary);
            }
            for subquery in plan.subquery_plans() {
                visit(subquery, summary);
            }
        }

        let mut summary = Self::default();
        visit(plan, &mut summary);
        summary
    }
}

/// Applies a single rule bottom-up to the whole plan.
///
/// Returns the rewritten plan and whether the rule changed anything.
fn apply_rule(
    rule: &dyn OptimizerRule,
    plan: LogicalPlan,
    context: &OptimizerContext,
) -> Result<(LogicalPlan, bool, Vec<crate::context::JoinOrderDecision>)> {
    if rule.is_plan_rule() {
        return rule.apply_owned_plan(plan, context);
    }

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
    Ok((result, changed.get(), Vec::new()))
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
mod tests;
