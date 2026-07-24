pub mod rules;

#[cfg(test)]
mod tests;

use crate::logical_plan::{LogicalPlan, PlanError, Transformed};

pub trait OptimizerRule: Send + Sync {
    fn name(&self) -> &'static str;
    fn apply(&self, plan: &LogicalPlan) -> Result<Transformed<LogicalPlan>, PlanError>;
}

pub fn optimize(
    plan: &LogicalPlan,
    max_passes: Option<usize>,
) -> (LogicalPlan, Vec<&'static str>) {
    let rules: Vec<Box<dyn OptimizerRule>> = vec![
        Box::new(rules::FilterPushdown),
        Box::new(rules::LimitPushdown),
        Box::new(rules::ProjectionPushdown),
    ];
    let max = max_passes.unwrap_or(10);
    let mut current = plan.clone();
    let mut fired_rules = Vec::new();

    for _pass in 0..max {
        let mut any_fired = false;
        for rule in &rules {
            if let Ok(result) = rule.apply(&current) {
                if result.transformed {
                    any_fired = true;
                    fired_rules.push(rule.name());
                    current = result.data;
                }
            }
        }
        if !any_fired {
            break;
        }
    }

    (current, fired_rules)
}
