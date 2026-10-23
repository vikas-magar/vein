//! Reduces outer joins only when WHERE predicates reject null-extended rows.

use std::collections::HashSet;
use std::sync::Arc;

use error::Result;
use parser::ast::pipe_op::JoinType;
use plan::{LogicalPlan, PlanTraversal};

use crate::context::OptimizerContext;
use crate::rule::{OptimizerRule, RuleType};
use super::predicate_analysis::{BuiltinPredicateSemantics, PredicateSemantics};

/// Simplifies outer joins using conservative SQL three-valued predicate proofs.
pub struct OuterJoinSimplificationRule<S = BuiltinPredicateSemantics> {
    semantics: S,
}

impl Default for OuterJoinSimplificationRule {
    fn default() -> Self { Self::with_semantics(BuiltinPredicateSemantics) }
}

impl<S> OuterJoinSimplificationRule<S> {
    /// Creates a rule with a custom, conservative predicate semantics provider.
    pub fn with_semantics(semantics: S) -> Self { Self { semantics } }
}

impl<S: PredicateSemantics + Send + Sync> OptimizerRule for OuterJoinSimplificationRule<S> {
    fn name(&self) -> &str { "OuterJoinSimplificationRule" }
    fn rule_type(&self) -> RuleType { RuleType::Heuristic }

    fn apply(&self, plan: &LogicalPlan, _: &OptimizerContext) -> Result<Option<LogicalPlan>> {
        let LogicalPlan::Filter { input, predicate } = plan else { return Ok(None) };
        let LogicalPlan::Join { join_type, left, right, on, using, schema } = input.as_ref()
        else { return Ok(None) };
        if !matches!(join_type, JoinType::Left | JoinType::Right | JoinType::Full) { return Ok(None); }
        if !self.semantics.reorder_safe(predicate)
            || on.as_ref().is_some_and(|on| !self.semantics.reorder_safe(on)) {
            return Ok(None);
        }
        let rejects = |input: &LogicalPlan| {
            let columns: HashSet<_> = input.schema().fields.iter().map(|column| column.id).collect();
            self.semantics.rejects_nulls(predicate, &columns)
        };
        let selected = match join_type {
            JoinType::Left if rejects(right) => JoinType::Inner,
            JoinType::Right if rejects(left) => JoinType::Inner,
            JoinType::Full => match (rejects(left), rejects(right)) {
                (true, true) => JoinType::Inner,
                (true, false) => JoinType::Left,
                (false, true) => JoinType::Right,
                _ => return Ok(None),
            },
            _ => return Ok(None),
        };
        tracing::debug!(target: "vein.optimizer", event = "optimizer_outer_join_simplified",
            original_join = ?join_type, selected_join = ?selected, proof = "where_rejects_null_extended_rows");
        let left_schema = left.schema();
        let right_schema = right.schema();
        let mut schema = schema.clone();
        for field in &mut schema.fields {
            let source = left_schema.fields.iter().find(|source| source.id == field.id)
                .map(|source| (source, matches!(selected, JoinType::Right | JoinType::Full)))
                .or_else(|| right_schema.fields.iter().find(|source| source.id == field.id)
                    .map(|source| (source, matches!(selected, JoinType::Left | JoinType::Full))));
            if let Some((source, null_extended)) = source {
                let nullable = source.nullable || null_extended;
                if field.nullable != nullable { Arc::make_mut(field).nullable = nullable; }
            }
        }
        Ok(Some(LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Join {
                join_type: selected,
                left: left.clone(),
                right: right.clone(),
                on: on.clone(),
                using: using.clone(),
                schema,
            }),
            predicate: predicate.clone(),
        }))
    }
}

#[cfg(test)]
mod tests;
