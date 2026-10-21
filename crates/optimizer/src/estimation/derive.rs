//! Operator models over borrowed metadata and small scalar summaries.

use std::collections::HashMap;

use plan::{LogicalExpr, LogicalPlan, PlanTraversal};
use parser::ast::query::SetOpKind;
use crate::context::StatisticsProvider;
use super::{Estimate, EstimateSource, SelectivityEstimator};
use super::columns::RelationEstimate;

mod table;
fn scan<'a, P: StatisticsProvider + ?Sized>(
    node: &'a LogicalPlan, provider: &'a P, model: &impl SelectivityEstimator,
) -> RelationEstimate<'a> {
    let LogicalPlan::Scan { table, schema, filters, .. } = node else { unreachable!(); };
    let mut input = table::table_estimate(table, schema, provider);
    // Evaluate all pushed predicates against the same pre-filter population.
    let mut fraction = super::PredicateEstimate::logical(1.0, 0.0);
    let mut fallback_predicates = 0usize;
    for (index, predicate) in filters.iter().enumerate() {
        if filters[..index].contains(predicate) { continue; }
        let estimate = model.estimate(predicate, &input.columns);
        let (selectivity, used_fallback) = conservative_filter_selectivity(estimate.true_fraction);
        if used_fallback {
            fallback_predicates += 1;
        }
        fraction = fraction.and(super::PredicateEstimate {
            true_fraction: selectivity,
            false_fraction: estimate.false_fraction,
        });
    }
    if fallback_predicates > 0 {
        let value = fraction.true_fraction.value().unwrap_or(1.0);
        fraction.true_fraction = Estimate::model(value, EstimateSource::SelectivityFallback);
        tracing::info!(target: "vein.optimizer", event = "optimizer_filter_selectivity_fallback",
            table = %table.name, predicate_count = filters.len(), fallback_predicates,
            fallback = "pass_through_for_unestimated_predicates",
            "filter rows remain estimable using a low-confidence pass-through prior");
    }
    if !filters.is_empty() {
        filter(&mut input, fraction.true_fraction);
        for predicate in filters { input.restrict(predicate); }
    }
    input
}


fn remap_relation<'a>(
    input: &RelationEstimate<'a>,
    mapping: impl IntoIterator<Item=(plan::ColumnId, plan::ColumnId)>,
    schema: &plan::VeinSchema,
) -> RelationEstimate<'a> {
    let mapping = mapping.into_iter().collect::<HashMap<_, _>>();
    let mut result = RelationEstimate::new(input.rows);
    for (source, output) in &mapping {
        if let Some(statistics) = input.columns.get(source) {
            result.columns.insert(*output, *statistics);
        }
    }
    result.joint_distinct = input.remap_joint_distinct(&mapping);
    result.retain_schema(schema);
    result
}
fn filter(input: &mut RelationEstimate<'_>, fraction: Estimate) {
    let fraction = if fraction.value().is_some_and(|v| v > 1.0) {
        Estimate::unknown(EstimateSource::InvalidStatistics)
    } else { fraction };
    let source = if fraction.source == EstimateSource::SelectivityFallback {
        EstimateSource::SelectivityFallback
    } else {
        EstimateSource::Filter
    };
    let rows = if input.rows.value() == Some(0.0) {
        input.rows
    } else if fraction.value() == Some(0.0) {
        fraction
    } else {
        input.rows.combine(fraction, source, |r, f| r * f)
    };
    input.subset(rows);
    input.selectivity = Some(fraction);
}

pub(super) fn derive<'a, P: StatisticsProvider + ?Sized>(
    node: &'a LogicalPlan, inputs: impl IntoIterator<Item=RelationEstimate<'a>>,
    provider: &'a P, model: &impl SelectivityEstimator,
) -> RelationEstimate<'a> {
    let mut inputs = inputs.into_iter();
    let mut input = inputs.next().unwrap_or_else(RelationEstimate::unknown);
    input.selectivity = None;
    match node {
        LogicalPlan::Scan { .. } => scan(node, provider, model),
        LogicalPlan::Intermediate { source_id, row_count, schema } => {
            if let Some(statistics) = provider.intermediate_statistics(*source_id) {
                if statistics.num_rows == Some(*row_count) {
                    return table::statistics_estimate(statistics, schema,
                                                      |column| common::statistics::intermediate_column_key(column.id), EstimateSource::RuntimeFeedback);
                }
                tracing::warn!(target: "vein.optimizer", event = "adaptive_statistics_rejected",
                    source_id, expected_rows = row_count, synopsis_rows = ?statistics.num_rows,
                    reason = "population_mismatch", "checkpoint statistics rejected");
            }
            let mut relation = RelationEstimate::new(Estimate::known(
                *row_count as f64,
                EstimateSource::RuntimeFeedback,
                super::EstimateConfidence::Statistics,
            ));
            for column in &schema.fields {
                relation.add_cardinality_prior(column.id, *row_count);
            }
            relation
        }
        LogicalPlan::ExternalScan {
            source_metadata: Some(table),
            schema,
            ..
        } => table::table_estimate(table, schema, provider),
        LogicalPlan::ExternalScan { .. } => RelationEstimate::new(
            Estimate::unknown(EstimateSource::MissingStatistics)),
        LogicalPlan::Filter { predicate, .. } => {
            let estimate = model.estimate(predicate, &input.columns);
            let (fraction, used_fallback) = conservative_filter_selectivity(estimate.true_fraction);
            if used_fallback {
                tracing::info!(target: "vein.optimizer", event = "optimizer_filter_selectivity_fallback",
                    operator = node.operator_name(), fallback = "pass_through_for_unestimated_predicate",
                    "filter rows remain estimable using a low-confidence pass-through prior");
            }
            filter(&mut input, fraction);
            input.restrict(predicate);
            input
        }
        LogicalPlan::Project { exprs, schema, .. } => {
            remap_relation(
                &input,
                exprs.iter().zip(&schema.fields).filter_map(|(expr, output)| {
                    if let LogicalExpr::Column(column) = expr {
                        Some((column.id, output.id))
                    } else {
                        None
                    }
                }),
                schema,
            )
        }
        LogicalPlan::SubqueryAlias { input: source, schema, .. } => {
            remap_relation(
                &input,
                source.schema().fields.iter().zip(&schema.fields)
                    .map(|(original, output)| (original.id, output.id)),
                schema,
            )
        }
        LogicalPlan::Aggregate { grouping_sets, schema, .. } =>
            super::aggregate::aggregate(input, grouping_sets, schema),
        LogicalPlan::Distinct { input: source, .. } => {
            let keys = source.schema().fields.iter().map(|column| Some(column.id));
            let rows = super::aggregate::groups(&input, keys);
            input.subset(rows);
            input
        }
        LogicalPlan::Join { join_type, on, schema, .. } =>
            super::join::join(input, inputs.next().unwrap_or_else(RelationEstimate::unknown),
                              join_type, on.as_ref(), schema),
        LogicalPlan::Limit { offset, count, .. } => {
            let rows = if *count == 0 { Estimate::logical(0.0) } else {
                input.rows.map(EstimateSource::Derived, |r| (r - *offset as f64).max(0.0).min(*count as f64))
            };
            input.subset(rows);
            input
        }
        LogicalPlan::Sample { percent, .. } if percent.is_finite() && (0.0..=100.0).contains(percent) => {
            let rows = input.rows.combine(Estimate::model(percent / 100.0, EstimateSource::Derived),
                                          EstimateSource::Derived, |r, s| r * s);
            input.subset(rows);
            input
        }
        LogicalPlan::SetOp { kind: SetOpKind::Union, all: true, .. } => {
            let right = inputs.next().unwrap_or_else(RelationEstimate::unknown);
            RelationEstimate::new(input.rows.combine(right.rows, EstimateSource::Derived, |l, r| l + r))
        }
        LogicalPlan::Sort { schema, .. } | LogicalPlan::Window { schema, .. }
        | LogicalPlan::Assert { schema, .. } => {
            input.retain_schema(schema);
            input
        }
        LogicalPlan::Explain { .. } => RelationEstimate::unknown(),
        LogicalPlan::SetOp { .. } | LogicalPlan::Sample { .. }
        | LogicalPlan::Pivot { .. } | LogicalPlan::Unpivot { .. } | LogicalPlan::Fork { .. }
        | LogicalPlan::Tee { .. } | LogicalPlan::SemanticFilter { .. }
        | LogicalPlan::SemanticJoin { .. } | LogicalPlan::SemanticCalibrate { .. }
        | LogicalPlan::Write { .. } | LogicalPlan::Call { .. } => RelationEstimate::unknown(),
    }
}

/// A filter's unknown selectivity uses the identity prior: assume it retains
/// every input row. This is an upper bound for a row filter and never a pruning
/// proof. Invalid extension estimates take the same safe planning path.
fn conservative_filter_selectivity(estimate: Estimate) -> (Estimate, bool) {
    if estimate.value().is_some_and(|fraction| (0.0..=1.0).contains(&fraction)) {
        (estimate, false)
    } else {
        (Estimate::model(1.0, EstimateSource::SelectivityFallback), true)
    }
}
