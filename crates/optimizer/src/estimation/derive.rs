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
    for (index, predicate) in filters.iter().enumerate() {
        if filters[..index].contains(predicate) { continue; }
        fraction = fraction.and(model.estimate(predicate, &input.columns));
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
    let rows = if input.rows.value() == Some(0.0) {
        input.rows
    } else if fraction.value() == Some(0.0) {
        fraction
    } else {
        input.rows.combine(fraction, EstimateSource::Filter, |r, f| r * f)
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
        LogicalPlan::ExternalScan { .. } => RelationEstimate::new(
            Estimate::unknown(EstimateSource::MissingStatistics)),
        LogicalPlan::Filter { predicate, .. } => {
            let fraction = model.estimate(predicate, &input.columns).true_fraction;
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
