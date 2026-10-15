//! Narrow, deterministic scan predicate stages, independent of storage format.

use std::collections::HashSet;
use plan::{ColumnId, LogicalExpr};
use super::PipelineCompiler;

struct Stage {
    columns: Vec<ColumnId>,
    correlated: bool,
    predicates: Vec<LogicalExpr>,
}

/// Group total, statistics-supported predicates with identical column sets.
/// Unknown expressions retain the original fused evaluation order. Estimates
/// only order safe stages; they never omit a predicate or establish correctness.
pub(super) fn group_predicates(
    predicates: &[LogicalExpr],
    estimate: impl Fn(&LogicalExpr) -> Option<f64>,
) -> Vec<Vec<LogicalExpr>> {
    let classes = predicates.iter().map(|predicate| {
        PipelineCompiler::scan_predicate_correlation_class(
            &PipelineCompiler::statistics_predicate(predicate))
    }).collect::<Option<Vec<_>>>();
    let Some(classes) = classes else { return vec![predicates.to_vec()]; };
    let mut stages: Vec<Stage> = Vec::new();
    for (predicate, correlated) in predicates.iter().zip(classes) {
        let mut columns = HashSet::new();
        PipelineCompiler::collect_column_ids(predicate, &mut columns);
        let mut columns = columns.into_iter().collect::<Vec<_>>();
        columns.sort_unstable();
        if let Some(stage) = stages.iter_mut().find(|stage| stage.columns == columns
            && stage.correlated == correlated) {
            stage.predicates.push(predicate.clone());
        } else {
            stages.push(Stage { columns, correlated, predicates: vec![predicate.clone()] });
        }
    }
    let mut ranked = stages.into_iter().map(|stage| {
        let combined = stage.predicates.iter().cloned().reduce(|left, right| {
            LogicalExpr::BinaryOp {
                left: Box::new(left),
                op: parser::ast::expr::BinaryOp::And,
                right: Box::new(right),
                return_type: plan::VeinDataType::Boolean,
            }
        }).expect("a stage contains at least one predicate");
        let pass = estimate(&combined).filter(|value| value.is_finite() && (0.0..=1.0).contains(value));
        // Input-column count is a conservative decode-cost proxy. A strategy
        // can supply selectivity from catalog bounds, histograms or sampling.
        let cost = pass.map(|pass| stage.columns.len().max(1) as f64 / (1.0 - pass))
            .unwrap_or(f64::INFINITY);
        (stage, pass, cost)
    }).collect::<Vec<_>>();
    ranked.sort_by(|(left, _, lc), (right, _, rc)| {
        left.correlated.cmp(&right.correlated).then_with(|| lc.total_cmp(rc))
    });
    // As soon as both operands are available, fuse a total column comparison
    // into that stage. Waiting for unrelated filter columns would decode rows
    // this comparison can already reject.
    let (mut correlations, independent): (Vec<_>, Vec<_>) =
        ranked.into_iter().partition(|(stage, _, _)| stage.correlated);
    let mut available = HashSet::new();
    let mut scheduled = Vec::new();
    for (mut stage, mut pass, cost) in independent {
        available.extend(stage.columns.iter().copied());
        let mut index = 0;
        while index < correlations.len() {
            if correlations[index].0.columns.iter().all(|column| available.contains(column)) {
                let (comparison, _, _) = correlations.remove(index);
                stage.columns.extend(comparison.columns);
                stage.columns.sort_unstable();
                stage.columns.dedup();
                stage.predicates.extend(comparison.predicates);
                stage.correlated = true;
                pass = None; // The literal-only estimate does not describe the fused stage.
            } else {
                index += 1;
            }
        }
        scheduled.push((stage, pass, cost));
    }
    scheduled.extend(correlations);
    scheduled.into_iter().enumerate().map(|(stage_index, (stage, pass, cost))| {
        tracing::debug!(target: "vein.optimizer", event = "scan_predicate_stage_order",
            stage_index, input_columns = ?stage.columns, correlated = stage.correlated,
            estimated_pass_fraction = ?pass, estimated_decode_cost_per_rejected_row = cost,
            "scan predicate stage ordered");
        stage.predicates
    }).collect()
}

#[cfg(test)]
mod tests;
