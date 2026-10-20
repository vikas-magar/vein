//! Query-local identity for avoiding duplicate adaptive scans.

use plan::{ColumnId, LogicalExpr, LogicalPlan, ResolvedColumn};
use parser::ast::expr::BinaryOp;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub(super) struct ScanPopulationSignature {
    pub(super) source: String,
    pub(super) filters: Vec<LogicalExpr>,
    pub(super) output_columns: Vec<String>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct SignatureMatchCounts {
    pub(super) same_source: usize,
    pub(super) same_filters: usize,
    pub(super) same_projection: usize,
    pub(super) exact: usize,
}

pub(super) fn signature_match_counts(
    candidate: &ScanPopulationSignature,
    scheduled: &[ScanPopulationSignature],
) -> SignatureMatchCounts {
    let mut counts = SignatureMatchCounts::default();
    for previous in scheduled.iter().filter(|previous| previous.source == candidate.source) {
        counts.same_source += 1;
        let filters_match = previous.filters == candidate.filters;
        let projection_matches = previous.output_columns == candidate.output_columns;
        counts.same_filters += usize::from(filters_match);
        counts.same_projection += usize::from(projection_matches);
        counts.exact += usize::from(filters_match && projection_matches);
    }
    counts
}

pub(super) fn scan_population_signature(plan: &LogicalPlan) -> Option<ScanPopulationSignature> {
    let LogicalPlan::Scan { table, projection, filters, schema } = plan else {
        return None;
    };
    let mut filters = filters.iter().cloned().map(normalize_expression).collect::<Option<Vec<_>>>()?;
    filters.sort_by_cached_key(|expression| format!("{expression:?}"));
    let output_columns = schema.fields.iter()
        .filter(|column| projection.as_ref().is_none_or(|ids| ids.contains(&column.id)))
        .map(|column| format!("{}:{:?}:{}", column.name, column.data_type, column.nullable))
        .collect();
    let source = table.catalog_snapshot.as_ref()
        .map(common::statistics::runtime::table_statistics_key)
        .unwrap_or_else(|| format!("{:?}", (&table.name, &table.location, &table.format, &table.files)));
    Some(ScanPopulationSignature { source, filters, output_columns })
}

fn normalize_expression(expression: LogicalExpr) -> Option<LogicalExpr> {
    match expression {
        LogicalExpr::Column(column) => Some(LogicalExpr::Column(normalize_column(&column))),
        LogicalExpr::BinaryOp { left, op, right, return_type } => {
            let mut left = normalize_expression(*left)?;
            let mut right = normalize_expression(*right)?;
            if is_commutative_predicate(op) && format!("{right:?}") < format!("{left:?}") {
                std::mem::swap(&mut left, &mut right);
            }
            Some(LogicalExpr::BinaryOp {
                left: Box::new(left), op, right: Box::new(right), return_type,
            })
        }
        LogicalExpr::OuterColumn(_)
        | LogicalExpr::ScalarSubquery(_)
        | LogicalExpr::Exists { .. } => None,
        expression => expression.map_children(&mut |child| {
            normalize_expression(child).ok_or_else(|| error::VeinError::internal(
                "adaptive checkpoint identity cannot normalize this expression",
            ))
        }).ok(),
    }
}

fn is_commutative_predicate(op: BinaryOp) -> bool {
    matches!(op, BinaryOp::And | BinaryOp::Or | BinaryOp::Eq | BinaryOp::Neq)
}

fn normalize_column(column: &ResolvedColumn) -> Arc<ResolvedColumn> {
    Arc::new(ResolvedColumn {
        id: ColumnId(0),
        name: column.name.clone(),
        qualifier: None,
        data_type: column.data_type.clone(),
        is_factorized: column.is_factorized,
        nullable: column.nullable,
    })
}

#[cfg(test)]
#[path = "checkpoint_identity_tests.rs"]
mod tests;
