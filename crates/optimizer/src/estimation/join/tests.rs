use std::sync::Arc;

use parser::ast::expr::BinaryOp;
use plan::{ColumnId, LogicalExpr, ResolvedColumn, VeinDataType};

use super::super::{Estimate, EstimateConfidence, EstimateSource};
use super::super::columns::{ColumnEstimate, RelationEstimate};
use super::join_selectivity;

fn relation(column_id: usize) -> RelationEstimate<'static> {
    let mut relation = RelationEstimate::new(Estimate::known(
        100.0,
        EstimateSource::Statistics,
        EstimateConfidence::Statistics,
    ));
    relation.columns.insert(
        ColumnId(column_id),
        ColumnEstimate {
            distinct: Some(10.0),
            null_fraction: Some(0.0),
            min: None,
            max: None,
            synopsis: None,
            synopsis_population: None,
        },
    );
    relation
}

fn key_equality() -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(Arc::new(ResolvedColumn::new(
            ColumnId(1),
            "left_key",
            None,
            VeinDataType::Int64,
        )))),
        op: BinaryOp::Eq,
        right: Box::new(LogicalExpr::Column(Arc::new(ResolvedColumn::new(
            ColumnId(2),
            "right_key",
            None,
            VeinDataType::Int64,
        )))),
        return_type: VeinDataType::Boolean,
    }
}

fn exact_join_rows(left: &[u64], right: &[u64]) -> u64 {
    left.iter()
        .zip(right)
        .map(|(left, right)| left * right)
        .sum()
}

#[test]
fn ndv_model_exposes_its_uniformity_limit_under_skew() {
    let left = relation(1);
    let right = relation(2);
    let predicate = key_equality();
    let selectivity = join_selectivity(&left, &right, None, &[&predicate]);
    let estimated_rows = left.rows.value().unwrap()
        * right.rows.value().unwrap()
        * selectivity.value().unwrap();

    let uniform = [10_u64; 10];
    let mut aligned_skew = [1_u64; 10];
    aligned_skew[0] = 91;
    let uniform_actual = exact_join_rows(&uniform, &uniform);
    let skewed_actual = exact_join_rows(&aligned_skew, &aligned_skew);

    assert_eq!(estimated_rows, 1_000.0);
    assert_eq!(uniform_actual, 1_000);
    assert_eq!(skewed_actual, 8_290);
    assert_eq!(selectivity.confidence, EstimateConfidence::Model);
}
