//! The default [`PhysicalExprFactory`]: maps a structured, planner-resolved
//! [`FilterPredicate`] (column index + operator + literal) into a
//! [`PhysicalExpr`] the executor can evaluate against Arrow batches.

use crate::expr::{BinaryExpr, BinaryOperator, ColumnExpr, LiteralExpr, LiteralValue, PhysicalExpr};
use crate::runtime::PhysicalExprFactory;
use common::protocol::{FilterOp, FilterPredicate, FilterValue};
use common::Result;

/// Builds filter predicates from the wire [`FilterPredicate`] form.
#[derive(Debug, Default)]
pub struct DefaultPhysicalExprFactory;

impl DefaultPhysicalExprFactory {
    /// Create a new default factory.
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalExprFactory for DefaultPhysicalExprFactory {
    fn build(&self, filter: &FilterPredicate) -> Result<Box<dyn PhysicalExpr>> {
        let column = ColumnExpr::new("", filter.column as usize);
        let op = lower_op(filter.op);
        let literal = LiteralExpr::new(lower_value(&filter.literal)?);
        Ok(Box::new(BinaryExpr::new(
            Box::new(column),
            op,
            Box::new(literal),
        )))
    }
}

/// Map a wire filter operator to an executor binary operator.
fn lower_op(op: FilterOp) -> BinaryOperator {
    match op {
        FilterOp::Eq => BinaryOperator::Eq,
        FilterOp::Neq => BinaryOperator::Neq,
        FilterOp::Lt => BinaryOperator::Lt,
        FilterOp::Gt => BinaryOperator::Gt,
        FilterOp::Leq => BinaryOperator::Leq,
        FilterOp::Geq => BinaryOperator::Geq,
    }
}

/// Map a wire filter literal to an executor literal value.
fn lower_value(value: &FilterValue) -> Result<LiteralValue> {
    Ok(match value {
        FilterValue::Boolean(b) => LiteralValue::Boolean(*b),
        FilterValue::Int(i) => LiteralValue::Int64(*i),
        FilterValue::Float(f) => LiteralValue::Float64(*f),
        FilterValue::String(s) => LiteralValue::String(s.clone()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn builds_gt_predicate() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![10, 50, 200])) as _],
        )
        .unwrap();

        let factory = DefaultPhysicalExprFactory::new();
        let expr = factory
            .build(&FilterPredicate {
                column: 0,
                op: FilterOp::Gt,
                literal: FilterValue::Int(100),
            })
            .unwrap();
        let chunk = crate::daemon::pipeline::DataChunk::from_record_batch(batch);
        let result = expr.evaluate(&chunk).unwrap();
        let bools = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(
            bools.iter().collect::<Vec<_>>(),
            vec![Some(false), Some(false), Some(true)]
        );
    }
}