use super::*;
use crate::operators::source::MemorySource;
use crate::spill::{LocalSpillStorage, SpillStorage};
use arrow::array::Array;
use common::identifiers::{QueryId, RequestId, StageId, TaskId};
use common::protocol::{
    AggOpCode, AggregatePlan,
    BinaryExprOp, DataFileFormatId, ExecutionMode, ExprKind, ExpressionProgram, ExpressionStep, FileScanTask, JoinKind, JoinOutput, JoinPlan, LimitSpec,
    PhysicalExpr, PhysicalPlan, PhysicalPlanNode, PhysicalPlanOperator, SortKey, SortPlan,
};
use std::sync::atomic::AtomicUsize;
use types::{LogicalLiteral, VeinDataType};

mod fixtures;
mod fan_in;
mod join_execution;
mod join_semantics;
mod memory;
mod operator_contract;
mod parallel_execution;
mod recursive_joins;
mod recursive_pipeline;
mod scan;
mod spill;
mod task_output;
mod view_contract;
mod view_streaming;
mod zero_copy_e2e;
mod expression_pipeline;
mod query_scope;
mod provider_units;
mod scan_worker;

pub(in crate::runtime::tests) use fixtures::*;

pub(in crate::runtime::tests) fn view_int64_values(
    view: &DataChunkView,
    column: usize,
) -> Vec<i64> {
    (0..view.num_rows())
        .map(|row| {
            let (array, physical_row) = view
                .resolve(column, row)
                .expect("resolve view column")
                .expect("expected non-null view row");
            array
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("Int64 view column")
                .value(physical_row)
        })
        .collect()
}

pub(in crate::runtime::tests) fn view_nullable_int64_values(
    view: &DataChunkView,
    column: usize,
) -> Vec<Option<i64>> {
    (0..view.num_rows())
        .map(|row| {
            let (array, physical_row) = view
                .resolve(column, row)
                .expect("resolve view column")
                ?;
            let values = array
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("Int64 view column");
            values.is_valid(physical_row).then(|| values.value(physical_row))
        })
        .collect()
}

pub(in crate::runtime::tests) fn column_expr(index: u32, return_type: VeinDataType) -> PhysicalExpr {
    PhysicalExpr { return_type, kind: ExprKind::Column { index } }
}

pub(in crate::runtime::tests) fn literal_expr(
    value: LogicalLiteral,
    return_type: VeinDataType,
) -> PhysicalExpr {
    PhysicalExpr { return_type, kind: ExprKind::Literal(value) }
}

pub(in crate::runtime::tests) fn binary_expr(
    op: BinaryExprOp,
    left: PhysicalExpr,
    right: PhysicalExpr,
    return_type: VeinDataType,
) -> PhysicalExpr {
    PhysicalExpr {
        return_type,
        kind: ExprKind::Binary { op, left: Box::new(left), right: Box::new(right) },
    }
}

pub(in crate::runtime::tests) fn filter_program(predicate: PhysicalExpr) -> Arc<ExpressionProgram> {
    Arc::new(ExpressionProgram {
        steps: vec![ExpressionStep::Filter(predicate)],
    })
}

pub(in crate::runtime::tests) fn projection_program(
    expressions: Vec<PhysicalExpr>,
) -> Arc<ExpressionProgram> {
    Arc::new(ExpressionProgram {
        steps: vec![ExpressionStep::Projection(expressions)],
    })
}
