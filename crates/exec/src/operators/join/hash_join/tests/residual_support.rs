use std::sync::Arc;

use common::protocol::{BinaryExprOp, ExprKind, ExpressionProgram, ExpressionStep, PhysicalExpr};
use types::VeinDataType;

use crate::expression::eval::{
    ExpressionKernelProvider, ExpressionKernelRegistry,
};
use crate::expression::executor::ExpressionExecutor;
use functions::FunctionRegistry;

use super::*;

pub(super) type TestExpressionExecutor = ExpressionExecutor<dyn ExpressionKernelProvider>;

pub(super) fn expression_column(index: u32, return_type: VeinDataType) -> PhysicalExpr {
    PhysicalExpr {
        return_type,
        kind: ExprKind::Column { index },
    }
}

pub(super) fn expression_binary(
    op: BinaryExprOp,
    left: PhysicalExpr,
    right: PhysicalExpr,
    return_type: VeinDataType,
) -> PhysicalExpr {
    PhysicalExpr {
        return_type,
        kind: ExprKind::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        },
    }
}

pub(super) fn expression_filter(
    predicate: PhysicalExpr,
    next: JoinFilterCollector,
) -> TestExpressionExecutor {
    let program = Arc::new(ExpressionProgram {
        steps: vec![ExpressionStep::Filter(predicate)],
    });
    let kernels: Arc<dyn ExpressionKernelProvider> =
        Arc::new(ExpressionKernelRegistry::with_builtins());
    let functions = Arc::new(FunctionRegistry::new());
    ExpressionExecutor::new(program, kernels, functions, Box::new(next))
}
