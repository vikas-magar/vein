use common::protocol::{BinaryExprOp, ExprKind, ExpressionProgram, ExpressionStep, PhysicalExpr};

pub(super) fn contains_filter(program: &ExpressionProgram) -> bool {
    program
        .steps
        .iter()
        .any(|step| matches!(step, ExpressionStep::Filter(_)))
}

pub(super) fn program_contains_binary(program: &ExpressionProgram, op: BinaryExprOp) -> bool {
    program.steps.iter().any(|step| match step {
        ExpressionStep::Filter(expression) => contains_binary(expression, op),
        ExpressionStep::Projection(expressions) => {
            expressions.iter().any(|expression| contains_binary(expression, op))
        }
    })
}

pub(super) fn contains_binary(expression: &PhysicalExpr, target: BinaryExprOp) -> bool {
    binary_count(expression, target) > 0
}

pub(super) fn binary_count(expression: &PhysicalExpr, target: BinaryExprOp) -> usize {
    match &expression.kind {
        ExprKind::Binary { op, left, right } => {
            usize::from(*op == target)
                + binary_count(left, target)
                + binary_count(right, target)
        }
        ExprKind::Cast { expr, .. } | ExprKind::Unary { expr, .. } => {
            binary_count(expr, target)
        }
        ExprKind::InList { expr, candidates, .. } => {
            binary_count(expr, target)
                + candidates.iter().map(|item| binary_count(item, target)).sum::<usize>()
        }
        ExprKind::Like { expr, pattern, .. } => {
            binary_count(expr, target) + binary_count(pattern, target)
        }
        ExprKind::Case { when_then, else_expr } => {
            let arms = when_then.iter().map(|(when, then)| {
                binary_count(when, target) + binary_count(then, target)
            }).sum::<usize>();
            arms + else_expr.as_deref().map_or(0, |value| binary_count(value, target))
        }
        ExprKind::Call { args, .. } => {
            args.iter().map(|arg| binary_count(arg, target)).sum()
        }
        ExprKind::Column { .. } | ExprKind::Literal(_) => 0,
    }
}
