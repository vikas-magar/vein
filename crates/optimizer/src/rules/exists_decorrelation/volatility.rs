use plan::{LogicalExpr, LogicalPlan, PlanTraversal};

/// Checks volatility through expression and subquery-plan boundaries.
///
/// `LogicalExpr::is_volatile` conservatively marks every subquery volatile.
/// Decorrelating EXISTS only needs to reject plans containing volatile work,
/// so inspect the nested plans while preserving the expression-level rules.
pub(super) fn contains_volatile_expression(expression: &LogicalExpr) -> bool {
    enum Pending<'a> {
        Expression(&'a LogicalExpr),
        Plan(&'a LogicalPlan),
    }

    let mut pending = vec![Pending::Expression(expression)];
    while let Some(node) = pending.pop() {
        match node {
            Pending::Expression(expression) => match expression {
                LogicalExpr::ScalarSubquery(plan) => pending.push(Pending::Plan(plan)),
                LogicalExpr::Exists { subquery, .. } => pending.push(Pending::Plan(subquery)),
                LogicalExpr::FunctionCall {
                    name,
                    is_aggregate,
                    distinct,
                    return_type,
                    ..
                } => {
                    let function = LogicalExpr::FunctionCall {
                        name: name.clone(),
                        args: Vec::new(),
                        is_aggregate: *is_aggregate,
                        distinct: *distinct,
                        return_type: return_type.clone(),
                    };
                    if function.is_volatile() {
                        return true;
                    }
                    pending.extend(expression.children().into_iter().map(Pending::Expression));
                }
                LogicalExpr::WindowCall { .. }
                | LogicalExpr::AiOperator { .. }
                | LogicalExpr::WasmUdf { .. }
                | LogicalExpr::EbpfUdf { .. }
                | LogicalExpr::PythonUdf { .. } => return true,
                _ => pending.extend(expression.children().into_iter().map(Pending::Expression)),
            },
            Pending::Plan(plan) => {
                let mut expressions = Vec::new();
                plan.for_each_expression(&mut |_, expression| expressions.push(expression));
                pending.extend(expressions.into_iter().map(Pending::Expression));
                pending.extend(plan.children().into_iter().map(Pending::Plan));
            }
        }
    }
    false
}
