use super::*;

pub(super) fn is_reorderable_join(plan: &LogicalPlan) -> bool {
    let LogicalPlan::Join {
        join_type,
        left,
        right,
        on: Some(on),
        using,
        ..
    } = plan
    else {
        return false;
    };
    if *join_type != JoinType::Inner || !using.is_empty() {
        return false;
    }
    let atoms = crate::rules::predicate_analysis::conjuncts(on);
    if atoms.iter().any(|expression| !expression.is_reorder_safe() || expression.is_volatile()) {
        return false;
    }
    atoms.iter().any(|expression| {
        if !matches!(expression, LogicalExpr::BinaryOp { op: BinaryOp::Eq, .. }) {
            return false;
        }
        let LogicalExpr::BinaryOp { left: a, right: b, .. } = expression else {
            return false;
        };
        let (LogicalExpr::Column(a), LogicalExpr::Column(b)) =
            (a.as_ref(), b.as_ref())
        else {
            return false;
        };
        if a.id == b.id {
            return false;
        }
        let left_a = left.schema().fields.iter().any(|column| column.id == a.id);
        let left_b = left.schema().fields.iter().any(|column| column.id == b.id);
        let right_a = right.schema().fields.iter().any(|column| column.id == a.id);
        let right_b = right.schema().fields.iter().any(|column| column.id == b.id);
        (left_a && !right_a && right_b && !left_b)
            || (left_b && !right_b && right_a && !left_a)
    })
}

#[cfg(test)]
pub(super) fn equi_atoms(expression: &LogicalExpr) -> Option<Vec<&LogicalExpr>> {
    let mut pending = vec![expression];
    let mut atoms = Vec::new();
    while let Some(expression) = pending.pop() {
        match expression {
            LogicalExpr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
                ..
            } => {
                pending.push(right);
                pending.push(left);
            }
            LogicalExpr::BinaryOp {
                left,
                op: BinaryOp::Eq,
                right,
                ..
            } if matches!(
                (left.as_ref(), right.as_ref()),
                (LogicalExpr::Column(_), LogicalExpr::Column(_))
            ) => atoms.push(expression),
            _ => return None,
        }
    }
    Some(atoms)
}
