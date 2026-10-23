use super::*;

pub(super) fn rebuild(plan: LogicalPlan, prepared: PreparedJoinOrder) -> Result<LogicalPlan> {
    let root_schema = plan.schema().clone();
    let mut leaves = Vec::with_capacity(prepared.relation_count);
    take_leaf_plans(plan, &mut leaves);
    if leaves.len() != prepared.relation_count {
        return Err(VeinError::planning(
            "join-order reconstruction found a different relation count than analysis",
            None,
        ));
    }
    let full_mask = (1usize << prepared.relation_count) - 1;
    let mut leaves = leaves.into_iter().map(Some).collect::<Vec<_>>();
    build_subset(
        full_mask,
        full_mask,
        &prepared,
        &mut leaves,
        &root_schema,
    )
}

fn take_leaf_plans(plan: LogicalPlan, leaves: &mut Vec<LogicalPlan>) {
    if is_reorderable_join(&plan) {
        let LogicalPlan::Join { left, right, .. } = plan else {
            unreachable!("join eligibility checked the plan variant");
        };
        take_leaf_plans(*left, leaves);
        take_leaf_plans(*right, leaves);
    } else {
        leaves.push(plan);
    }
}

fn build_subset(
    mask: usize,
    full_mask: usize,
    prepared: &PreparedJoinOrder,
    leaves: &mut [Option<LogicalPlan>],
    root_schema: &VeinSchema,
) -> Result<LogicalPlan> {
    if mask.count_ones() == 1 {
        let index = mask.trailing_zeros() as usize;
        return leaves
            .get_mut(index)
            .and_then(Option::take)
            .ok_or_else(|| VeinError::planning("join-order leaf was already consumed", None));
    }
    let (left_mask, right_mask) = *prepared
        .splits
        .get(&mask)
        .ok_or_else(|| VeinError::planning("join-order split is missing", None))?;
    let left = build_subset(left_mask, full_mask, prepared, leaves, root_schema)?;
    let right = build_subset(right_mask, full_mask, prepared, leaves, root_schema)?;
    let predicates = prepared
        .predicates
        .iter()
        .filter(|predicate| predicates::crosses(predicate.relations, left_mask, right_mask))
        .map(|predicate| predicate.expression.clone())
        .collect::<Vec<_>>();
    let on = predicates
        .into_iter()
        .reduce(|left, right| LogicalExpr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::And,
            right: Box::new(right),
            return_type: VeinDataType::Boolean,
        })
        .ok_or_else(|| VeinError::planning("join-order split has no connecting predicate", None))?;
    let schema = if mask == full_mask {
        root_schema.clone()
    } else {
        VeinSchema::new(
            left.schema()
                .fields
                .iter()
                .chain(&right.schema().fields)
                .cloned()
                .collect(),
        )
    };
    Ok(LogicalPlan::Join {
        join_type: JoinType::Inner,
        left: Box::new(left),
        right: Box::new(right),
        on: Some(on),
        using: Vec::new(),
        schema: Box::new(schema),
    })
}
