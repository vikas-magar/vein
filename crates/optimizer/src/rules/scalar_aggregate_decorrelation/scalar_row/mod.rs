//! Scalar row decorrelation over deterministic, independent build relations.
//! Correlation predicates become the exact join condition; supported equalities
//! remain hash keys and other predicates use bounded reference-backed candidates.
use super::*;

pub(super) fn decorrelate(subquery: &LogicalPlan, outer: &VeinSchema) -> Option<Decorrelation> {
    let LogicalPlan::Project { input, exprs, schema } = subquery else { return None; };
    let [expression] = exprs.as_slice() else { return None; };
    let [output] = schema.fields.as_slice() else { return None; };
    if expression.data_type() != output.data_type || !is_local_scalar_input_expr(expression)
        || expression.is_volatile() || contains_missing_local_column(expression, input.schema())
    { return None; }

    let mut terms = Vec::new();
    let mut base = input.as_ref();
    while let LogicalPlan::Filter { input, predicate } = base {
        split_conjunction(predicate, &mut terms);
        base = input;
    }
    let mut right = base.clone();
    if let LogicalPlan::Scan { filters, .. } = &mut right {
        for filter in filters.iter() { split_conjunction(filter, &mut terms); }
        filters.clear();
    }
    if !independent(&right) { return None; }

    let mut correlations = Vec::new();
    let mut residual = Vec::new();
    let mut local = Vec::new();
    for term in terms {
        if term.is_volatile() || !term.subquery_plans().is_empty() { return None; }
        if !contains_outer_column(&term) {
            local.push(term);
            continue;
        }
        // Validate scope before removing OuterColumn markers, including inside
        // casts, arithmetic, disjunctions and NULL predicates.
        let lowered = localize(term.clone(), right.schema(), outer).ok()?;
        if let Some((inner, outer)) = equality_correlation(&term) {
            if !correlations.iter().any(|pair: &CorrelationPair|
                pair.inner.id == inner.id && pair.outer.id == outer.id) {
                correlations.push(CorrelationPair { inner, outer });
            }
        } else {
            residual.push(lowered);
        }
    }
    if correlations.is_empty() && residual.is_empty() { return None; }
    if let Some(predicate) = conjunction(local) {
        right = LogicalPlan::Filter { input: Box::new(right), predicate };
    }

    let mut fields = Vec::new();
    for pair in &correlations {
        if !fields.iter().any(|field: &ResolvedColumnRef| field.id == pair.inner.id) {
            fields.push(Arc::clone(&pair.inner));
        }
    }
    for field in &right.schema().fields {
        if residual.iter().any(|predicate| references_column(predicate, field.id))
            && !fields.iter().any(|existing| existing.id == field.id) {
            fields.push(Arc::clone(field));
        }
    }
    let mut projection = fields.iter().cloned().map(LogicalExpr::Column).collect::<Vec<_>>();
    let value = if let LogicalExpr::Column(column) = expression
        && fields.iter().any(|field| field.id == column.id) {
            nullable_column(column)
        } else {
            projection.push(expression.clone());
            fields.push(Arc::clone(output));
            nullable_column(output)
        };
    let right = LogicalPlan::Project {
        input: Box::new(right), exprs: projection, schema: Box::new(VeinSchema::new(fields)),
    };
    Some(Decorrelation {
        right, correlations, residual, join_kind: ScalarJoinKind::Single,
        value, aggregate_name: "SINGLE_ROW".into(), replacement: None,
    })
}

fn localize(expression: LogicalExpr, inner: &VeinSchema, outer: &VeinSchema) -> Result<LogicalExpr> {
    match expression {
        LogicalExpr::OuterColumn(column) if outer.position_of_id(column.id).is_some() =>
            Ok(LogicalExpr::Column(column)),
        LogicalExpr::Column(column) if inner.position_of_id(column.id).is_some() =>
            Ok(LogicalExpr::Column(column)),
        LogicalExpr::OuterColumn(_) | LogicalExpr::Column(_) =>
            Err(error::VeinError::planning("correlation references an unavailable scope", None)),
        expression => expression.map_children(&mut |child| localize(child, inner, outer)),
    }
}

fn independent(plan: &LogicalPlan) -> bool {
    if !matches!(plan, LogicalPlan::Scan { .. } | LogicalPlan::ExternalScan { .. }
        | LogicalPlan::Intermediate { .. } | LogicalPlan::Filter { .. } | LogicalPlan::Project { .. }
        | LogicalPlan::Aggregate { .. } | LogicalPlan::Distinct { .. } | LogicalPlan::Join { .. }
        | LogicalPlan::Sort { .. } | LogicalPlan::Limit { .. } | LogicalPlan::SubqueryAlias { .. }) {
        return false;
    }
    let mut safe = true;
    plan.for_each_expression(&mut |_, expression| {
        safe &= is_local_scalar_input_expr(expression) && !expression.is_volatile();
    });
    safe && plan.children().into_iter().all(independent)
}

#[cfg(test)]
mod tests;
