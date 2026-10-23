//! Ordinal lineage from a join key through safe, row-preserving expression stages.
use common::protocol::{DataSource, ExprKind, ExpressionProgram, ExpressionStep, JoinPlan, PhysicalPlanNode, PhysicalPlanOperator};
use types::ColumnId;

/// Resolves source identities without relying on names or alias output identities.
pub(in crate::runtime) fn direct_scan_probe_columns<'a>(
    probe: &'a PhysicalPlanNode, plan: &JoinPlan,
) -> Option<(&'a DataSource, Vec<ColumnId>)> {
    if plan.probe_keys.is_empty() || plan.probe_keys.len() != plan.build_keys.len() { return None; }
    let mut keys = plan.probe_keys.clone();
    let mut types = Vec::with_capacity(keys.len());
    for (&probe_key, &build_key) in keys.iter().zip(&plan.build_keys) {
        let field = probe.output_schema.fields.get(probe_key as usize)?;
        let expected = plan.probe_types.get(probe_key as usize)?;
        if field.data_type != *expected || plan.build_types.get(build_key as usize)? != expected { return None; }
        types.push(expected);
    }
    let mut node = probe;
    loop {
        match &node.operator {
            PhysicalPlanOperator::Transform { input, program } => {
                remap(program, &mut keys)?;
                node = input;
            }
            PhysicalPlanOperator::Pipeline { source, input_schema, program } => {
                remap(program, &mut keys)?;
                let DataSource::File(scan) = source.as_ref() else { return None; };
                let mut columns = Vec::with_capacity(keys.len());
                for (key, expected) in keys.iter().zip(types) {
                    let column = input_schema.fields.get(*key as usize)?;
                    let source_column = scan.projected.fields.iter().find(|source| source.id == column.id)?;
                    if column.data_type != *expected || source_column.data_type != *expected { return None; }
                    columns.push(column.id);
                }
                return Some((source.as_ref(), columns));
            }
            _ => return None,
        }
    }
}

fn remap(program: &ExpressionProgram, keys: &mut [u32]) -> Option<()> {
    for step in program.steps.iter().rev() {
        match step {
            ExpressionStep::Filter(predicate) if predicate.is_reorder_safe() => {}
            ExpressionStep::Projection(expressions) if expressions.iter().all(|expr| expr.is_reorder_safe()) => {
                for key in keys.iter_mut() {
                    let ExprKind::Column { index } = expressions.get(*key as usize)?.kind else { return None; };
                    *key = index;
                }
            }
            _ => return None,
        }
    }
    Some(())
}

#[cfg(test)]
mod tests;
