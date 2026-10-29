use super::*;
use crate::protocol::AggregatePlan;
pub(super) fn aggregate(node: &PhysicalPlanNode, input: &PhysicalPlanNode, plan: &AggregatePlan, path: &str) -> Result<()> {
    validate_node(input, &format!("{path}.input"), false)?;
    let groups = usize::try_from(plan.groups)
        .map_err(|_| invalid(path, "aggregate group count exceeds addressable memory"))?;
    let input_width = groups
        .checked_add(plan.aggs.len())
        .ok_or_else(|| invalid(path, "aggregate input column count overflowed"))?;
    let output_width = groups
        .checked_add(plan.aggs.len())
        .ok_or_else(|| invalid(path, "aggregate output column count overflowed"))?;
    if input_width != input.output_schema.fields.len() {
        return Err(invalid(
            path,
            format!(
                "aggregate consumes {input_width} key/argument columns but child schema has {}",
                input.output_schema.fields.len()
            ),
        ));
    }
    if plan.aggregate_input_types.len() != plan.aggs.len()
        || plan.types.len() != output_width
        || node.output_schema.fields.len() != output_width
    {
        return Err(invalid(
            path,
            "aggregate operation, input type, output type, and schema widths disagree",
        ));
    }
    for (index, (declared, field)) in plan
        .types
        .iter()
        .zip(&node.output_schema.fields)
        .enumerate()
    {
        if declared != &field.data_type {
            return Err(invalid(
                path,
                format!(
                    "aggregate output type at column {index} is {declared:?}, schema declares {:?}",
                    field.data_type
                ),
            ));
        }
    }
    if let Some(grouping_sets) = &plan.grouping_sets {
        for (set_index, set) in grouping_sets.iter().enumerate() {
            for &column in set {
                check_column_index(
                    column,
                    groups,
                    path,
                    &format!("grouping set {set_index}"),
                )?;
            }
        }
    }
    Ok(())
}

pub(super) fn distinct(node: &PhysicalPlanNode, input: &PhysicalPlanNode, plan: &AggregatePlan, path: &str) -> Result<()> {
    validate_node(input, &format!("{path}.input"), false)?;
    let input_width = input.output_schema.fields.len();
    let groups = usize::try_from(plan.groups)
        .map_err(|_| invalid(path, "distinct group count exceeds addressable memory"))?;
    let expected_keys = (0..input_width)
        .map(|index| {
            u32::try_from(index)
                .map_err(|_| invalid(path, "distinct input has too many columns"))
        })
        .collect::<Result<Vec<_>>>()?;
    if groups != input_width
        || !plan.aggs.is_empty()
        || !plan.aggregate_input_types.is_empty()
        || plan.grouping_sets.as_ref() != Some(&vec![expected_keys])
        || plan.emit_empty_group_on_empty_input
    {
        return Err(invalid(
            path,
            "distinct must group every input column without aggregates or an empty-input group",
        ));
    }
    if plan.types.len() != input_width
        || plan
        .types
        .iter()
        .zip(&input.output_schema.fields)
        .any(|(data_type, field)| data_type != &field.data_type)
    {
        return Err(invalid(
            path,
            "distinct output types must match its input schema",
        ));
    }
    if plan
        .types
        .iter()
        .zip(&node.output_schema.fields)
        .any(|(data_type, field)| data_type != &field.data_type)
    {
        return Err(invalid(
            path,
            "distinct output types must match its output schema",
        ));
    }
    ensure_same_schema(&node.output_schema, &input.output_schema, path, "distinct")?;
    Ok(())
}
