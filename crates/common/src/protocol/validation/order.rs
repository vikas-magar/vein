use super::*;
use crate::protocol::SortPlan;
pub(super) fn validate(node: &PhysicalPlanNode, input: &PhysicalPlanNode, plan: &SortPlan, key_program: Option<&ExpressionProgram>, output_columns: u32, path: &str) -> Result<()> {
    validate_node(input, &format!("{path}.input"), false)?;
    let visible = usize::try_from(output_columns)
        .map_err(|_| invalid(path, "sort output width exceeds addressable memory"))?;
    if visible != input.output_schema.fields.len()
        || visible != node.output_schema.fields.len()
    {
        return Err(invalid(
            path,
            "sort visible output width must match its child and node schemas",
        ));
    }
    let key_width = if let Some(program) = key_program {
        validate_program(program, &input.output_schema, None, &format!("{path}.keys"))?
            .unwrap_or(input.output_schema.fields.len())
    } else {
        input.output_schema.fields.len()
    };
    for key in &plan.keys {
        check_column_index(key.column, key_width, path, "sort key")?;
    }
    Ok(())
}
