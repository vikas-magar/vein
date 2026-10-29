use super::*;
pub(super) fn validate_program(
    program: &ExpressionProgram,
    input_schema: &VeinSchema,
    expected_output_width: Option<usize>,
    path: &str,
) -> Result<Option<usize>> {
    expression_validation::validate_program(
        program,
        input_schema,
        expected_output_width,
        path,
    )
}

pub(super) fn check_column_index(index: u32, width: usize, path: &str, role: &str) -> Result<()> {
    if usize::try_from(index).map_or(true, |index| index >= width) {
        return Err(invalid(
            path,
            format!("{role} column {index} is outside schema width {width}"),
        ));
    }
    Ok(())
}

pub(super) fn ensure_same_schema(
    actual: &VeinSchema,
    expected: &VeinSchema,
    path: &str,
    operator: &str,
) -> Result<()> {
    if actual.fields.len() != expected.fields.len()
        || actual
        .fields
        .iter()
        .zip(&expected.fields)
        .any(|(actual, expected)| {
            actual.data_type != expected.data_type || actual.nullable != expected.nullable
        })
    {
        return Err(invalid(
            path,
            format!("{operator} schema does not match its input schema"),
        ));
    }
    Ok(())
}

pub(super) fn invalid(path: &str, message: impl std::fmt::Display) -> VeinError {
    VeinError::planning(format!("invalid physical plan at {path}: {message}"), None)
}
