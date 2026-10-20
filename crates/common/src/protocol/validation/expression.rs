//! Schema validation for typed physical-expression trees.

use super::invalid;
use crate::protocol::{ExprKind, ExpressionProgram, ExpressionStep, PhysicalExpr};
use crate::Result;
use types::{literal_type, VeinDataType, VeinSchema};

/// Validates input ordinals and pipeline schema transitions without execution state.
pub(super) fn validate_program(
    program: &ExpressionProgram,
    input_schema: &VeinSchema,
    expected_output_width: Option<usize>,
    path: &str,
) -> Result<Option<usize>> {
    let mut columns = input_schema
        .fields
        .iter()
        .map(|field| field.data_type.clone())
        .collect::<Vec<_>>();
    let mut projected_width = None;

    for (index, step) in program.steps.iter().enumerate() {
        let step_path = format!("{path}.steps[{index}]");
        match step {
            ExpressionStep::Filter(predicate) => {
                validate_expression(predicate, &columns, &step_path)?;
                if predicate.return_type != VeinDataType::Boolean {
                    return Err(invalid(&step_path, "filter expression must return BOOLEAN"));
                }
            }
            ExpressionStep::Projection(expressions) => {
                let mut output = Vec::with_capacity(expressions.len());
                for (column, expression) in expressions.iter().enumerate() {
                    let expression_path = format!("{step_path}.projection[{column}]");
                    validate_expression(expression, &columns, &expression_path)?;
                    output.push(expression.return_type.clone());
                }
                projected_width = Some(output.len());
                columns = output;
            }
        }
    }

    let output_width = columns.len();
    if expected_output_width.is_some_and(|expected| expected != output_width) {
        return Err(invalid(
            path,
            format!(
                "expression pipeline emits {output_width} columns but node schema has {}",
                expected_output_width.unwrap_or_default()
            ),
        ));
    }
    Ok(projected_width)
}

fn validate_expression(
    expression: &PhysicalExpr,
    columns: &[VeinDataType],
    path: &str,
) -> Result<()> {
    let boolean = VeinDataType::Boolean;
    match &expression.kind {
        ExprKind::Column { index } => {
            let index_usize = usize::try_from(*index)
                .map_err(|_| invalid(path, "column index exceeds addressable memory"))?;
            let actual = columns
                .get(index_usize)
                .ok_or_else(|| invalid(path, format!("column {index} is outside schema width {}", columns.len())))?;
            if actual != &expression.return_type {
                return Err(invalid(
                    path,
                    format!("column {index} has type {actual:?}, expression declares {:?}", expression.return_type),
                ));
            }
        }
        ExprKind::Literal(value) => {
            if literal_type(value) != expression.return_type {
                return Err(invalid(path, "literal type does not match expression result type"));
            }
        }
        ExprKind::Binary { op, left, right } => {
            validate_child(left, columns, path, "left")?;
            validate_child(right, columns, path, "right")?;
            use crate::protocol::BinaryExprOp::*;
            let returns_boolean = matches!(
                op,
                Equal | NotEqual | Less | LessEqual | Greater | GreaterEqual | And | Or
            );
            if returns_boolean != (expression.return_type == boolean) {
                return Err(invalid(path, "binary expression result type is inconsistent with its operator"));
            }
        }
        ExprKind::Cast { expr, target_type } => {
            validate_child(expr, columns, path, "cast")?;
            if target_type != &expression.return_type {
                return Err(invalid(path, "cast result type differs from its target type"));
            }
        }
        ExprKind::Unary { op, expr } => {
            validate_child(expr, columns, path, "unary")?;
            use crate::protocol::UnaryExprOp::*;
            let expected = match op {
                Not | IsNull | IsNotNull => &boolean,
                Negate => &expr.return_type,
            };
            if expected != &expression.return_type {
                return Err(invalid(path, "unary expression result type is inconsistent with its operator"));
            }
        }
        ExprKind::InList { expr, candidates, .. } => {
            validate_child(expr, columns, path, "value")?;
            for (index, candidate) in candidates.iter().enumerate() {
                validate_child(candidate, columns, path, &format!("candidate[{index}]"))?;
            }
            require_boolean(expression, path)?;
        }
        ExprKind::Like { expr, pattern, .. } => {
            validate_child(expr, columns, path, "value")?;
            validate_child(pattern, columns, path, "pattern")?;
            require_boolean(expression, path)?;
        }
        ExprKind::Case { when_then, else_expr } => {
            for (index, (condition, value)) in when_then.iter().enumerate() {
                validate_child(condition, columns, path, &format!("when[{index}]"))?;
                if condition.return_type != boolean {
                    return Err(invalid(path, "CASE condition must return BOOLEAN"));
                }
                validate_child(value, columns, path, &format!("then[{index}]"))?;
            }
            if let Some(value) = else_expr {
                validate_child(value, columns, path, "else")?;
            }
        }
        ExprKind::Call { args, .. } => {
            for (index, argument) in args.iter().enumerate() {
                validate_child(argument, columns, path, &format!("argument[{index}]"))?;
            }
        }
    }
    Ok(())
}

fn validate_child(
    expression: &PhysicalExpr,
    columns: &[VeinDataType],
    path: &str,
    child: &str,
) -> Result<()> {
    validate_expression(expression, columns, &format!("{path}.{child}"))
}

fn require_boolean(expression: &PhysicalExpr, path: &str) -> Result<()> {
    if expression.return_type != VeinDataType::Boolean {
        return Err(invalid(path, "predicate expression must return BOOLEAN"));
    }
    Ok(())
}
