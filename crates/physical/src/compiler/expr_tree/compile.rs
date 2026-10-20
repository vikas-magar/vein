use common::{Result, VeinError};
use common::protocol::{ExprKind, PhysicalExpr, UnaryExprOp};
use plan::expr::logical::LogicalExpr;
use plan::schema::{ResolvedColumnRef, VeinDataType};
use plan::expr::inference::literal_type;

use super::ExpressionCompiler;

impl ExpressionCompiler<'_> {
    pub(super) fn compile(&self, expression: &LogicalExpr) -> Result<PhysicalExpr> {
        let (kind, return_type) = match expression {
            LogicalExpr::Column(column) | LogicalExpr::OuterColumn(column) => {
                (ExprKind::Column { index: self.column_index(column)? },
                 column.data_type.clone())
            }
            LogicalExpr::Literal(value) => (
                ExprKind::Literal(value.clone()), literal_type(value),
            ),
            LogicalExpr::BinaryOp { left, op, right, return_type } => (
                ExprKind::Binary {
                    op: super::binary_op(*op),
                    left: Box::new(self.compile(left)?),
                    right: Box::new(self.compile(right)?),
                },
                return_type.clone(),
            ),
            LogicalExpr::Cast { expr, target_type } => (
                ExprKind::Cast {
                    expr: Box::new(self.compile(expr)?),
                    target_type: target_type.clone(),
                },
                target_type.clone(),
            ),
            LogicalExpr::Not { expr } => (
                ExprKind::Unary { op: UnaryExprOp::Not, expr: Box::new(self.compile(expr)?) },
                VeinDataType::Boolean,
            ),
            LogicalExpr::Negate { expr } => {
                let value = self.compile(expr)?;
                (ExprKind::Unary { op: UnaryExprOp::Negate, expr: Box::new(value.clone()) },
                 value.return_type)
            }
            LogicalExpr::IsNull { expr } | LogicalExpr::IsNotNull { expr } => {
                let (op, value) = match expression {
                    LogicalExpr::IsNull { expr } => (UnaryExprOp::IsNull, expr),
                    _ => (UnaryExprOp::IsNotNull, expr),
                };
                (ExprKind::Unary { op, expr: Box::new(self.compile(value)?) },
                 VeinDataType::Boolean)
            }
            LogicalExpr::InList { expr, list, negated } => (
                ExprKind::InList {
                    expr: Box::new(self.compile(expr)?),
                    candidates: list.iter().map(|item| self.compile(item))
                        .collect::<Result<Vec<_>>>()?,
                    negated: *negated,
                },
                VeinDataType::Boolean,
            ),
            LogicalExpr::Like { expr, pattern, negated } => (
                ExprKind::Like {
                    expr: Box::new(self.compile(expr)?),
                    pattern: Box::new(self.compile(pattern)?),
                    negated: *negated,
                },
                VeinDataType::Boolean,
            ),
            LogicalExpr::Case { when, then, else_, return_type } => {
                if when.len() != then.len() {
                    return Err(VeinError::planning("CASE arm counts differ", None));
                }
                let when_then = when.iter().zip(then).map(|(condition, value)| {
                    Ok((self.compile(condition)?, self.compile(value)?))
                }).collect::<Result<Vec<_>>>()?;
                (ExprKind::Case {
                    when_then,
                    else_expr: else_.as_ref().map(|value| self.compile(value).map(Box::new))
                        .transpose()?,
                }, return_type.clone())
            }
            LogicalExpr::FunctionCall { name, args, is_aggregate, return_type, .. } => {
                if *is_aggregate {
                    return Err(super::unsupported(expression));
                }
                (ExprKind::Call {
                    name: name.clone(),
                    args: args.iter().map(|arg| self.compile(arg))
                        .collect::<Result<Vec<_>>>()?,
                }, return_type.clone())
            }
            LogicalExpr::AiOperator { name, args, return_type } => (
                ExprKind::Call {
                    name: name.clone(),
                    args: args.iter().map(|arg| self.compile(arg))
                        .collect::<Result<Vec<_>>>()?,
                },
                return_type.clone(),
            ),
            _ => return Err(super::unsupported(expression)),
        };
        Ok(PhysicalExpr { return_type, kind })
    }

    fn column_index(&self, column: &ResolvedColumnRef) -> Result<u32> {
        if let Some(index) = self.columns.get(&column.id) {
            return Ok(*index);
        }
        let mut matches = self.schema.fields.iter().enumerate().filter(|(_, field)| {
            field.name == column.name && column.qualifier.as_deref()
                .is_none_or(|qualifier| field.qualifier.as_deref() == Some(qualifier))
        });
        let Some((index, _)) = matches.next() else {
            return Err(VeinError::planning(format!(
                "column {}#{} is absent from the physical input schema",
                column.name, column.id.0
            ), None));
        };
        if matches.next().is_some() {
            return Err(VeinError::planning(format!(
                "column {} is ambiguous in the physical input schema",
                column.name
            ), None));
        }
        u32::try_from(index).map_err(|_| VeinError::planning(
            "input schema has too many columns", None
        ))
    }
}
