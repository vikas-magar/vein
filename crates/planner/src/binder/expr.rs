use arrow::datatypes::{DataType, Schema};
use error::{Result, VeinError};
use parser::ast::expr::{BinaryOp as AstBinaryOp, Expr, Literal as AstLiteral, SpannedExpr};

use crate::expr::{LogicalExpr, LogicalLiteral, ResolvedColumn};

/// Responsible for translating AST expressions into Logical Expressions
/// by resolving column names against an input schema and determining types.
pub struct ExprBinder<'a> {
    input_schema: &'a Schema,
}

impl<'a> ExprBinder<'a> {
    /// Creates a new ExprBinder bound to a specific input schema.
    pub fn new(input_schema: &'a Schema) -> Self {
        Self { input_schema }
    }

    /// Recursively binds an AST expression into a type-aware LogicalExpr.
    pub fn bind(&self, ast: &SpannedExpr) -> Result<LogicalExpr> {
        match &ast.node {
            Expr::Column { column, .. } => {
                let (index, field) = self
                    .input_schema
                    .column_with_name(column)
                    .ok_or_else(|| {
                        VeinError::internal(format!("Column '{}' not found in input schema", column))
                    })?;

                Ok(LogicalExpr::Column(ResolvedColumn {
                    name: column.clone(),
                    index,
                    data_type: field.data_type().clone(),
                }))
            }
            Expr::Literal(lit) => {
                let logical_lit = match lit {
                    AstLiteral::Boolean(b) => LogicalLiteral::Boolean(*b),
                    AstLiteral::Int(i) => LogicalLiteral::Int64(*i),
                    AstLiteral::Float(f) => LogicalLiteral::Float64(f.0),
                    AstLiteral::String(s) => LogicalLiteral::String(s.clone()),
                    AstLiteral::Null => LogicalLiteral::Null(DataType::Null),
                };
                Ok(LogicalExpr::Literal(logical_lit))
            }
            Expr::BinaryOp { left, op, right } => {
                let bound_left = self.bind(left)?;
                let bound_right = self.bind(right)?;

                let return_type = Self::coerce_binary_op(
                    &bound_left.data_type(),
                    op,
                    &bound_right.data_type(),
                )?;

                Ok(LogicalExpr::BinaryOp {
                    left: Box::new(bound_left),
                    op: *op,
                    right: Box::new(bound_right),
                    return_type,
                })
            }
            Expr::Cast { expr, data_type } => {
                let bound_expr = self.bind(expr)?;
                // For now, we assume parser::ast::DataType cleanly maps or is identical
                // In Vein, if AST DataType isn't arrow::DataType, we need a mapper.
                // Assuming we use a mapper from parser DataType to Arrow DataType:
                let target_arrow_type = Self::map_data_type(data_type)?;
                Ok(LogicalExpr::Cast {
                    expr: Box::new(bound_expr),
                    target_type: target_arrow_type,
                })
            }
            _ => Err(VeinError::internal(format!(
                "Expression binding not implemented for: {:?}",
                ast.node
            ))),
        }
    }

    /// Determines the return type of a binary operation based on its operands.
    /// In advanced engines (DataFusion/DuckDB), this logic is very complex and
    /// accounts for implicit casting (e.g. Int32 + Float64 -> Float64).
    fn coerce_binary_op(
        left_type: &DataType,
        op: &AstBinaryOp,
        right_type: &DataType,
    ) -> Result<DataType> {
        match op {
            // Logical and Comparison operators always return Boolean
            AstBinaryOp::Eq
            | AstBinaryOp::Neq
            | AstBinaryOp::Lt
            | AstBinaryOp::Gt
            | AstBinaryOp::Leq
            | AstBinaryOp::Geq
            | AstBinaryOp::And
            | AstBinaryOp::Or => Ok(DataType::Boolean),

            // Math operators return a coerced numeric type
            AstBinaryOp::Plus
            | AstBinaryOp::Minus
            | AstBinaryOp::Multiply
            | AstBinaryOp::Divide
            | AstBinaryOp::Modulo => {
                Self::coerce_numeric(left_type, right_type).ok_or_else(|| {
                    VeinError::internal(format!(
                        "Cannot apply math operator {:?} to types {:?} and {:?}",
                        op, left_type, right_type
                    ))
                })
            }
            
            AstBinaryOp::Concat => Ok(DataType::Utf8),
        }
    }

    /// Finds the widest numeric type that can safely hold both left and right.
    fn coerce_numeric(left: &DataType, right: &DataType) -> Option<DataType> {
        // Simplified coercion matrix.
        match (left, right) {
            (DataType::Float64, _) | (_, DataType::Float64) => Some(DataType::Float64),
            (DataType::Float32, _) | (_, DataType::Float32) => Some(DataType::Float32),
            (DataType::Int64, _) | (_, DataType::Int64) => Some(DataType::Int64),
            (DataType::Int32, _) | (_, DataType::Int32) => Some(DataType::Int32),
            _ => None,
        }
    }

    /// Maps AST data types to Arrow data types.
    fn map_data_type(ast_dt: &parser::ast::data_type::DataType) -> Result<DataType> {
        match ast_dt {
            parser::ast::data_type::DataType::Int => Ok(DataType::Int32),
            parser::ast::data_type::DataType::BigInt => Ok(DataType::Int64),
            parser::ast::data_type::DataType::Float => Ok(DataType::Float32),
            parser::ast::data_type::DataType::Double => Ok(DataType::Float64),
            parser::ast::data_type::DataType::Boolean => Ok(DataType::Boolean),
            parser::ast::data_type::DataType::String => Ok(DataType::Utf8),
            _ => Err(VeinError::internal(format!(
                "Cannot map AST DataType to Arrow DataType: {:?}",
                ast_dt
            ))),
        }
    }
}
