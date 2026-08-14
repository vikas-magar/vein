use error::{Result, VeinError};
use parser::ast::expr::{BinaryOp as AstBinaryOp, Expr, Literal as AstLiteral, SpannedExpr};
use crate::expr::{LogicalExpr, LogicalLiteral};
use crate::binder::context::BinderContext;
use crate::binder::scope::ScopeStack;
use crate::schema::data_type::VeinDataType;
use crate::traits::{FunctionKind, RuntimeKind};

/// Responsible for translating AST expressions into Logical Expressions
pub struct ExprBinder<'a, 'b> {
    context: &'a BinderContext<'b>,
    scopes: &'a mut ScopeStack<'b>,
}

impl<'a, 'b> ExprBinder<'a, 'b> {
    /// Creates a new ExprBinder using the current context and scope stack
    pub fn new(context: &'a BinderContext<'b>, scopes: &'a mut ScopeStack<'b>) -> Self {
        Self { context, scopes }
    }

    /// Recursively binds an AST expression into a type-aware LogicalExpr
    pub async fn bind(&mut self, ast: &SpannedExpr) -> Result<LogicalExpr> {
        match &ast.node {
            Expr::Column { table, column } => {
                let resolved = self.scopes.resolve(table.as_deref(), column)?;
                Ok(LogicalExpr::Column(resolved))
            }
            Expr::Literal(lit) => {
                let logical_lit = match lit {
                    AstLiteral::Boolean(b) => LogicalLiteral::Boolean(*b),
                    AstLiteral::Int(i) => LogicalLiteral::Int64(*i),
                    AstLiteral::Float(f) => LogicalLiteral::Float64(f.0),
                    AstLiteral::String(s) => LogicalLiteral::String(s.clone()),
                    AstLiteral::Null => LogicalLiteral::Null(VeinDataType::Null),
                };
                Ok(LogicalExpr::Literal(logical_lit))
            }
            Expr::Function { name, args, .. } => {
                // 1. Ask the FunctionRegistry what kind of function this is (No hardcoding!)
                let signature = self.context.functions.resolve(&name.to_string())?;
                
                // 2. Bind arguments recursively
                let mut bound_args = Vec::with_capacity(args.len());
                for arg in args {
                    // Because 'bind' is an async method on self, we can call it recursively 
                    // for each argument in the SpannedExpr vector!
                    let bound = Box::pin(self.bind(arg)).await?;
                    bound_args.push(bound);
                }

                match signature.kind {
                    FunctionKind::AiOperator => {
                        // FinOps Security Hook: Prevent accidental $50k bills
                        self.validate_ai_budget(name, &bound_args)?;
                        Ok(LogicalExpr::AiOperator {
                            name: name.clone(),
                            args: bound_args,
                            return_type: signature.return_type,
                        })
                    }
                    FunctionKind::UserDefined(runtime) => {
                        match runtime {
                            RuntimeKind::Wasm => Ok(LogicalExpr::WasmUdf {
                                bytecode_ref: signature.bytecode_ref,
                                args: bound_args,
                                return_type: signature.return_type,
                            }),
                            RuntimeKind::Ebpf => Ok(LogicalExpr::EbpfUdf {
                                bytecode_ref: signature.bytecode_ref,
                                args: bound_args,
                                return_type: signature.return_type,
                            }),
                            RuntimeKind::Python => Ok(LogicalExpr::PythonUdf {
                                bytecode_ref: signature.bytecode_ref,
                                args: bound_args,
                                return_type: signature.return_type,
                            }),
                        }
                    }
                    FunctionKind::Scalar | FunctionKind::Aggregate => {
                        Ok(LogicalExpr::FunctionCall {
                            name: name.clone(),
                            args: bound_args,
                            is_aggregate: signature.kind == FunctionKind::Aggregate,
                            return_type: signature.return_type,
                        })
                    }
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let bound_left = Box::pin(self.bind(left)).await?;
                let bound_right = Box::pin(self.bind(right)).await?;
                
                let left_type = bound_left.data_type();
                let right_type = bound_right.data_type();
                
                // Super basic type coercion (in a real engine this is much more complex)
                let return_type = match op {
                    AstBinaryOp::Eq | AstBinaryOp::Neq | AstBinaryOp::Lt | AstBinaryOp::Gt 
                    | AstBinaryOp::Leq | AstBinaryOp::Geq | AstBinaryOp::And | AstBinaryOp::Or => {
                        VeinDataType::Boolean
                    },
                    _ => {
                        // Math/Concat operators
                        if left_type == right_type {
                            left_type
                        } else {
                            // Needs cast, but for now just fallback to left_type
                            left_type 
                        }
                    }
                };

                Ok(LogicalExpr::BinaryOp {
                    left: Box::new(bound_left),
                    op: *op,
                    right: Box::new(bound_right),
                    return_type,
                })
            }
            Expr::UnaryOp { op, expr } => {
                let bound = Box::pin(self.bind(expr)).await?;
                use parser::ast::expr::UnaryOp as AstUnaryOp;
                match op {
                    AstUnaryOp::Not => Ok(LogicalExpr::Not { expr: Box::new(bound) }),
                    AstUnaryOp::Negate => Ok(LogicalExpr::Negate { expr: Box::new(bound) }),
                    AstUnaryOp::IsNull => Ok(LogicalExpr::IsNull { expr: Box::new(bound) }),
                    AstUnaryOp::IsNotNull => Ok(LogicalExpr::IsNotNull { expr: Box::new(bound) }),
                    AstUnaryOp::Plus => Ok(bound),
                }
            }
            _ => Err(VeinError::internal(format!("Expression binding not implemented for: {:?}", ast.node))),
        }
    }

    /// FinOps Guardrail: Validates that the estimated token cost of an AI Operator does not exceed the budget.
    fn validate_ai_budget(&self, op_name: &str, _args: &[LogicalExpr]) -> Result<()> {
        // In a real implementation, this would:
        // 1. Check self.scopes for the table's row count cardinality.
        // 2. Multiply by average prompt token size.
        // 3. Compare against a contextual MAX_QUERY_COST variable.
        
        let contextual_max_cost_usd = 10.00; // Hardcoded safety limit for now
        let estimated_cost_usd = 0.05; // Mock estimation

        if estimated_cost_usd > contextual_max_cost_usd {
            return Err(VeinError::internal(format!(
                "FinOps Fault: AI Operator '{}' estimated to cost ${:.2}, exceeding limit of ${:.2}",
                op_name, estimated_cost_usd, contextual_max_cost_usd
            )));
        }

        Ok(())
    }
}
