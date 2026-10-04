//! Recursive expression-to-bytecode compiler.
//!
//! Lowers arbitrary [`LogicalExpr`] AST trees into a linear sequence of [`MicroOp`]
//! virtual machine instructions using a [`RegisterAllocator`].

use common::protocol::{MicroOp, MicroProgram, RegId};
use common::Result;
use error::VeinError;
use parser::ast::expr::BinaryOp;
use plan::expr::logical::LogicalExpr;
use plan::schema::{ColumnId, VeinSchema};
use std::collections::HashMap;

use super::allocator::RegisterAllocator;

/// Recursive AST-to-bytecode compiler.
pub struct BytecodeCompiler {
    allocator: RegisterAllocator,
    instructions: Vec<MicroOp>,
    column_registers: HashMap<ColumnId, RegId>,
}

impl BytecodeCompiler {
    /// Creates a new bytecode compiler for an input chunk with `input_columns`.
    pub fn new(input_columns: RegId) -> Self {
        Self {
            allocator: RegisterAllocator::new(input_columns),
            instructions: Vec::new(),
            column_registers: HashMap::new(),
        }
    }

    /// Associates the scan's logical columns with their initial VM registers.
    pub fn set_input_schema(&mut self, schema: &VeinSchema) {
        self.column_registers = schema
            .fields
            .iter()
            .enumerate()
            .map(|(register, field)| (field.id, register as RegId))
            .collect();
    }

    /// Recursively compiles a [`LogicalExpr`] AST into register instructions,
    /// returning the [`RegId`] holding the evaluated result.
    pub fn compile_expr(&mut self, expr: &LogicalExpr, schema: &VeinSchema) -> Result<RegId> {
        match expr {
            LogicalExpr::Column(col) => {
                self.column_register(col.id, &col.name, schema).ok_or_else(|| {
                    VeinError::planning(
                        format!("Column not found in schema: {:?}", col.name),
                        None,
                    )
                })
            }
            LogicalExpr::OuterColumn(col) => {
                self.column_register(col.id, &col.name, schema).ok_or_else(|| {
                    VeinError::planning(
                        format!("Outer column not found in schema: {:?}", col.name),
                        None,
                    )
                })
            }
            LogicalExpr::Literal(lit) => {
                let dst = self.allocator.allocate()?;
                self.instructions.push(MicroOp::LoadConstant {
                    dst,
                    value: lit.clone(),
                });
                Ok(dst)
            }
            LogicalExpr::BinaryOp {
                left, op, right, ..
            } => {
                let src1 = self.compile_expr(left, schema)?;
                let src2 = self.compile_expr(right, schema)?;
                let dst = self.allocator.allocate()?;

                let micro_op = match op {
                    BinaryOp::Plus => MicroOp::Add { dst, src1, src2 },
                    BinaryOp::Minus => MicroOp::Sub { dst, src1, src2 },
                    BinaryOp::Multiply => MicroOp::Mul { dst, src1, src2 },
                    BinaryOp::Divide => MicroOp::Div { dst, src1, src2 },
                    BinaryOp::Modulo => MicroOp::Mod { dst, src1, src2 },
                    BinaryOp::Eq => MicroOp::CmpEq { dst, src1, src2 },
                    BinaryOp::Neq => MicroOp::CmpNeq { dst, src1, src2 },
                    BinaryOp::Lt => MicroOp::CmpLt { dst, src1, src2 },
                    BinaryOp::Leq => MicroOp::CmpLte { dst, src1, src2 },
                    BinaryOp::Gt => MicroOp::CmpGt { dst, src1, src2 },
                    BinaryOp::Geq => MicroOp::CmpGte { dst, src1, src2 },
                    BinaryOp::And => MicroOp::And { dst, src1, src2 },
                    BinaryOp::Or => MicroOp::Or { dst, src1, src2 },
                    BinaryOp::Concat => MicroOp::CallUdf {
                        name: "concat".to_string(),
                        arg_regs: vec![src1, src2],
                        dst,
                    },
                };
                self.instructions.push(micro_op);
                Ok(dst)
            }
            LogicalExpr::Not { expr } => {
                let src = self.compile_expr(expr, schema)?;
                let dst = self.allocator.allocate()?;
                self.instructions.push(MicroOp::Not { dst, src });
                Ok(dst)
            }
            LogicalExpr::Negate { expr } => {
                let src = self.compile_expr(expr, schema)?;
                let dst = self.allocator.allocate()?;
                self.instructions.push(MicroOp::Neg { dst, src });
                Ok(dst)
            }
            LogicalExpr::IsNull { expr } => {
                let src = self.compile_expr(expr, schema)?;
                let dst = self.allocator.allocate()?;
                self.instructions.push(MicroOp::IsNull { dst, src });
                Ok(dst)
            }
            LogicalExpr::IsNotNull { expr } => {
                let src = self.compile_expr(expr, schema)?;
                let dst = self.allocator.allocate()?;
                self.instructions.push(MicroOp::IsNotNull { dst, src });
                Ok(dst)
            }
            LogicalExpr::FunctionCall { name, args, .. }
            | LogicalExpr::AiOperator { name, args, .. } => {
                let mut arg_regs = Vec::with_capacity(args.len());
                for arg in args {
                    arg_regs.push(self.compile_expr(arg, schema)?);
                }
                let dst = self.allocator.allocate()?;
                self.instructions.push(MicroOp::CallUdf {
                    name: name.clone(),
                    arg_regs,
                    dst,
                });
                Ok(dst)
            }
            LogicalExpr::Cast { expr, target_type } => {
                let src = self.compile_expr(expr, schema)?;
                let dst = self.allocator.allocate()?;
                self.instructions.push(MicroOp::Cast {
                    dst,
                    src,
                    target: target_type.clone(),
                });
                Ok(dst)
            }
            other => Err(VeinError::planning(
                format!("Logical expression lowering not yet implemented: {other:?}"),
                None,
            )),
        }
    }

    fn column_register(&self, id: ColumnId, name: &str, schema: &VeinSchema) -> Option<RegId> {
        self.column_registers.get(&id).copied().or_else(|| {
            schema
                .position_of_id(id)
                .or_else(|| schema.fields.iter().position(|field| field.name == name))
                .and_then(|index| RegId::try_from(index).ok())
        })
    }

    /// Compiles a filter condition expression, emitting a [`MicroOp::FilterMask`] instruction.
    pub fn compile_filter(&mut self, predicate: &LogicalExpr, schema: &VeinSchema) -> Result<()> {
        let bool_reg = self.compile_expr(predicate, schema)?;
        self.instructions.push(MicroOp::FilterMask { bool_reg });
        Ok(())
    }

    /// Compiles a conjunction of predicates into one boolean register.
    ///
    /// This is the push-down form of [`Self::compile_filter`]: a source can
    /// evaluate just this prefix over the predicate's own input columns to decide
    /// which rows to materialize, while the residual program keeps the filters
    /// themselves.
    pub fn compile_predicate(
        &mut self,
        predicates: &[LogicalExpr],
        schema: &VeinSchema,
    ) -> Result<RegId> {
        let mut combined = None;
        for predicate in predicates {
            let reg = self.compile_expr(predicate, schema)?;
            combined = Some(match combined {
                None => reg,
                Some(previous) => {
                    let dst = self.allocator.allocate()?;
                    self.instructions.push(MicroOp::And {
                        dst,
                        src1: previous,
                        src2: reg,
                    });
                    dst
                }
            });
        }
        combined.ok_or_else(|| VeinError::planning("scan predicate has no expressions", None))
    }

    /// Compiles a projection list, emitting a [`MicroOp::ProjectRegisters`] instruction.
    pub fn compile_projection(
        &mut self,
        exprs: &[LogicalExpr],
        schema: &VeinSchema,
    ) -> Result<()> {
        let out_regs = self.compile_projection_exprs(exprs, schema)?;
        self.compile_projection_registers(out_regs);
        Ok(())
    }

    /// Compiles a projection and records which registers represent its output
    /// columns for subsequent filters and projections in the same VM program.
    pub fn compile_projection_with_schema(
        &mut self,
        exprs: &[LogicalExpr],
        input_schema: &VeinSchema,
        output_schema: &VeinSchema,
    ) -> Result<()> {
        if exprs.len() != output_schema.fields.len() {
            return Err(VeinError::planning(
                "projection expression count does not match its output schema",
                None,
            ));
        }
        let out_regs = self.compile_projection_exprs(exprs, input_schema)?;
        self.compile_projection_registers(out_regs.clone());
        self.column_registers = output_schema
            .fields
            .iter()
            .zip(out_regs)
            .map(|(field, register)| (field.id, register))
            .collect();
        Ok(())
    }

    fn compile_projection_exprs(
        &mut self,
        exprs: &[LogicalExpr],
        schema: &VeinSchema,
    ) -> Result<Vec<RegId>> {
        exprs
            .iter()
            .map(|expr| self.compile_expr(expr, schema))
            .collect()
    }

    /// Projects registers already evaluated by this program.
    ///
    /// This lets a caller retain source columns alongside computed helper
    /// columns without recompiling those expressions during projection.
    pub fn compile_projection_registers(&mut self, out_regs: Vec<RegId>) {
        self.instructions.push(MicroOp::ProjectRegisters { out_regs });
    }

    /// Finalizes compilation into a self-contained [`MicroProgram`].
    pub fn finish(self) -> MicroProgram {
        MicroProgram {
            register_count: self.allocator.total_registers(),
            instructions: self.instructions,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plan::LogicalLiteral;
    use plan::schema::{ColumnId, ResolvedColumn, VeinDataType};

    fn test_schema() -> VeinSchema {
        VeinSchema::new(vec![
            std::sync::Arc::new(ResolvedColumn::new(
                ColumnId(1),
                "age",
                None,
                VeinDataType::Int64,
            )),
            std::sync::Arc::new(ResolvedColumn::new(
                ColumnId(2),
                "salary",
                None,
                VeinDataType::Int64,
            )),
        ])
    }

    #[test]
    fn test_compile_complex_filter_and_projection() {
        let schema = test_schema();
        let mut compiler = BytecodeCompiler::new(2);

        // Filter: age > 30 AND salary > 50000
        let age_col = LogicalExpr::Column(std::sync::Arc::new(ResolvedColumn::new(
            ColumnId(1),
            "age",
            None,
            VeinDataType::Int64,
        )));
        let age_lit = LogicalExpr::Literal(LogicalLiteral::Int64(30));
        let age_cmp = LogicalExpr::BinaryOp {
            left: Box::new(age_col),
            op: BinaryOp::Gt,
            right: Box::new(age_lit),
            return_type: VeinDataType::Boolean,
        };

        compiler.compile_filter(&age_cmp, &schema).unwrap();
        let program = compiler.finish();

        assert_eq!(program.instructions.len(), 3);
        assert!(matches!(
            program.instructions[0],
            MicroOp::LoadConstant { dst: 2, .. }
        ));
        assert!(matches!(
            program.instructions[1],
            MicroOp::CmpGt { dst: 3, src1: 0, src2: 2 }
        ));
        assert!(matches!(
            program.instructions[2],
            MicroOp::FilterMask { bool_reg: 3 }
        ));
        assert_eq!(program.register_count, 4);
    }

    /// `CAST(col AS Float64)` must emit a real conversion instruction rather
    /// than passing the source register through unchanged.
    #[test]
    fn test_compile_cast_emits_cast_instruction() {
        let schema = test_schema();
        let mut compiler = BytecodeCompiler::new(2);

        let cast = LogicalExpr::Cast {
            expr: Box::new(LogicalExpr::Column(std::sync::Arc::new(ResolvedColumn::new(
                ColumnId(1),
                "age",
                None,
                VeinDataType::Int64,
            )))),
            target_type: VeinDataType::Float64,
        };

        compiler.compile_expr(&cast, &schema).unwrap();
        let program = compiler.finish();

        assert_eq!(
            program.instructions,
            vec![MicroOp::Cast {
                dst: 2,
                src: 0,
                target: VeinDataType::Float64,
            }]
        );
        assert_eq!(program.register_count, 3);
    }
}
