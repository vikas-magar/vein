//! Recursive expression-to-bytecode compiler.
//!
//! Lowers arbitrary [`LogicalExpr`] AST trees into a linear sequence of [`MicroOp`]
//! virtual machine instructions using a [`RegisterAllocator`].

use common::Result;
use common::protocol::{MicroOp, MicroProgram, RegId};
use error::VeinError;
use parser::ast::expr::BinaryOp;
use plan::expr::logical::LogicalExpr;
use plan::schema::{ColumnId, ResolvedColumnRef, VeinSchema};
use std::collections::HashMap;

use super::allocator::RegisterAllocator;

/// Recursive AST-to-bytecode compiler.
pub struct BytecodeCompiler {
    allocator: RegisterAllocator,
    instructions: Vec<MicroOp>,
    column_registers: HashMap<ColumnId, RegId>,
    register_columns: Vec<(ResolvedColumnRef, RegId)>,
    input_schema_registered: bool,
}

impl BytecodeCompiler {
    /// Creates a new bytecode compiler for an input chunk with `input_columns`.
    pub fn new(input_columns: RegId) -> Self {
        Self {
            allocator: RegisterAllocator::new(input_columns),
            instructions: Vec::new(),
            column_registers: HashMap::new(),
            register_columns: Vec::new(),
            input_schema_registered: false,
        }
    }

    /// Associates the scan's logical columns with their initial VM registers.
    pub fn set_input_schema(&mut self, schema: &VeinSchema) {
        self.input_schema_registered = true;
        self.register_columns = schema
            .fields
            .iter()
            .enumerate()
            .map(|(register, field)| (std::sync::Arc::clone(field), register as RegId))
            .collect();
        self.column_registers = self
            .register_columns
            .iter()
            .map(|(field, register)| (field.id, *register))
            .collect();
    }

    /// Recursively compiles a [`LogicalExpr`] AST into register instructions,
    /// returning the [`RegId`] holding the evaluated result.
    pub fn compile_expr(&mut self, expr: &LogicalExpr, schema: &VeinSchema) -> Result<RegId> {
        match expr {
            LogicalExpr::Column(col) => self
                .column_register(col.id, &col.name, col.qualifier.as_deref(), schema)
                .ok_or_else(|| {
                    VeinError::planning(
                        missing_column_message("column", col.id, &col.name, schema),
                        None,
                    )
                }),
            LogicalExpr::OuterColumn(col) => self
                .column_register(col.id, &col.name, col.qualifier.as_deref(), schema)
                .ok_or_else(|| {
                    VeinError::planning(
                        missing_column_message("outer column", col.id, &col.name, schema),
                        None,
                    )
                }),
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
            LogicalExpr::Like {
                expr,
                pattern,
                negated,
            } => {
                let src = self.compile_expr(expr, schema)?;
                let pattern = self.compile_expr(pattern, schema)?;
                let dst = self.allocator.allocate()?;
                self.instructions.push(MicroOp::Like {
                    dst,
                    src,
                    pattern,
                    negated: *negated,
                });
                Ok(dst)
            }
            LogicalExpr::InList {
                expr,
                list,
                negated,
            } => {
                let value = self.compile_expr(expr, schema)?;
                let mut candidates = Vec::with_capacity(list.len());
                for candidate in list {
                    candidates.push(self.compile_expr(candidate, schema)?);
                }
                let dst = self.allocator.allocate()?;
                self.instructions.push(MicroOp::InList {
                    dst,
                    value,
                    candidates,
                    negated: *negated,
                });
                Ok(dst)
            }
            LogicalExpr::Case {
                when,
                then,
                else_,
                return_type,
            } => {
                let mut when_regs = Vec::with_capacity(when.len());
                let mut then_regs = Vec::with_capacity(then.len());
                for condition in when {
                    when_regs.push(self.compile_expr(condition, schema)?);
                }
                for result in then {
                    then_regs.push(self.compile_expr(result, schema)?);
                }
                let else_reg = else_
                    .as_ref()
                    .map(|result| self.compile_expr(result, schema))
                    .transpose()?;
                let dst = self.allocator.allocate()?;
                self.instructions.push(MicroOp::Case {
                    dst,
                    when_regs,
                    then_regs,
                    else_reg,
                    return_type: return_type.clone(),
                });
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
            unsupported => Err(VeinError::unsupported(
                format!(
                    "expression kind {} is not supported by VM bytecode lowering; route it through relational lowering",
                    unsupported.kind_name()
                ),
                None,
            )),
        }
    }

    fn column_register(
        &self,
        id: ColumnId,
        name: &str,
        qualifier: Option<&str>,
        schema: &VeinSchema,
    ) -> Option<RegId> {
        self.column_registers.get(&id).copied().or_else(|| {
            if self.input_schema_registered {
                // Optimizer rewrites can preserve a column's name and qualifier
                // while changing its ID. Resolve such columns against the actual
                // physical input layout; never use an ordinal from a wider logical
                // schema, which can point past an optimizer-pruned batch.
                let mut matches = self.register_columns.iter().filter(|(field, _)| {
                    field.name == name
                        && qualifier
                        .is_none_or(|qualifier| field.qualifier.as_deref() == Some(qualifier))
                });
                let (_, register) = matches.next()?;
                if matches.next().is_some() {
                    return None;
                }
                return Some(*register);
            }
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
    pub fn compile_projection(&mut self, exprs: &[LogicalExpr], schema: &VeinSchema) -> Result<()> {
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
        self.register_columns = output_schema
            .fields
            .iter()
            .zip(out_regs)
            .map(|(field, register)| (std::sync::Arc::clone(field), register))
            .collect();
        self.column_registers = self
            .register_columns
            .iter()
            .map(|(field, register)| (field.id, *register))
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
        self.instructions
            .push(MicroOp::ProjectRegisters { out_regs });
    }

    /// Finalizes compilation into a self-contained [`MicroProgram`].
    pub fn finish(self) -> MicroProgram {
        MicroProgram {
            register_count: self.allocator.total_registers(),
            instructions: self.instructions,
        }
    }
}

fn missing_column_message(kind: &str, id: ColumnId, name: &str, schema: &VeinSchema) -> String {
    let available = schema
        .fields
        .iter()
        .map(|field| format!("{}#{}", field.name, field.id.0))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "{kind} {name}#{} is not present in input schema [{available}]",
        id.0
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use plan::{LogicalLiteral, LogicalPlan};
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
    fn unsupported_subquery_error_does_not_dump_embedded_plan() {
        let expr = LogicalExpr::ScalarSubquery(Box::new(LogicalPlan::ExternalScan {
            url: "private_source_marker".to_string(),
            options: Vec::new(),
            files: Vec::new(),
            schema: Box::new(VeinSchema::empty()),
        }));
        let mut compiler = BytecodeCompiler::new(0);

        let error = compiler
            .compile_expr(&expr, &VeinSchema::empty())
            .expect_err("scalar subqueries require relational lowering");
        let message = error.to_string();

        assert!(message.contains("ScalarSubquery"), "{message}");
        assert!(message.contains("relational lowering"), "{message}");
        assert!(!message.contains("private_source_marker"), "{message}");
        assert_eq!(error.category(), error::ErrorCategory::Unsupported);
    }

    #[test]
    fn missing_column_error_names_column_and_concise_input_schema() {
        let schema = VeinSchema::new(vec![std::sync::Arc::new(ResolvedColumn::new(
            ColumnId(7),
            "available",
            None,
            VeinDataType::Int64,
        ))]);
        let missing = std::sync::Arc::new(ResolvedColumn::new(
            ColumnId(99),
            "missing_value",
            None,
            VeinDataType::Int64,
        ));
        let mut compiler = BytecodeCompiler::new(1);
        compiler.set_input_schema(&schema);

        let error = compiler
            .compile_expr(&LogicalExpr::Column(missing), &schema)
            .expect_err("a referenced column must exist in the input schema");

        assert_eq!(error.category(), error::ErrorCategory::Planning);
        assert_eq!(
            error.to_string(),
            "Planning error: column missing_value#99 is not present in input schema [available#7]"
        );
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
            MicroOp::CmpGt {
                dst: 3,
                src1: 0,
                src2: 2
            }
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
            expr: Box::new(LogicalExpr::Column(std::sync::Arc::new(
                ResolvedColumn::new(ColumnId(1), "age", None, VeinDataType::Int64),
            ))),
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

    #[test]
    fn compile_like_emits_vectorized_like_instruction() {
        let schema = VeinSchema::new(vec![std::sync::Arc::new(ResolvedColumn::new(
            ColumnId(1),
            "name",
            None,
            VeinDataType::Utf8,
        ))]);
        let expr = LogicalExpr::Like {
            expr: Box::new(LogicalExpr::Column(schema.fields[0].clone())),
            pattern: Box::new(LogicalExpr::Literal(LogicalLiteral::String(
                "%BRASS".to_string(),
            ))),
            negated: true,
        };
        let mut compiler = BytecodeCompiler::new(1);
        compiler.set_input_schema(&schema);

        assert_eq!(compiler.compile_expr(&expr, &schema).unwrap(), 2);
        assert!(matches!(
            compiler.finish().instructions.as_slice(),
            [
                MicroOp::LoadConstant { dst: 1, .. },
                MicroOp::Like {
                    dst: 2,
                    src: 0,
                    pattern: 1,
                    negated: true
                }
            ]
        ));
    }

    #[test]
    fn compile_in_list_emits_membership_instruction() {
        let schema = test_schema();
        let expr = LogicalExpr::InList {
            expr: Box::new(LogicalExpr::Column(schema.fields[0].clone())),
            list: vec![
                LogicalExpr::Literal(LogicalLiteral::Int64(10)),
                LogicalExpr::Literal(LogicalLiteral::Int64(20)),
            ],
            negated: true,
        };
        let mut compiler = BytecodeCompiler::new(schema.fields.len() as RegId);
        compiler.set_input_schema(&schema);

        let output = compiler.compile_expr(&expr, &schema).unwrap();
        let program = compiler.finish();
        assert_eq!(
            program.instructions.last(),
            Some(&MicroOp::InList {
                dst: output,
                value: 0,
                candidates: vec![2, 3],
                negated: true,
            })
        );
    }

    #[test]
    fn compile_case_emits_typed_case_instruction() {
        let schema = test_schema();
        let expr = LogicalExpr::Case {
            when: vec![LogicalExpr::Literal(LogicalLiteral::Boolean(true))],
            then: vec![LogicalExpr::Literal(LogicalLiteral::Int64(7))],
            else_: Some(Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(0)))),
            return_type: VeinDataType::Int64,
        };
        let mut compiler = BytecodeCompiler::new(schema.fields.len() as RegId);
        compiler.set_input_schema(&schema);

        let output = compiler.compile_expr(&expr, &schema).unwrap();
        let program = compiler.finish();
        assert_eq!(
            program.instructions.last(),
            Some(&MicroOp::Case {
                dst: output,
                when_regs: vec![2],
                then_regs: vec![3],
                else_reg: Some(4),
                return_type: VeinDataType::Int64,
            })
        );
    }

    #[test]
    fn registered_input_schema_prevents_logical_ordinal_fallback() {
        let logical_schema = test_schema();
        let physical_schema = VeinSchema::new(vec![
            std::sync::Arc::new(ResolvedColumn::new(
                ColumnId(2),
                "salary",
                None,
                VeinDataType::Int64,
            )),
            std::sync::Arc::new(ResolvedColumn::new(
                ColumnId(99),
                "age",
                None,
                VeinDataType::Int64,
            )),
        ]);
        let mut compiler = BytecodeCompiler::new(2);
        compiler.set_input_schema(&physical_schema);

        assert_eq!(
            compiler.column_register(ColumnId(2), "salary", None, &logical_schema),
            Some(0)
        );
        assert_eq!(
            compiler.column_register(ColumnId(1), "age", None, &logical_schema),
            Some(1)
        );
        assert_eq!(
            compiler.column_register(ColumnId(3), "missing", None, &logical_schema),
            None
        );
    }

    #[test]
    fn name_resolution_tracks_projected_register_layout() {
        let input_schema = test_schema();
        let projected_schema = VeinSchema::new(vec![
            std::sync::Arc::new(ResolvedColumn::new(
                ColumnId(90),
                "salary_alias",
                None,
                VeinDataType::Int64,
            )),
            std::sync::Arc::new(ResolvedColumn::new(
                ColumnId(91),
                "age_alias",
                None,
                VeinDataType::Int64,
            )),
        ]);
        let mut compiler = BytecodeCompiler::new(2);
        compiler.set_input_schema(&input_schema);
        compiler
            .compile_projection_with_schema(
                &[
                    LogicalExpr::Column(input_schema.fields[1].clone()),
                    LogicalExpr::Column(input_schema.fields[0].clone()),
                ],
                &input_schema,
                &projected_schema,
            )
            .expect("projection should compile");

        assert_eq!(
            compiler.column_register(ColumnId(900), "age_alias", None, &projected_schema),
            Some(0)
        );
        assert_eq!(
            compiler.column_register(ColumnId(901), "missing", None, &projected_schema),
            None
        );
    }
}
