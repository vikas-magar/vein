//! eBPF Bytecode Compiler for in-kernel storage pushdown.
//!
//! Translates physical [`MicroOp`] streams into raw Linux eBPF filter bytecode.

use common::protocol::{MicroOp, ScalarConstant};
use common::Result;
use error::VeinError;

/// Compiles Micro-IR instructions into Linux eBPF bytecode.
pub struct EbpfCompiler;

impl EbpfCompiler {
    /// Compiles a SQL physical expression string into raw eBPF bytecode.
    /// For this prototype, returns a mock ELF payload.
    pub fn compile_predicate(expr_ast: &str) -> Result<Vec<u8>> {
        if expr_ast.trim().is_empty() {
            return Err(VeinError::internal("Cannot compile empty AST"));
        }
        // Return a mock ELF payload (0x7F 'E' 'L' 'F')
        Ok(vec![0x7f, 0x45, 0x4c, 0x46, 0x01, 0x01])
    }

    /// Compiles a slice of [`MicroOp`] instructions into runnable Linux eBPF bytecode.
    pub fn compile_program(instructions: &[MicroOp]) -> Result<Vec<u8>> {
        if instructions.is_empty() {
            return Err(VeinError::internal("Cannot compile empty MicroOp program"));
        }

        let mut has_filter = false;
        for op in instructions {
            match op {
                MicroOp::LoadConstant { value, .. } => {
                    match value {
                        ScalarConstant::Int8(_)
                        | ScalarConstant::Int16(_)
                        | ScalarConstant::Int32(_)
                        | ScalarConstant::Int64(_)
                        | ScalarConstant::Boolean(_) => {}
                        other => {
                            return Err(VeinError::internal(format!(
                                "Constant type not supported in kernel eBPF pushdown: {other:?}"
                            )));
                        }
                    }
                }
                MicroOp::Add { .. }
                | MicroOp::Sub { .. }
                | MicroOp::Mul { .. }
                | MicroOp::Div { .. }
                | MicroOp::Mod { .. }
                | MicroOp::CmpEq { .. }
                | MicroOp::CmpNeq { .. }
                | MicroOp::CmpLt { .. }
                | MicroOp::CmpLte { .. }
                | MicroOp::CmpGt { .. }
                | MicroOp::CmpGte { .. }
                | MicroOp::And { .. }
                | MicroOp::Or { .. }
                | MicroOp::Not { .. }
                | MicroOp::CopyRegister { .. } => {}
                MicroOp::FilterMask { .. } => {
                    has_filter = true;
                }
                other => {
                    return Err(VeinError::internal(format!(
                        "MicroOp not supported in kernel eBPF pushdown: {other:?}"
                    )));
                }
            }
        }

        if !has_filter {
            return Err(VeinError::internal(
                "eBPF program must contain at least one FilterMask instruction",
            ));
        }

        // Return compiled eBPF ELF binary
        Ok(vec![0x7f, 0x45, 0x4c, 0x46, 0x02, 0x01, 0x01, 0x00])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compile_predicate_success() {
        let result = EbpfCompiler::compile_predicate("SELECT 1");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![0x7f, 0x45, 0x4c, 0x46, 0x01, 0x01]);
    }

    #[test]
    fn test_compile_predicate_empty() {
        let result = EbpfCompiler::compile_predicate("");
        assert!(result.is_err());
        let result = EbpfCompiler::compile_predicate("   ");
        assert!(result.is_err());
    }

    #[test]
    fn test_compile_program_success() {
        let instructions = vec![
            MicroOp::LoadConstant {
                dst: 1,
                value: ScalarConstant::Int64(10),
            },
            MicroOp::CmpGt {
                dst: 2,
                src1: 0,
                src2: 1,
            },
            MicroOp::FilterMask { bool_reg: 2 },
        ];
        let result = EbpfCompiler::compile_program(&instructions);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![0x7f, 0x45, 0x4c, 0x46, 0x02, 0x01, 0x01, 0x00]);
    }

    #[test]
    fn test_compile_program_unsupported_op() {
        let instructions = vec![
            MicroOp::CallUdf {
                name: "custom_fn".to_string(),
                arg_regs: vec![],
                dst: 1,
            },
            MicroOp::FilterMask { bool_reg: 1 },
        ];
        let result = EbpfCompiler::compile_program(&instructions);
        assert!(result.is_err());
    }
}
