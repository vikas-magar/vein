use common::Result;
use error::VeinError;

pub struct EbpfCompiler;

impl EbpfCompiler {
    /// Compiles a SQL physical expression into raw eBPF bytecode.
    /// For this prototype, returns a mock ELF payload.
    pub fn compile_predicate(expr_ast: &str) -> Result<Vec<u8>> {
        if expr_ast.trim().is_empty() {
            return Err(VeinError::internal("Cannot compile empty AST"));
        }
        // Return a mock ELF payload (0x7F 'E' 'L' 'F')
        Ok(vec![0x7f, 0x45, 0x4c, 0x46, 0x01, 0x01])
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
}
