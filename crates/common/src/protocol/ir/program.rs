//! MicroProgram and register identifier definitions for Micro-IR.

use serde::{Deserialize, Serialize};
use super::op::MicroOp;

/// Virtual register identifier indexing into the active DataChunk vector register file.
///
/// `u32`, per `vein_design/02-execution-and-vir-lld.md` §2. The earlier `u8`
/// imposed a 255-register ceiling, which capped the number of columns a query
/// could carry through the VM — a limit wide tables hit.
pub type RegId = u32;

/// A complete, self-contained vectorized program executed on in-flight DataChunks.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MicroProgram {
    /// Number of total registers required (input columns + intermediate scratch + projections).
    pub register_count: u32,
    /// The linear sequence of micro-instructions.
    pub instructions: Vec<MicroOp>,
}

impl MicroProgram {
    /// Creates a new MicroProgram with specified register allocation.
    pub fn new(register_count: u32, instructions: Vec<MicroOp>) -> Self {
        Self {
            register_count,
            instructions,
        }
    }

    /// Creates an empty MicroProgram.
    pub fn empty() -> Self {
        Self {
            register_count: 0,
            instructions: Vec::new(),
        }
    }
}
