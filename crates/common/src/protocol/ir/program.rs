//! MicroProgram and register identifier definitions for Micro-IR.

use serde::{Deserialize, Serialize};
use super::op::MicroOp;

/// Virtual register identifier indexing into the active DataChunk vector register file.
pub type RegId = u8;

/// A complete, self-contained vectorized program executed on in-flight DataChunks.
#[derive(
    Debug, Clone, PartialEq, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub struct MicroProgram {
    /// Number of total registers required (input columns + intermediate scratch + projections).
    pub register_count: u8,
    /// The linear sequence of micro-instructions.
    pub instructions: Vec<MicroOp>,
}

impl MicroProgram {
    /// Creates a new MicroProgram with specified register allocation.
    pub fn new(register_count: u8, instructions: Vec<MicroOp>) -> Self {
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
