//! Micro-IR: Vectorized intermediate representation for Vein queries.

pub mod op;
pub mod program;
pub mod state;

pub use op::MicroOp;
pub use program::{MicroProgram, RegId};
pub use state::{AggOpCode, JoinKind, StateMode};
