//! The Vector VM execution engine module.
//!
//! Replaces legacy monolithic operator transforms with a register-based,
//! vector virtual machine interpreting [`common::protocol::MicroProgram`] instructions.

pub mod executor;
pub mod ops;
pub mod register;

pub use executor::VectorVmExecutor;
pub use register::RegisterFile;
