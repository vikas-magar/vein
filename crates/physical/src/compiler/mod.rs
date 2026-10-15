//! The physical bytecode compiler and lowering engine.
//!
//! Replaces legacy filter/project mapping with a unified virtual machine compiler
//! translating logical AST expressions and plan operators into [`common::protocol::MicroProgram`]s.

pub mod allocator;
pub mod expr;
pub mod plan;
mod scan_stages;

pub use allocator::RegisterAllocator;
pub use expr::BytecodeCompiler;
pub use plan::PipelineCompiler;
