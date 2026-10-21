//! Function definition and signature types.

mod definition;
mod implementation;
mod kind;
mod signature;

pub use definition::FunctionDefinition;
pub use implementation::FunctionImplementation;
pub use kind::{FunctionKind, RuntimeKind};
pub use signature::{FunctionSignature, Volatility};
