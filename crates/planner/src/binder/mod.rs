use std::collections::HashMap;
use crate::plan::LogicalPlan;
use crate::traits::{SchemaProvider, FunctionRegistry};
use crate::schema::VeinSchemaRef;

/// Context and catalog resolution
pub mod context;
/// Scope management for column resolution
pub mod scope;
/// AST to Logical Expression binding
pub mod expr;
/// Pipe query clause binding
pub mod pipe;
/// The default SQL binder implementation
pub mod default_binder;
