//! Dedicated types crate for the Vein database engine.
//!
//! Provides fundamental data types, columns, and schemas used across the entire engine,
//! with direct, zero-allocation bidirectional conversions to Apache Arrow and serialization support.

/// Column identities and resolved columns.
pub mod column;
/// Data types and arrow conversions.
pub mod data_type;
/// Literal values of the engine's data types.
pub mod literal;
/// Schemas and column layouts.
pub mod schema;

pub use column::{ColumnId, ResolvedColumn, ResolvedColumnRef};
pub use data_type::VeinDataType;
pub use literal::LogicalLiteral;
pub use schema::{VeinSchema, VeinSchemaRef};
