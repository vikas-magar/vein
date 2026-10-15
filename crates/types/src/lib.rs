//! Dedicated types crate for the Vein database engine.
//!
//! Provides fundamental data types, columns, and schemas used across the entire engine,
//! with direct, zero-allocation bidirectional conversions to Apache Arrow and serialization support.

/// Column identities and resolved columns.
pub mod column;
/// Type-level coercion and common-supertype rules.
pub mod coercion;
/// Data types and arrow conversions.
pub mod data_type;
/// Literal values of the engine's data types.
pub mod literal;
/// Schemas and column layouts.
pub mod schema;
/// Temporal interval construction and unit conversion.
pub mod temporal;

pub use column::{ColumnId, ResolvedColumn, ResolvedColumnRef};
pub use coercion::unify_types;
pub use data_type::{HashJoinKeyType, VeinDataType, aggregate_return_type};
pub use literal::{LogicalLiteral, literal_type};
pub use schema::{VeinSchema, VeinSchemaRef};
pub use temporal::{TemporalUnit, days_from_civil, interval_from_unit, truncate_epoch_value};
pub use arrow::datatypes::{IntervalUnit, TimeUnit};
