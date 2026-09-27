#![deny(missing_docs)]

//! The shared logical data model of the Vein engine.
//!
//! This crate is the engine-wide IR contract: the [`binder`] creates a
//! [`LogicalPlan`], the optimizer rewrites it, the physical planner
//! consumes it, and catalogs describe tables through [`VeinSchema`]s. It is
//! a leaf crate — it must never depend on the binder, optimizer, catalog, or
//! execution layers.

/// Logical expressions and literal values.
pub mod expr;

/// Logical plan nodes and tree traversal.
pub mod plan;

/// Data types, columns, and table schemas.
pub mod schema;

/// Standard table metadata, storage formats, and statistics.
pub mod metadata;

/// Read-only catalog contracts used during query planning.
pub mod catalog;

pub use expr::{LogicalExpr, LogicalLiteral, Volatility};
pub use catalog::{CatalogProvider, CatalogRevision, PlanningSession};
pub use metadata::{
    ColumnStatistics, TableFormat, TableMetadata, TableMetadataRef, TableStatistics, TableType,
};
pub use plan::display;
pub use plan::{LogicalPlan, PlanTraversal, SortKey, WindowExpr};
pub use schema::{ColumnId, ResolvedColumn, VeinDataType, VeinSchema, VeinSchemaRef};