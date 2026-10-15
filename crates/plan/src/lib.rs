#![deny(missing_docs)]

//! The shared logical data model of the Vein engine.
//!
//! This crate is the engine-wide IR contract: the [`binder`] creates a
//! [`LogicalPlan`], the optimizer rewrites it, the physical planner
//! consumes it, and catalogs describe tables through [`VeinSchema`]s. It is
//! a leaf crate — it must never depend on the binder, optimizer, catalog
//! implementations, or execution layers.

/// Logical expressions and literal values.
pub mod expr;

/// Logical plan nodes and tree traversal.
pub mod plan;

/// Data types, columns, and table schemas.
pub mod schema;

// Catalog contracts are defined in the `catalog` crate so that planning,
// execution, and catalog implementations share one definition without the
// planner's expression types leaking into the contract. They are re-exported
// here because this is where the binder and optimizer already reach for them.
pub use ::catalog::{
    CatalogProvider, CatalogRevision, CatalogSnapshotToken, ColumnGroupStatistics, ColumnSortKey,
    ColumnStatistics, HistogramBucket, HistogramSample, HistogramStatistics, MetadataStore,
    MostCommonValue, MostCommonValuesStatistics, PlanningSession, TableFormat, TableMetadata,
    TableMetadataRef, TableStatistics, TableType,
};
pub use expr::{LogicalExpr, LogicalLiteral, Volatility};
pub use plan::display;
pub use plan::{LogicalPlan, PlanTraversal, SortKey, WindowExpr};
pub use schema::{
    ColumnId, ResolvedColumn, ResolvedColumnRef, VeinDataType, VeinSchema, VeinSchemaRef,
};
