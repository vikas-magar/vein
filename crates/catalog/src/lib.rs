#![deny(missing_docs)]

//! Format-agnostic catalog contracts and metadata infrastructure.
//!
//! This crate owns everything the engine knows about tables that is not tied to
//! a storage format:
//!
//! * [`metadata`] — the [`TableMetadata`] contract: schema, layout, physical
//!   format, partitioning, clustering, statistics, and properties. Every
//!   catalog, whatever it reads, describes a table with these types.
//! * [`store`] — a bounded, self-invalidating store of parsed file metadata that
//!   catalogs fill and execution reads. Payloads are erased behind
//!   [`std::any::Any`], so a Parquet footer, an Iceberg manifest, or a CSV
//!   header travels through the same handle without the engine learning the
//!   format.
//! * [`contract`] — the [`CatalogProvider`] trait the binder resolves tables
//!   through, together with the revision and planning-session types that pin one
//!   consistent metadata view per query.
//! * [`snapshot`] — the [`CatalogSnapshotToken`] a plan carries so execution can
//!   tell whether the metadata it was planned against still holds.
//!
//! Format implementations live *above* this crate: the local Parquet catalog is
//! in `cli` today and moves to a dedicated `catalog-parquet` crate, which will
//! consume [`store`] rather than teach the engine about Parquet.
//!
//! This crate is deliberately a leaf: it must not depend on `plan`, `exec`, or
//! any format reader, so that catalogs, planning, and execution can all share
//! it without a dependency cycle. Table metadata refers to columns by
//! [`types::ResolvedColumn`] and to constants by [`types::LogicalLiteral`]
//! rather than to planner expression trees.

/// Read-only catalog contracts used while binding and planning a query.
pub mod contract;
/// Standard table metadata, storage formats, sort order, and statistics.
pub mod metadata;
/// Catalog snapshot identity pinned by a query plan.
pub mod snapshot;
/// Bounded, self-invalidating store of parsed file metadata.
pub mod store;

pub use contract::{CatalogProvider, CatalogRevision, PlanningSession};
pub use metadata::{
    ColumnSortKey, ColumnStatistics, TableFormat, TableMetadata, TableMetadataRef, TableStatistics,
    TableType,
};
pub use snapshot::{ArchivedCatalogSnapshotToken, CatalogSnapshotToken};
pub use store::{DEFAULT_CAPACITY, MetadataKey, MetadataKind, MetadataStore};
