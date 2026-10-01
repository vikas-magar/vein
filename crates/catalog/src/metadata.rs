//! Standard table metadata contract shared by every catalog.
//!
//! `TableMetadata` is what catalogs hand to the binder, the cost-based
//! optimizer, and the physical planner: schema, storage layout, physical
//! format, partitioning, clustering/sort ordering, statistics, and extensible
//! properties. Nothing here knows about a specific catalog implementation, so a
//! local file catalog, Iceberg, and Delta all describe a table the same way.

use std::collections::HashMap;
use std::sync::Arc;

use types::{LogicalLiteral, ResolvedColumnRef, VeinSchemaRef};

use crate::snapshot::CatalogSnapshotToken;

/// The kind of table represented in the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum TableType {
    /// A persistent managed base table.
    #[default]
    Base,
    /// A virtual view defined by a query.
    View,
    /// A temporary session-scoped table.
    Temporary,
    /// An unmanaged external table pointing to external storage paths.
    External,
}

/// The physical storage format of a table.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum TableFormat {
    /// Apache Parquet columnar format.
    Parquet,
    /// Comma-separated values format.
    Csv,
    /// JSON text format.
    Json,
    /// Apache Arrow IPC / Feather streaming format.
    Arrow,
    /// Apache Iceberg open table format.
    Iceberg,
    /// Delta Lake open table format.
    Delta,
    /// Custom or external table format.
    Custom(String),
}

impl TableFormat {
    /// Returns the canonical string representation of this format.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Parquet => "parquet",
            Self::Csv => "csv",
            Self::Json => "json",
            Self::Arrow => "arrow",
            Self::Iceberg => "iceberg",
            Self::Delta => "delta",
            Self::Custom(s) => s.as_str(),
        }
    }
}

impl std::fmt::Display for TableFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for TableFormat {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "parquet" => Self::Parquet,
            "csv" => Self::Csv,
            "json" => Self::Json,
            "arrow" | "feather" => Self::Arrow,
            "iceberg" => Self::Iceberg,
            "delta" => Self::Delta,
            other => Self::Custom(other.to_string()),
        }
    }
}

impl From<String> for TableFormat {
    fn from(s: String) -> Self {
        TableFormat::from(s.as_str())
    }
}

/// Statistics for an individual column, used by the optimizer for selectivity estimation,
/// join ordering, and min/max data skipping (zone maps).
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct ColumnStatistics {
    /// Number of null values in this column.
    pub null_count: Option<u64>,
    /// Number of distinct values (NDV) in this column for join cardinality estimation.
    pub distinct_count: Option<u64>,
    /// Minimum scalar value in this column for min/max data skipping.
    pub min_value: Option<LogicalLiteral>,
    /// Maximum scalar value in this column for min/max data skipping.
    pub max_value: Option<LogicalLiteral>,
}

/// Aggregate statistics for a table, consumed by the Cost-Based Optimizer (CBO).
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct TableStatistics {
    /// Total estimated or exact row count.
    pub num_rows: Option<u64>,
    /// Total estimated or exact physical storage size in bytes.
    pub total_bytes: Option<u64>,
    /// Per-column statistics keyed by column name.
    pub column_statistics: HashMap<String, ColumnStatistics>,
}

/// One column of a table's physical sort or clustering order.
///
/// The order is recorded against resolved columns rather than planner
/// expressions: clustering is a property of the stored data, and keeping the
/// expression tree out of the contract lets a catalog describe layout without
/// depending on the planner.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ColumnSortKey {
    /// The sorted column.
    pub column: ResolvedColumnRef,
    /// Whether the column is sorted ascending (`true`) or descending (`false`).
    pub ascending: bool,
    /// Whether nulls sort before (`true`) or after (`false`) the non-null values.
    pub nulls_first: bool,
}

/// Authoritative metadata for a table in the catalog.
///
/// Encapsulates all relational, physical, partitioning, and statistical properties
/// needed for query analysis, logical optimization, and physical plan generation.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TableMetadata {
    /// The unique or qualified name of the table.
    pub name: String,
    /// The authoritative schema of the table.
    pub schema: VeinSchemaRef,
    /// The table kind (Base, View, Temporary, External).
    pub table_type: TableType,
    /// Storage root location or base URI (e.g. "vfs://input", "s3://bucket/path").
    pub location: Option<String>,
    /// Physical table format (e.g. `TableFormat::Exo`, `TableFormat::Parquet`).
    pub format: Option<TableFormat>,
    /// Explicit physical file paths or partition URIs to scan.
    pub files: Vec<String>,
    /// Partition column names used for partition pruning in the optimizer.
    pub partition_columns: Vec<String>,
    /// Physical sort or clustering order for sort elimination and streaming execution.
    pub sort_order: Vec<ColumnSortKey>,
    /// Statistical metadata for Cost-Based Optimization (CBO).
    pub statistics: Option<TableStatistics>,
    /// Catalog snapshot pinned for planning and execution.
    pub catalog_snapshot: Option<CatalogSnapshotToken>,
    /// Extensible key-value properties (e.g. compression codec, snapshot IDs, options).
    pub properties: HashMap<String, String>,
    /// Optional human-readable table description or comment.
    pub comment: Option<String>,
}

/// Shared reference to a [`TableMetadata`].
pub type TableMetadataRef = Arc<TableMetadata>;

impl TableMetadata {
    /// Creates a new `TableMetadata` with default properties for a given table name and schema.
    pub fn new(name: impl Into<String>, schema: VeinSchemaRef) -> Self {
        Self {
            name: name.into(),
            schema,
            table_type: TableType::Base,
            location: None,
            format: None,
            files: Vec::new(),
            partition_columns: Vec::new(),
            sort_order: Vec::new(),
            statistics: None,
            catalog_snapshot: None,
            properties: HashMap::new(),
            comment: None,
        }
    }

    /// Pins the table to a catalog snapshot for planning and execution.
    pub fn with_catalog_snapshot(mut self, snapshot: CatalogSnapshotToken) -> Self {
        self.catalog_snapshot = Some(snapshot);
        self
    }

    /// Sets the table type.
    pub fn with_table_type(mut self, table_type: TableType) -> Self {
        self.table_type = table_type;
        self
    }

    /// Sets the physical storage location.
    pub fn with_location(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
    }

    /// Sets the physical storage format.
    pub fn with_format(mut self, format: impl Into<TableFormat>) -> Self {
        self.format = Some(format.into());
        self
    }

    /// Sets the physical files or partitions.
    pub fn with_files(mut self, files: Vec<String>) -> Self {
        self.files = files;
        self
    }

    /// Sets the partition columns for partition pruning.
    pub fn with_partition_columns(mut self, columns: Vec<String>) -> Self {
        self.partition_columns = columns;
        self
    }

    /// Sets the physical sort order for sort elimination.
    pub fn with_sort_order(mut self, sort_order: Vec<ColumnSortKey>) -> Self {
        self.sort_order = sort_order;
        self
    }

    /// Sets table statistics for Cost-Based Optimization.
    pub fn with_statistics(mut self, statistics: TableStatistics) -> Self {
        self.statistics = Some(statistics);
        self
    }

    /// Adds a key-value property.
    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Sets the table comment.
    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }
}
