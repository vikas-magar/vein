//! The planner-facing catalog seam and its in-memory development catalog.
//!
//! The [`TableCatalog`] trait answers one question — *what does table `X`
//! look like and where is it stored?* — and decouples the binder and planner
//! from every concrete storage backend (Iceberg, VCS, object stores).
//! [`InMemoryCatalog`] is the development implementation backed by a table map,
//! used by the CLI and the binder test corpus.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use error::{Result, VeinError};
use plan::{
    ResolvedColumn, TableFormat, TableMetadata, VeinDataType, VeinSchema, VeinSchemaRef,
};

/// Type alias for [`TableMetadata`] representing an authoritative catalog table entry.
pub type TableEntry = TableMetadata;

/// The catalog contract consumed by the binder, optimizer, and planner: source of authoritative table metadata.
///
/// Implementations are expected to be cheap and read-mostly; the binder
/// issues one lookup per referenced table and caches nothing itself.
#[async_trait]
pub trait TableCatalog: Send + Sync {
    /// Returns the authoritative metadata of the named table.
    ///
    /// Fails with [`VeinError::NotFound`] when the table does not exist.
    async fn get_table(&self, name: &str) -> Result<Arc<TableMetadata>>;

    /// Returns the schema of the named table.
    ///
    /// Default implementation delegates to [`Self::get_table`].
    async fn get_table_schema(&self, name: &str) -> Result<VeinSchemaRef> {
        let table = self.get_table(name).await?;
        Ok(table.schema.clone())
    }

    /// Returns the physical storage location of the named table, if known.
    async fn get_table_location(&self, name: &str) -> Result<Option<String>> {
        let table = self.get_table(name).await?;
        Ok(table.location.clone())
    }

    /// Returns the physical storage format of the named table, if known.
    async fn get_table_format(&self, name: &str) -> Result<Option<String>> {
        let table = self.get_table(name).await?;
        Ok(table.format.as_ref().map(|f| f.to_string()))
    }

    /// Returns the physical files or partition URIs of the named table, if known.
    async fn get_table_files(&self, name: &str) -> Result<Vec<String>> {
        let table = self.get_table(name).await?;
        Ok(table.files.clone())
    }

    /// Registers authoritative table metadata in the catalog.
    async fn register_table(&self, metadata: TableMetadata) -> Result<()>;

    /// Registers a table with an explicit schema, physical storage location, and format.
    async fn register_table_entry(
        &self,
        name: &str,
        schema: VeinSchemaRef,
        location: Option<String>,
        format: Option<String>,
    ) -> Result<()> {
        let mut meta = TableMetadata::new(name, schema);
        if let Some(loc) = location {
            meta = meta.with_location(loc);
        }
        if let Some(fmt) = format {
            meta = meta.with_format(TableFormat::from(fmt.as_str()));
        }
        self.register_table(meta).await
    }
}

/// An in-memory [`TableCatalog`] for development and testing.
///
/// Ships with the standard development and TPC-H-flavored fixture corpus,
/// and accepts additional tables at runtime via [`add_table`](Self::add_table)
/// or [`register_table`](TableCatalog::register_table).
pub struct InMemoryCatalog {
    tables: RwLock<HashMap<String, Arc<TableMetadata>>>,
}

impl InMemoryCatalog {
    /// Creates a catalog populated with the standard development tables:
    /// `users`, `orders`, `lineitem`, `customer`, `nation`, `orders_tpch`.
    pub fn new() -> Self {
        let mut tables = HashMap::new();

        tables.insert(
            "users".to_string(),
            Arc::new(TableMetadata::new(
                "users",
                fixture_table(
                    "users",
                    &[
                        ("id", VeinDataType::Int64),
                        ("name", VeinDataType::Utf8),
                        ("age", VeinDataType::Int32),
                    ],
                ),
            )),
        );

        tables.insert(
            "orders".to_string(),
            Arc::new(TableMetadata::new(
                "orders",
                fixture_table(
                    "orders",
                    &[
                        ("id", VeinDataType::Int64),
                        ("user_id", VeinDataType::Int64),
                        ("amount", VeinDataType::Float64),
                    ],
                ),
            )),
        );

        // TPC-H-flavored fixtures used by the paper-style pipe corpus (Q1/Q3/Q5/Q6/Q13).
        tables.insert(
            "lineitem".to_string(),
            Arc::new(TableMetadata::new(
                "lineitem",
                fixture_table(
                    "lineitem",
                    &[
                        ("l_orderkey", VeinDataType::Int64),
                        ("l_partkey", VeinDataType::Int64),
                        ("l_suppkey", VeinDataType::Int64),
                        ("l_linenumber", VeinDataType::Int32),
                        ("l_quantity", VeinDataType::Int64),
                        ("l_extendedprice", VeinDataType::Float64),
                        ("l_discount", VeinDataType::Float64),
                        ("l_tax", VeinDataType::Float64),
                        ("l_returnflag", VeinDataType::Utf8),
                        ("l_linestatus", VeinDataType::Utf8),
                        ("l_shipdate", VeinDataType::Date32),
                        ("l_shipmode", VeinDataType::Utf8),
                    ],
                ),
            )),
        );

        tables.insert(
            "customer".to_string(),
            Arc::new(TableMetadata::new(
                "customer",
                fixture_table(
                    "customer",
                    &[
                        ("c_custkey", VeinDataType::Int64),
                        ("c_name", VeinDataType::Utf8),
                        ("c_nationkey", VeinDataType::Int64),
                        ("c_acctbal", VeinDataType::Float64),
                        ("c_mktsegment", VeinDataType::Utf8),
                    ],
                ),
            )),
        );

        tables.insert(
            "nation".to_string(),
            Arc::new(TableMetadata::new(
                "nation",
                fixture_table(
                    "nation",
                    &[
                        ("n_nationkey", VeinDataType::Int64),
                        ("n_name", VeinDataType::Utf8),
                        ("n_regionkey", VeinDataType::Int64),
                    ],
                ),
            )),
        );

        tables.insert(
            "orders_tpch".to_string(),
            Arc::new(TableMetadata::new(
                "orders_tpch",
                fixture_table(
                    "orders_tpch",
                    &[
                        ("o_orderkey", VeinDataType::Int64),
                        ("o_custkey", VeinDataType::Int64),
                        ("o_orderstatus", VeinDataType::Utf8),
                        ("o_totalprice", VeinDataType::Float64),
                        ("o_orderdate", VeinDataType::Date32),
                        ("o_orderpriority", VeinDataType::Utf8),
                    ],
                ),
            )),
        );

        Self {
            tables: RwLock::new(tables),
        }
    }

    /// Registers an additional table schema that the catalog does not know
    /// yet (e.g. a fixture created per-test).
    pub fn add_table(&self, name: impl Into<String>, schema: VeinSchemaRef) {
        let name_str = name.into();
        let metadata = Arc::new(TableMetadata::new(name_str.clone(), schema));
        if let Ok(mut tables) = self.tables.write() {
            tables.insert(name_str, metadata);
        }
    }
}

impl Default for InMemoryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TableCatalog for InMemoryCatalog {
    async fn get_table(&self, name: &str) -> Result<Arc<TableMetadata>> {
        let base_name = std::path::Path::new(name)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(name);

        if let Ok(tables) = self.tables.read() {
            if let Some(entry) = tables.get(name).or_else(|| tables.get(base_name)) {
                return Ok(entry.clone());
            }
            for entry in tables.values() {
                if let Some(loc) = &entry.location {
                    if loc == name || loc == base_name {
                        return Ok(entry.clone());
                    }
                }
            }
        }

        // Smart source discovery fallback
        if let Ok(discovered) = crate::discovery::resolve_source(name).await {
            let format: TableFormat = discovered.format.as_str().into();
            let metadata = Arc::new(
                TableMetadata::new(discovered.name, discovered.schema)
                    .with_location(discovered.location)
                    .with_format(format)
                    .with_files(discovered.files),
            );
            if let Ok(mut tables) = self.tables.write() {
                tables.insert(name.to_string(), metadata.clone());
                if !tables.contains_key(base_name) {
                    tables.insert(base_name.to_string(), metadata.clone());
                }
            }
            return Ok(metadata);
        }

        Err(VeinError::not_found(format!(
            "Table '{name}' is not registered in VCS Catalog"
        )))
    }

    async fn register_table(&self, metadata: TableMetadata) -> Result<()> {
        let mut tables = self
            .tables
            .write()
            .map_err(|e| VeinError::internal(format!("Failed to acquire catalog lock: {e}")))?;
        let table_name = metadata.name.clone();
        let schema_columns = metadata.schema.fields.len();
        tables.insert(table_name.clone(), Arc::new(metadata));
        tracing::info!(
            target: "vein.audit",
            event = "vcs_table_registered",
            table = %table_name,
            columns = schema_columns,
            "vcs_table_registered"
        );
        Ok(())
    }
}

/// Builds a non-factorized, non-nullable fixture column with a placeholder
/// id (the binder reassigns ids from its column counter on bind).
fn fixture_column(name: &str, qualifier: &str, data_type: VeinDataType) -> ResolvedColumn {
    ResolvedColumn::new(plan::ColumnId(0), name, Some(qualifier.to_string()), data_type)
}

/// Builds a table schema for a catalog fixture.
fn fixture_table(qualifier: &str, columns: &[(&str, VeinDataType)]) -> VeinSchemaRef {
    Arc::new(VeinSchema {
        fields: columns
            .iter()
            .map(|(name, ty)| fixture_column(name, qualifier, ty.clone()))
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_register_and_get_table() {
        let catalog = InMemoryCatalog::new();
        let schema = fixture_table(
            "custom_table",
            &[("id", VeinDataType::Int64), ("val", VeinDataType::Utf8)],
        );

        let meta = TableMetadata::new("custom_table", schema.clone())
            .with_location("vfs://custom_table")
            .with_format(TableFormat::Exo);

        catalog
            .register_table(meta)
            .await
            .expect("register should succeed");

        let fetched = catalog
            .get_table("custom_table")
            .await
            .expect("fetch should succeed");
        assert_eq!(fetched.schema.fields.len(), 2);
        assert_eq!(fetched.location, Some("vfs://custom_table".to_string()));
        assert_eq!(fetched.format, Some(TableFormat::Exo));
    }

    #[tokio::test]
    async fn test_register_table_entry_with_location_and_format() {
        let catalog = InMemoryCatalog::new();
        let schema = fixture_table(
            "custom_orders",
            &[
                ("id", VeinDataType::Int64),
                ("amount", VeinDataType::Float64),
            ],
        );

        catalog
            .register_table_entry(
                "custom_orders",
                schema.clone(),
                Some("vfs://custom_orders".to_string()),
                Some("exo".to_string()),
            )
            .await
            .expect("register should succeed");

        let fetched_schema = catalog
            .get_table_schema("custom_orders")
            .await
            .expect("fetch schema should succeed");
        assert_eq!(fetched_schema.fields.len(), 2);

        let loc = catalog
            .get_table_location("custom_orders")
            .await
            .expect("fetch location should succeed");
        assert_eq!(loc, Some("vfs://custom_orders".to_string()));

        let fmt = catalog
            .get_table_format("custom_orders")
            .await
            .expect("fetch format should succeed");
        assert_eq!(fmt, Some("exo".to_string()));

        // Lookup by location path should also resolve the table
        let by_loc = catalog
            .get_table("vfs://custom_orders")
            .await
            .expect("lookup by location path should succeed");
        assert_eq!(by_loc.schema.fields.len(), 2);
    }

    #[tokio::test]
    async fn test_unregistered_table_returns_not_found() {
        let catalog = InMemoryCatalog::new();
        let err = catalog
            .get_table_schema("non_existent_table")
            .await
            .expect_err("should fail");
        assert!(err.to_string().contains("is not registered in VCS Catalog"));
    }
}