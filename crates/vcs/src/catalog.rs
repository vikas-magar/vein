//! The planner-facing catalog seam and its in-memory development catalog.
//!
//! The [`TableCatalog`] trait answers one question — *what does table `X`
//! look like?* — and decouples the binder from every concrete storage
//! backend (Iceberg, VCS, object stores). [`InMemoryCatalog`] is the
//! development implementation backed by a simple table map, used by the CLI
//! and the binder test corpus.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use error::{Result, VeinError};
use plan::{ResolvedColumn, VeinDataType, VeinSchema, VeinSchemaRef};

/// The catalog contract consumed by the binder: source of table schemas.
///
/// Implementations are expected to be cheap and read-mostly; the binder
/// issues one lookup per referenced table and caches nothing itself.
#[async_trait]
pub trait TableCatalog: Send + Sync {
    /// Returns the schema of the named table.
    ///
    /// Fails with [`VeinError::NotFound`] when the table does not exist.
    async fn get_table_schema(&self, name: &str) -> Result<VeinSchemaRef>;
}

/// An in-memory [`TableCatalog`] for development and testing.
///
/// Ships with the TPC-H-flavored fixture corpus the planner test suite
/// uses, and accepts additional tables at runtime via [`add_table`](Self::add_table).
pub struct InMemoryCatalog {
    tables: HashMap<String, VeinSchemaRef>,
}

impl InMemoryCatalog {
    /// Creates a catalog populated with the standard development tables:
    /// `users`, `orders`, `lineitem`, `customer`, `nation`, `orders_tpch`.
    pub fn new() -> Self {
        let mut tables = HashMap::new();

        tables.insert(
            "users".to_string(),
            fixture_table(
                "users",
                &[
                    ("id", VeinDataType::Int64),
                    ("name", VeinDataType::Utf8),
                    ("age", VeinDataType::Int32),
                ],
            ),
        );

        tables.insert(
            "orders".to_string(),
            fixture_table(
                "orders",
                &[
                    ("id", VeinDataType::Int64),
                    ("user_id", VeinDataType::Int64),
                    ("amount", VeinDataType::Float64),
                ],
            ),
        );

        // TPC-H-flavored fixtures used by the paper-style pipe corpus (Q1/Q3/Q5/Q6/Q13).
        tables.insert(
            "lineitem".to_string(),
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
        );

        tables.insert(
            "customer".to_string(),
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
        );

        tables.insert(
            "nation".to_string(),
            fixture_table(
                "nation",
                &[
                    ("n_nationkey", VeinDataType::Int64),
                    ("n_name", VeinDataType::Utf8),
                    ("n_regionkey", VeinDataType::Int64),
                ],
            ),
        );

        tables.insert(
            "orders_tpch".to_string(),
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
        );

        Self { tables }
    }

    /// Registers an additional table schema that the catalog does not know
    /// yet (e.g. a fixture created per-test).
    pub fn add_table(&mut self, name: impl Into<String>, schema: VeinSchemaRef) {
        self.tables.insert(name.into(), schema);
    }
}

impl Default for InMemoryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TableCatalog for InMemoryCatalog {
    async fn get_table_schema(&self, name: &str) -> Result<VeinSchemaRef> {
        self.tables
            .get(name)
            .cloned()
            .ok_or_else(|| VeinError::not_found(format!("Table '{name}' not found in VCS Catalog")))
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