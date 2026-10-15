//! In-memory catalog fixtures used by binder tests.

use async_trait::async_trait;
use error::VeinError;
use plan::{CatalogProvider, MetadataStore, ResolvedColumn, TableMetadata, VeinDataType, VeinSchema};
use std::collections::HashMap;
use std::sync::Arc;

/// Minimal fixture catalog used by binder unit tests.
#[derive(Default)]
pub(crate) struct TestCatalog {
    tables: HashMap<String, Arc<TableMetadata>>,
}

impl TestCatalog {
    pub(super) fn with_fixtures() -> Self {
        let mut catalog = Self::default();
        catalog.add_table("users", &[("id", VeinDataType::Int64), ("name", VeinDataType::Utf8), ("age", VeinDataType::Int64)]);
        catalog.add_table("orders", &[("id", VeinDataType::Int64), ("user_id", VeinDataType::Int64), ("amount", VeinDataType::Float64)]);
        catalog.add_table(
            "dates",
            &[
                ("order_date", VeinDataType::Date32),
                ("date64", VeinDataType::Date64),
                ("time_of_day", VeinDataType::Time64(types::TimeUnit::Microsecond)),
                ("timestamp", VeinDataType::Timestamp(types::TimeUnit::Microsecond, None)),
            ],
        );
        catalog.add_table(
            "vector_values",
            &[
                ("left_vector", VeinDataType::Vector(3)),
                ("right_vector", VeinDataType::Vector(3)),
            ],
        );
        catalog.add_table(
            "decimal_values",
            &[("amount", VeinDataType::Decimal128(15, 2))],
        );
        catalog.add_table(
            "numeric_values",
            &[
                ("i8_value", VeinDataType::Int8),
                ("i16_value", VeinDataType::Int16),
                ("i32_value", VeinDataType::Int32),
                ("u8_value", VeinDataType::UInt8),
                ("u16_value", VeinDataType::UInt16),
                ("u32_value", VeinDataType::UInt32),
                ("u64_value", VeinDataType::UInt64),
                ("f16_value", VeinDataType::Float16),
                ("f32_value", VeinDataType::Float32),
                ("decimal256_value", VeinDataType::Decimal256(20, 3)),
            ],
        );
        catalog
    }

    fn add_table(&mut self, name: &str, columns: &[(&str, VeinDataType)]) {
        let schema = VeinSchema::new(columns.iter().enumerate().map(|(id, (column, data_type))| {
            std::sync::Arc::new(ResolvedColumn::new(
                plan::ColumnId(id),
                *column,
                Some(name.to_owned()),
                data_type.clone(),
            ))
        }).collect());
        self.tables.insert(name.to_owned(), Arc::new(TableMetadata::new(name, Arc::new(schema))));
    }
}

#[async_trait]
impl CatalogProvider for TestCatalog {
    async fn get_table(&self, name: &str) -> error::Result<Arc<TableMetadata>> {
        self.tables.get(name).cloned().ok_or_else(|| {
            VeinError::not_found(format!("table '{name}' is not registered in test catalog"))
        })
    }

    fn metadata_store(&self) -> MetadataStore {
        MetadataStore::new()
    }
}

