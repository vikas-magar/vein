use crate::traits::{FunctionRegistry, FunctionSignature, SchemaProvider};
use crate::schema::{ColumnId, ResolvedColumn, VeinDataType, VeinSchema, VeinSchemaRef};
use async_trait::async_trait;
use error::VeinError;
use std::collections::HashMap;
use std::sync::Arc;

/// An in-memory catalog simulating the Vein VCS server for testing
pub struct InMemoryVcsCatalog {
    tables: HashMap<String, VeinSchemaRef>,
}

impl InMemoryVcsCatalog {
    /// Creates a new catalog populated with test tables (`users` and `orders`)
    pub fn new() -> Self {
        let mut tables = HashMap::new();

        // Table: users (id: Int64, name: Utf8, age: Int32)
        let users_schema = Arc::new(VeinSchema {
            fields: vec![
                ResolvedColumn {
                    id: ColumnId(0), // Temporary ID, assigned properly by ScopeStack
                    name: "id".to_string(),
                    qualifier: Some("users".to_string()),
                    data_type: VeinDataType::Int64,
                    is_factorized: false,
                },
                ResolvedColumn {
                    id: ColumnId(0),
                    name: "name".to_string(),
                    qualifier: Some("users".to_string()),
                    data_type: VeinDataType::Utf8,
                    is_factorized: false,
                },
                ResolvedColumn {
                    id: ColumnId(0),
                    name: "age".to_string(),
                    qualifier: Some("users".to_string()),
                    data_type: VeinDataType::Int32,
                    is_factorized: false,
                },
            ],
        });

        // Table: orders (id: Int64, user_id: Int64, amount: Float64)
        let orders_schema = Arc::new(VeinSchema {
            fields: vec![
                ResolvedColumn {
                    id: ColumnId(0),
                    name: "id".to_string(),
                    qualifier: Some("orders".to_string()),
                    data_type: VeinDataType::Int64,
                    is_factorized: false,
                },
                ResolvedColumn {
                    id: ColumnId(0),
                    name: "user_id".to_string(),
                    qualifier: Some("orders".to_string()),
                    data_type: VeinDataType::Int64,
                    is_factorized: false,
                },
                ResolvedColumn {
                    id: ColumnId(0),
                    name: "amount".to_string(),
                    qualifier: Some("orders".to_string()),
                    data_type: VeinDataType::Float64,
                    is_factorized: false,
                },
            ],
        });

        tables.insert("users".to_string(), users_schema);
        tables.insert("orders".to_string(), orders_schema);

        Self { tables }
    }
}

#[async_trait]
impl SchemaProvider for InMemoryVcsCatalog {
    async fn get_table_schema(&self, name: &str) -> Result<VeinSchemaRef, VeinError> {
        self.tables
            .get(name)
            .cloned()
            .ok_or_else(|| VeinError::internal(format!("Table '{}' not found in VCS Catalog", name)))
    }
}

/// A mock function registry for testing multi-runtime UDFs
pub struct MockFunctionRegistry;

impl FunctionRegistry for MockFunctionRegistry {
    fn resolve(&self, name: &str) -> Result<FunctionSignature, VeinError> {
        Err(VeinError::internal(format!("Function '{}' not found", name)))
    }
}
