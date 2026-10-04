use crate::pipe::query::QueryBinder;
use crate::traits::{
    FunctionKind, FunctionRegistry, FunctionSignature, LogicalBinder,
};
use error::VeinError;
use parser::parser::Parser;
use plan::{CatalogProvider, LogicalPlan, MetadataStore, ResolvedColumn, TableMetadata, VeinDataType, VeinSchema, Volatility};
use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;

/// Minimal fixture catalog used by binder unit tests.
#[derive(Default)]
pub(crate) struct TestCatalog {
    tables: HashMap<String, Arc<TableMetadata>>,
}

impl TestCatalog {
    fn with_fixtures() -> Self {
        let mut catalog = Self::default();
        catalog.add_table("users", &[("id", VeinDataType::Int64), ("name", VeinDataType::Utf8), ("age", VeinDataType::Int64)]);
        catalog.add_table("orders", &[("id", VeinDataType::Int64), ("user_id", VeinDataType::Int64), ("amount", VeinDataType::Float64)]);
        catalog.add_table(
            "dates",
            &[
                ("order_date", VeinDataType::Date32),
                ("date64", VeinDataType::Date64),
                ("time_of_day", VeinDataType::Time64),
                ("timestamp", VeinDataType::Timestamp(None)),
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

/// Defines the built-in functions a fresh [`MockFunctionRegistry`] knows.
fn builtin(
    return_type: VeinDataType,
    arg_types: &[VeinDataType],
    kind: FunctionKind,
) -> FunctionSignature {
    FunctionSignature {
        return_type,
        volatility: Volatility::Immutable,
        arg_types: arg_types.to_vec(),
        kind,
        bytecode_ref: None,
    }
}

/// A mock function registry for testing: a registration-driven map of
/// functions, pre-populated with the built-ins the test corpus uses.
///
/// Functions resolve by name (case-insensitive) through the map; unknown
/// names yield [`VeinError::NotFound`].
pub struct MockFunctionRegistry {
    functions: HashMap<String, FunctionSignature>,
}

impl MockFunctionRegistry {
    /// Creates a registry pre-populated with the test-corpus built-ins
    /// (`SUM`, `COUNT`, `AVG`, `MIN`, `MAX`, `LOWER`, `ROUND`, temporal extractors,
    /// plus the AI/embedding mocks `EMBED` and `VECTOR_DISTANCE`).
    pub fn new() -> Self {
        let mut functions = HashMap::new();
        for (name, sig) in [
            (
                "SUM".to_string(),
                builtin(VeinDataType::Float64, &[], FunctionKind::Aggregate),
            ),
            (
                "COUNT".to_string(),
                builtin(VeinDataType::Int64, &[], FunctionKind::Aggregate),
            ),
            (
                "AVG".to_string(),
                builtin(VeinDataType::Float64, &[], FunctionKind::Aggregate),
            ),
            (
                "MIN".to_string(),
                builtin(VeinDataType::Float64, &[], FunctionKind::Aggregate),
            ),
            (
                "MAX".to_string(),
                builtin(VeinDataType::Float64, &[], FunctionKind::Aggregate),
            ),
            (
                "LOWER".to_string(),
                builtin(
                    VeinDataType::Utf8,
                    &[VeinDataType::Utf8],
                    FunctionKind::Scalar,
                ),
            ),
            (
                "ROUND".to_string(),
                builtin(
                    VeinDataType::Float64,
                    &[VeinDataType::Float64],
                    FunctionKind::Scalar,
                ),
            ),
            (
                "EMBED".to_string(),
                builtin(
                    VeinDataType::Vector(1536),
                    &[VeinDataType::Utf8],
                    FunctionKind::AiOperator,
                ),
            ),
            (
                "VECTOR_DISTANCE".to_string(),
                builtin(
                    VeinDataType::Float64,
                    &[VeinDataType::Vector(0), VeinDataType::Vector(0)],
                    FunctionKind::Scalar,
                ),
            ),
        ] {
            functions.insert(name, sig);
        }
        for name in crate::functions::TEMPORAL_EXTRACTORS {
            functions.insert(
                (*name).to_string(),
                builtin(VeinDataType::Int32, &[], FunctionKind::TemporalExtract),
            );
        }
        functions.insert(
            "SHIFT".to_string(),
            builtin(VeinDataType::Null, &[], FunctionKind::TemporalShift),
        );
        functions.insert(
            "TRUNCATE".to_string(),
            builtin(VeinDataType::Null, &[], FunctionKind::TemporalTruncate),
        );
        Self { functions }
    }

    /// Registers an additional function, overriding any existing entry.
    pub fn register(&mut self, name: impl Into<String>, signature: FunctionSignature) {
        self.functions.insert(name.into().to_uppercase(), signature);
    }
}

impl Default for MockFunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl FunctionRegistry for MockFunctionRegistry {
    fn resolve(&self, name: &str) -> Result<FunctionSignature, VeinError> {
        self.functions
            .get(&name.to_uppercase())
            .cloned()
            .ok_or_else(|| VeinError::not_found(format!("Function '{name}' not found in registry")))
    }
}

/// Binds a SQL string end-to-end: parse → bind with the in-memory catalog and
/// the pre-populated mock registry. Returns a fully-typed logical plan or a
/// typed `VeinError`; never panics on user input.
pub async fn plan(sql: &str) -> error::Result<LogicalPlan> {
    let node = Parser::new(sql)
        .parse_statement()
        .map_err(|e| VeinError::semantic(format!("parse error: {e}"), parse_error_span(&e)))?;

    QueryBinder::new(
        Arc::new(TestCatalog::with_fixtures()),
        Arc::new(MockFunctionRegistry::new()),
    )
        .bind(&node)
        .await
}

/// Extracts the source span of a parse error, when the variant carries one.
fn parse_error_span(err: &parser::parser::ParseError) -> Option<parser::token::Span> {
    use parser::parser::ParseError;
    match err {
        ParseError::Expected { span, .. }
        | ParseError::Unexpected { span, .. }
        | ParseError::NumberParse { span, .. } => Some(*span),
        ParseError::Lexical(_)
        | ParseError::Unsupported(_)
        | ParseError::RecursionLimitExceeded { .. } => None,
    }
}