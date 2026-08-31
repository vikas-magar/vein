//! Expression translation abstractions: maps [`LogicalExpr`] trees to wire-level physical structures.

use common::protocol::{FilterPredicate, SortKey as WireSortKey};
use common::Result;
use error::VeinError;
use plan::expr::logical::LogicalExpr;
use plan::schema::VeinSchema;
use plan::SortKey as PlanSortKey;

/// Trait for translating logical expressions into physical, index-based wire structures.
pub trait PhysicalExprTranslator: Send + Sync {
    /// Translates a binary comparison `LogicalExpr` into a physical [`FilterPredicate`].
    fn translate_filter(&self, expr: &LogicalExpr, schema: &VeinSchema) -> Result<FilterPredicate>;

    /// Resolves a column reference to its 0-based index in the input schema fields.
    fn resolve_column_index(&self, expr: &LogicalExpr, schema: &VeinSchema) -> Result<i32>;

    /// Translates a projection list into a vector of physical column indices.
    fn translate_projection(&self, exprs: &[LogicalExpr], schema: &VeinSchema) -> Result<Vec<i32>>;

    /// Translates logical sort keys into physical sort keys with column indices.
    fn translate_sort_keys(
        &self,
        keys: &[PlanSortKey],
        schema: &VeinSchema,
    ) -> Result<Vec<WireSortKey>>;
}

/// Default expression translator abstraction.
#[derive(Debug, Default, Clone)]
pub struct DefaultExprTranslator;

impl DefaultExprTranslator {
    /// Creates a new default expression translator.
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalExprTranslator for DefaultExprTranslator {
    fn translate_filter(&self, _expr: &LogicalExpr, _schema: &VeinSchema) -> Result<FilterPredicate> {
        Err(VeinError::internal("DefaultExprTranslator::translate_filter is not yet implemented"))
    }

    fn resolve_column_index(&self, _expr: &LogicalExpr, _schema: &VeinSchema) -> Result<i32> {
        Err(VeinError::internal("DefaultExprTranslator::resolve_column_index is not yet implemented"))
    }

    fn translate_projection(&self, _exprs: &[LogicalExpr], _schema: &VeinSchema) -> Result<Vec<i32>> {
        Err(VeinError::internal("DefaultExprTranslator::translate_projection is not yet implemented"))
    }

    fn translate_sort_keys(
        &self,
        _keys: &[PlanSortKey],
        _schema: &VeinSchema,
    ) -> Result<Vec<WireSortKey>> {
        Err(VeinError::internal("DefaultExprTranslator::translate_sort_keys is not yet implemented"))
    }
}
