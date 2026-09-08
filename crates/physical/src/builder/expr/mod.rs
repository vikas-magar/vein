use std::collections::HashMap;

use common::protocol::{
    FilterOp, FilterPredicate, FilterValue, SortKey as WireSortKey,
};
use common::Result;
use error::VeinError;
use parser::ast::expr::BinaryOp;
use plan::expr::literal::LogicalLiteral;
use plan::expr::logical::LogicalExpr;
use plan::schema::VeinSchema;
use plan::ColumnId;
use plan::SortKey as PlanSortKey;

/// Trait for translating logical expressions into physical, index-based wire structures.
pub trait PhysicalExprTranslator: Send + Sync {
    /// Translates a binary comparison `LogicalExpr` into a physical [`FilterPredicate`].
    fn translate_filter(&self, expr: &LogicalExpr, schema: &VeinSchema) -> Result<FilterPredicate>;

    /// Resolves a column reference to its 0-based index in the input schema fields.
    fn map_column_to_physical_index(&self, expr: &LogicalExpr, schema: &VeinSchema) -> Result<i32>;

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
    fn map_column_to_physical_index(&self, expr: &LogicalExpr, schema: &VeinSchema) -> Result<i32> {
        match expr {
            LogicalExpr::Column(col) => schema
                .position_of_id(col.id)
                .map(|idx| idx as i32)
                .ok_or_else(|| {
                    VeinError::planning(
                        format!(
                            "Column '{}' (id: {:?}) not found in schema fields: {:?}",
                            col.name,
                            col.id,
                            schema
                                .fields
                                .iter()
                                .map(|f| (&f.name, f.id))
                                .collect::<Vec<_>>()
                        ),
                        None,
                    )
                }),
            other => Err(VeinError::planning(
                format!("Expected column reference for index resolution, found: {other:?}"),
                None,
            )),
        }
    }

    fn translate_filter(&self, expr: &LogicalExpr, schema: &VeinSchema) -> Result<FilterPredicate> {
        match expr {
            LogicalExpr::BinaryOp { left, op, right, .. } => {
                // Case 1: Column on left, Literal on right (e.g. `age > 20`)
                if let (LogicalExpr::Column(_), LogicalExpr::Literal(lit)) =
                    (left.as_ref(), right.as_ref())
                {
                    let col_idx = self.map_column_to_physical_index(left, schema)?;
                    let filter_op = lower_binary_op(*op)?;
                    let filter_val = lower_literal(lit)?;
                    return Ok(FilterPredicate {
                        column: col_idx,
                        op: filter_op,
                        literal: filter_val,
                    });
                }

                // Case 2: Literal on left, Column on right (e.g. `20 < age` -> `age > 20`)
                if let (LogicalExpr::Literal(lit), LogicalExpr::Column(_)) =
                    (left.as_ref(), right.as_ref())
                {
                    let col_idx = self.map_column_to_physical_index(right, schema)?;
                    let filter_op = flip_binary_op(lower_binary_op(*op)?);
                    let filter_val = lower_literal(lit)?;
                    return Ok(FilterPredicate {
                        column: col_idx,
                        op: filter_op,
                        literal: filter_val,
                    });
                }

                Err(VeinError::planning(
                    format!(
                        "Filter predicate must be a comparison between column and literal, got: {expr:?}"
                    ),
                    None,
                ))
            }
            other => Err(VeinError::planning(
                format!("Unsupported filter expression: {other:?}"),
                None,
            )),
        }
    }

    fn translate_projection(&self, exprs: &[LogicalExpr], schema: &VeinSchema) -> Result<Vec<i32>> {
        // Schemas with 16 or fewer columns fit directly in CPU L1 data cache.
        const SMALL_SCHEMA_THRESHOLD: usize = 16;

        if schema.fields.len() <= SMALL_SCHEMA_THRESHOLD {
            // Fast path for narrow schemas: zero heap allocation, cache-friendly
            let mut columns = Vec::with_capacity(exprs.len());
            for expr in exprs {
                let idx = self.map_column_to_physical_index(expr, schema)?;
                columns.push(idx);
            }
            Ok(columns)
        } else {
            // Fast path for wide tables: pre-index the schema ONCE in O(N)
            let index_map: HashMap<ColumnId, usize> = schema
                .fields
                .iter()
                .enumerate()
                .map(|(idx, field)| (field.id, idx))
                .collect();

            let mut columns = Vec::with_capacity(exprs.len());
            for expr in exprs {
                match expr {
                    LogicalExpr::Column(col) => {
                        let idx = index_map.get(&col.id).copied().ok_or_else(|| {
                            VeinError::planning(
                                format!(
                                    "Column '{}' (id: {:?}) not found in schema fields: {:?}",
                                    col.name,
                                    col.id,
                                    schema
                                        .fields
                                        .iter()
                                        .map(|f| (&f.name, f.id))
                                        .collect::<Vec<_>>()
                                ),
                                None,
                            )
                        })?;
                        columns.push(idx as i32);
                    }
                    other => {
                        return Err(VeinError::planning(
                            format!(
                                "Expected column reference for physical index mapping, found: {other:?}"
                            ),
                            None,
                        ));
                    }
                }
            }
            Ok(columns)
        }
    }

    fn translate_sort_keys(
        &self,
        keys: &[PlanSortKey],
        schema: &VeinSchema,
    ) -> Result<Vec<WireSortKey>> {
        let mut wire_keys = Vec::with_capacity(keys.len());
        for k in keys {
            let col_idx = self.map_column_to_physical_index(&k.expr, schema)?;
            wire_keys.push(WireSortKey {
                column: col_idx,
                descending: !k.asc,
            });
        }
        Ok(wire_keys)
    }
}

fn lower_binary_op(op: BinaryOp) -> Result<FilterOp> {
    match op {
        BinaryOp::Eq => Ok(FilterOp::Eq),
        BinaryOp::Neq => Ok(FilterOp::Neq),
        BinaryOp::Lt => Ok(FilterOp::Lt),
        BinaryOp::Gt => Ok(FilterOp::Gt),
        BinaryOp::Leq => Ok(FilterOp::Leq),
        BinaryOp::Geq => Ok(FilterOp::Geq),
        other => Err(VeinError::planning(
            format!("Unsupported binary operator in filter predicate: {other:?}"),
            None,
        )),
    }
}

fn flip_binary_op(op: FilterOp) -> FilterOp {
    match op {
        FilterOp::Eq => FilterOp::Eq,
        FilterOp::Neq => FilterOp::Neq,
        FilterOp::Lt => FilterOp::Gt,
        FilterOp::Gt => FilterOp::Lt,
        FilterOp::Leq => FilterOp::Geq,
        FilterOp::Geq => FilterOp::Leq,
    }
}

fn lower_literal(lit: &LogicalLiteral) -> Result<FilterValue> {
    match lit {
        LogicalLiteral::Boolean(b) => Ok(FilterValue::Boolean(*b)),
        LogicalLiteral::Int64(i) => Ok(FilterValue::Int(*i)),
        LogicalLiteral::Float64(f) => Ok(FilterValue::Float(*f)),
        LogicalLiteral::String(s) => Ok(FilterValue::String(s.clone())),
        LogicalLiteral::Date(d) => Ok(FilterValue::Int(*d as i64)),
        LogicalLiteral::Timestamp(ts) => Ok(FilterValue::Int(*ts)),
        other => Err(VeinError::planning(
            format!("Unsupported literal in filter predicate: {other:?}"),
            None,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parser::ast::expr::BinaryOp;
    use plan::expr::literal::LogicalLiteral;
    use plan::schema::{ColumnId, ResolvedColumn, VeinDataType, VeinSchema};

    fn test_schema() -> VeinSchema {
        VeinSchema {
            fields: vec![
                ResolvedColumn::new(ColumnId(1), "id", Some("users".to_string()), VeinDataType::Int64),
                ResolvedColumn::new(ColumnId(2), "name", Some("users".to_string()), VeinDataType::Utf8),
                ResolvedColumn::new(ColumnId(3), "age", Some("users".to_string()), VeinDataType::Int32),
            ],
        }
    }

    #[test]
    fn test_map_column_to_physical_index() {
        let schema = test_schema();
        let translator = DefaultExprTranslator::new();

        // 1. Direct ColumnId to index resolution
        let col_age = LogicalExpr::Column(ResolvedColumn::new(
            ColumnId(3),
            "age",
            Some("users".to_string()),
            VeinDataType::Int32,
        ));
        assert_eq!(translator.map_column_to_physical_index(&col_age, &schema).unwrap(), 2);

        let col_id = LogicalExpr::Column(ResolvedColumn::new(
            ColumnId(1),
            "id",
            None,
            VeinDataType::Int64,
        ));
        assert_eq!(translator.map_column_to_physical_index(&col_id, &schema).unwrap(), 0);

        // 2. Unmatched ColumnId strictly errors
        let col_unmatched = LogicalExpr::Column(ResolvedColumn::new(
            ColumnId(999),
            "age",
            None,
            VeinDataType::Int32,
        ));
        assert!(translator.map_column_to_physical_index(&col_unmatched, &schema).is_err());

        // 3. Non-column expression strictly errors
        let lit_expr = LogicalExpr::Literal(LogicalLiteral::Int64(42));
        assert!(translator.map_column_to_physical_index(&lit_expr, &schema).is_err());
    }

    #[test]
    fn test_map_column_to_physical_index_joined_tables() {
        // Multi-table joined schema with duplicate column names
        let schema = VeinSchema {
            fields: vec![
                ResolvedColumn::new(ColumnId(1), "id", Some("users".to_string()), VeinDataType::Int64),
                ResolvedColumn::new(ColumnId(2), "id", Some("orders".to_string()), VeinDataType::Int64),
                ResolvedColumn::new(ColumnId(3), "amount", Some("orders".to_string()), VeinDataType::Float64),
            ],
        };
        let translator = DefaultExprTranslator::new();

        // Distinct ColumnIds resolve to their exact physical slots without ambiguity
        let user_id = LogicalExpr::Column(ResolvedColumn::new(
            ColumnId(1),
            "id",
            None,
            VeinDataType::Int64,
        ));
        assert_eq!(translator.map_column_to_physical_index(&user_id, &schema).unwrap(), 0);

        let order_id = LogicalExpr::Column(ResolvedColumn::new(
            ColumnId(2),
            "id",
            None,
            VeinDataType::Int64,
        ));
        assert_eq!(translator.map_column_to_physical_index(&order_id, &schema).unwrap(), 1);
    }

    #[test]
    fn test_translate_filter() {
        let schema = test_schema();
        let translator = DefaultExprTranslator::new();

        let col_age = LogicalExpr::Column(ResolvedColumn::new(
            ColumnId(3),
            "age",
            Some("users".to_string()),
            VeinDataType::Int32,
        ));
        let lit_20 = LogicalExpr::Literal(LogicalLiteral::Int64(20));

        let expr = LogicalExpr::BinaryOp {
            left: Box::new(col_age.clone()),
            op: BinaryOp::Gt,
            right: Box::new(lit_20.clone()),
            return_type: VeinDataType::Boolean,
        };

        let filter = translator.translate_filter(&expr, &schema).unwrap();
        assert_eq!(filter.column, 2);
        assert_eq!(filter.op, FilterOp::Gt);
        assert_eq!(filter.literal, FilterValue::Int(20));

        // Test symmetric normalization: 20 < age -> age > 20
        let expr_sym = LogicalExpr::BinaryOp {
            left: Box::new(lit_20),
            op: BinaryOp::Lt,
            right: Box::new(col_age),
            return_type: VeinDataType::Boolean,
        };
        let filter_sym = translator.translate_filter(&expr_sym, &schema).unwrap();
        assert_eq!(filter_sym.column, 2);
        assert_eq!(filter_sym.op, FilterOp::Gt);
        assert_eq!(filter_sym.literal, FilterValue::Int(20));
    }

    #[test]
    fn test_translate_projection() {
        let schema = test_schema();
        let translator = DefaultExprTranslator::new();

        let col_id = LogicalExpr::Column(ResolvedColumn::new(
            ColumnId(1),
            "id",
            None,
            VeinDataType::Int64,
        ));
        let col_name = LogicalExpr::Column(ResolvedColumn::new(
            ColumnId(2),
            "name",
            None,
            VeinDataType::Utf8,
        ));

        let cols = translator
            .translate_projection(&[col_id, col_name], &schema)
            .unwrap();
        assert_eq!(cols, vec![0, 1]);
    }

    #[test]
    fn test_translate_projection_wide_schema() {
        // Construct a wide schema with 30 columns (> SMALL_SCHEMA_THRESHOLD of 16)
        let fields: Vec<ResolvedColumn> = (1..=30)
            .map(|i| {
                ResolvedColumn::new(
                    ColumnId(i),
                    format!("col_{i}"),
                    Some("wide_table".to_string()),
                    VeinDataType::Int64,
                )
            })
            .collect();
        let schema = VeinSchema { fields };
        let translator = DefaultExprTranslator::new();

        // Project columns 25, 5, and 1
        let exprs = vec![
            LogicalExpr::Column(ResolvedColumn::new(
                ColumnId(25),
                "col_25",
                Some("wide_table".to_string()),
                VeinDataType::Int64,
            )),
            LogicalExpr::Column(ResolvedColumn::new(
                ColumnId(5),
                "col_5",
                Some("wide_table".to_string()),
                VeinDataType::Int64,
            )),
            LogicalExpr::Column(ResolvedColumn::new(
                ColumnId(1),
                "col_1",
                Some("wide_table".to_string()),
                VeinDataType::Int64,
            )),
        ];

        let cols = translator.translate_projection(&exprs, &schema).unwrap();
        // 0-indexed positions: col 25 is index 24, col 5 is index 4, col 1 is index 0
        assert_eq!(cols, vec![24, 4, 0]);

        // Verify that looking up a nonexistent column returns an error in wide mode
        let invalid_col = LogicalExpr::Column(ResolvedColumn::new(
            ColumnId(999),
            "col_999",
            Some("wide_table".to_string()),
            VeinDataType::Int64,
        ));
        assert!(translator
            .translate_projection(&[invalid_col], &schema)
            .is_err());
    }
}
