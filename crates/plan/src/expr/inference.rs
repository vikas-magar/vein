use std::sync::Arc;

use crate::expr::{LogicalExpr, LogicalLiteral};
use crate::plan::traversal::PlanTraversal;
use crate::schema::{ResolvedColumn, VeinDataType};

impl LogicalExpr {
    /// Returns the precise data type of the logical expression.
    /// Used by the binder and optimizer to validate schemas and enforce type safety.
    pub fn data_type(&self) -> VeinDataType {
        match self {
            LogicalExpr::Column(col) | LogicalExpr::OuterColumn(col) => col.data_type.clone(),
            LogicalExpr::Literal(lit) => literal_type(lit),
            LogicalExpr::BinaryOp { return_type, .. } => return_type.clone(),
            LogicalExpr::Cast { target_type, .. } => target_type.clone(),

            // Logical and comparison operators always return Boolean
            LogicalExpr::Not { .. }
            | LogicalExpr::IsNull { .. }
            | LogicalExpr::IsNotNull { .. }
            | LogicalExpr::InList { .. }
            | LogicalExpr::Like { .. }
            | LogicalExpr::Exists { .. } => VeinDataType::Boolean,

            // Mathematical negation preserves the type
            LogicalExpr::Negate { expr } => expr.data_type(),

            LogicalExpr::Case { return_type, .. } => return_type.clone(),

            LogicalExpr::FunctionCall { return_type, .. }
            | LogicalExpr::WindowCall { return_type, .. }
            | LogicalExpr::AiOperator { return_type, .. }
            | LogicalExpr::WasmUdf { return_type, .. }
            | LogicalExpr::EbpfUdf { return_type, .. }
            | LogicalExpr::PythonUdf { return_type, .. } => return_type.clone(),

            LogicalExpr::Lambda { body, .. } => body.data_type(), // Lambda returns its body's type

            LogicalExpr::ScalarSubquery(plan) => {
                // Scalar subqueries return the type of their first column
                plan.schema()
                    .fields
                    .first()
                    .map(|f| f.data_type.clone())
                    .unwrap_or(VeinDataType::Null)
            }
        }
    }
}

/// Returns the logical type of a literal value.
pub fn literal_type(lit: &LogicalLiteral) -> VeinDataType {
    match lit {
        LogicalLiteral::Null(t) => t.clone(),
        LogicalLiteral::Boolean(_) => VeinDataType::Boolean,
        LogicalLiteral::Int64(_) => VeinDataType::Int64,
        LogicalLiteral::Float64(_) => VeinDataType::Float64,
        LogicalLiteral::String(_) => VeinDataType::Utf8,
        LogicalLiteral::Date(_) => VeinDataType::Date32,
        LogicalLiteral::Timestamp(_) => VeinDataType::Timestamp(None),
        LogicalLiteral::Time(_) => VeinDataType::Time64,
        LogicalLiteral::Interval { .. } => VeinDataType::Interval,
        LogicalLiteral::Decimal(_, precision, scale) => VeinDataType::Decimal128(*precision, *scale),
        LogicalLiteral::List(items) => {
            VeinDataType::List(Arc::new(common_element_type(items)))
        }
        LogicalLiteral::Struct(entries) => {
            let fields = entries
                .iter()
                .map(|(name, lit)| ResolvedColumn {
                    id: crate::schema::ColumnId(0),
                    name: name.clone(),
                    qualifier: None,
                    data_type: literal_type(lit),
                    is_factorized: false,
                    nullable: true,
                })
                .collect::<Vec<_>>();
            VeinDataType::Struct(fields.into())
        }
        LogicalLiteral::Map(entries) => {
            let key = entries
                .first()
                .map(|(k, _)| literal_type(k))
                .unwrap_or(VeinDataType::Null);
            let value = entries
                .first()
                .map(|(_, v)| literal_type(v))
                .unwrap_or(VeinDataType::Null);
            VeinDataType::Map(Arc::new(key), Arc::new(value))
        }
        LogicalLiteral::Vector(v) => VeinDataType::Vector(v.len() as u32),
    }
}

/// The element type of a list literal: the first non-`Null` element's type,
/// falling back to `Null` for an empty/all-Null list.
fn common_element_type(items: &[LogicalLiteral]) -> VeinDataType {
    items
        .iter()
        .find_map(|lit| {
            if matches!(lit, LogicalLiteral::Null(_)) {
                None
            } else {
                Some(literal_type(lit))
            }
        })
        .unwrap_or(VeinDataType::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LogicalLiteral as Lit;

    #[test]
    fn time_and_interval_infer_their_types() {
        assert_eq!(literal_type(&Lit::Time(1_000)), VeinDataType::Time64);
        assert_eq!(
            literal_type(&Lit::Interval {
                months: 1,
                days: 2,
                nanos: 3
            }),
            VeinDataType::Interval
        );
        assert_eq!(
            literal_type(&Lit::Decimal(123, 76, 20)),
            VeinDataType::Decimal128(76, 20)
        );
    }

    #[test]
    fn list_infers_homogeneous_element_type() {
        assert_eq!(
            literal_type(&Lit::List(vec![Lit::Int64(1), Lit::Int64(2)])),
            VeinDataType::List(Arc::new(VeinDataType::Int64))
        );
    }

    #[test]
    fn list_skips_null_elements_for_element_type() {
        assert_eq!(
            literal_type(&Lit::List(vec![Lit::Null(VeinDataType::Int64), Lit::String("x".into())])),
            VeinDataType::List(Arc::new(VeinDataType::Utf8))
        );
    }

    #[test]
    fn empty_list_falls_back_to_null() {
        assert_eq!(
            literal_type(&Lit::List(vec![])),
            VeinDataType::List(Arc::new(VeinDataType::Null))
        );
    }

    #[test]
    fn struct_infers_named_fields() {
        let ty = literal_type(&Lit::Struct(vec![
            ("id".to_string(), Lit::Int64(1)),
            ("name".to_string(), Lit::String("vein".into())),
        ]));
        let VeinDataType::Struct(fields) = ty else {
            panic!("expected struct");
        };
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "id");
        assert_eq!(fields[0].data_type, VeinDataType::Int64);
        assert_eq!(fields[1].name, "name");
        assert_eq!(fields[1].data_type, VeinDataType::Utf8);
    }

    #[test]
    fn map_infers_key_and_value_types() {
        assert_eq!(
            literal_type(&Lit::Map(vec![(
                Lit::String("k".into()),
                Lit::Float64(1.5)
            )])),
            VeinDataType::Map(
                Arc::new(VeinDataType::Utf8),
                Arc::new(VeinDataType::Float64)
            )
        );
    }

    #[test]
    fn vector_infers_embedding_dimension() {
        assert_eq!(
            literal_type(&Lit::Vector(vec![1.0, 2.0, 3.0, 4.0])),
            VeinDataType::Vector(4)
        );
    }
}
