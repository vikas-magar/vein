//! Schema definitions and conversions for Vein.
//!
//! Provides [`VeinSchema`] and [`VeinSchemaRef`], offering unified schema
//! management, column resolution, and zero-allocation conversion to/from Apache Arrow.

use std::sync::Arc;
use arrow::datatypes::{Field, Schema};
use error::Result;
use serde::{Deserialize, Serialize};

use crate::column::{ColumnId, ResolvedColumn, ResolvedColumnRef};

/// Query schema representing an ordered sequence of resolved columns.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VeinSchema {
    /// The fields making up this schema.
    ///
    /// Columns are reference-counted, so duplicating a schema copies pointers
    /// rather than names and type trees.
    pub fields: Vec<ResolvedColumnRef>,
}

impl VeinSchema {
    /// Creates a new [`VeinSchema`] from a vector of resolved columns.
    pub fn new(fields: Vec<ResolvedColumnRef>) -> Self {
        Self { fields }
    }

    /// Creates an empty [`VeinSchema`].
    pub fn empty() -> Self {
        Self { fields: Vec::new() }
    }

    /// Finds the 0-based physical field index for a given `ColumnId`.
    pub fn position_of_id(&self, id: ColumnId) -> Option<usize> {
        self.fields.iter().position(|field| field.id == id)
    }

    /// Finds the 0-based physical field index for a given column name.
    pub fn position_by_name(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|field| field.name == name)
    }

    /// Returns a reference to the column with the given name, if present.
    pub fn field_by_name(&self, name: &str) -> Option<&ResolvedColumnRef> {
        self.fields.iter().find(|field| field.name == name)
    }

    /// Converts this schema to an Apache Arrow [`Schema`].
    pub fn to_arrow(&self) -> Schema {
        let arrow_fields = self
            .fields
            .iter()
            .map(|f| Field::new(&f.name, f.data_type.to_arrow(), f.nullable))
            .collect::<Vec<_>>();
        Schema::new(arrow_fields)
    }
}

impl TryFrom<&Schema> for VeinSchema {
    type Error = error::VeinError;

    fn try_from(arrow_schema: &Schema) -> Result<Self> {
        let fields = arrow_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                Ok(Arc::new(ResolvedColumn::new(
                    ColumnId(i),
                    f.name().clone(),
                    None,
                    crate::data_type::VeinDataType::try_from(f.data_type())?,
                )
                    .with_nullable(f.is_nullable())))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { fields })
    }
}

impl From<&VeinSchema> for Schema {
    fn from(schema: &VeinSchema) -> Self {
        schema.to_arrow()
    }
}

impl std::fmt::Display for VeinSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cols = self
            .fields
            .iter()
            .map(|col| format!("{}: {:?}", col.name, col.data_type))
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "[{cols}]")
    }
}

/// Reference-counted [`VeinSchema`].
pub type VeinSchemaRef = Arc<VeinSchema>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_type::VeinDataType;

    #[test]
    fn test_schema_arrow_roundtrip() {
        let schema = VeinSchema::new(vec![
            Arc::new(ResolvedColumn::new(ColumnId(0), "id", None, VeinDataType::Int64)),
            Arc::new(
                ResolvedColumn::new(ColumnId(1), "name", None, VeinDataType::Utf8)
                    .with_nullable(true),
            ),
            Arc::new(ResolvedColumn::new(
                ColumnId(2),
                "time",
                None,
                VeinDataType::Time32(crate::TimeUnit::Second),
            )),
            Arc::new(ResolvedColumn::new(
                ColumnId(3),
                "timestamp",
                None,
                VeinDataType::Timestamp(crate::TimeUnit::Nanosecond, Some("UTC".into())),
            )),
            Arc::new(ResolvedColumn::new(
                ColumnId(4),
                "year_month_interval",
                None,
                VeinDataType::Interval(crate::IntervalUnit::YearMonth),
            )),
            Arc::new(ResolvedColumn::new(
                ColumnId(5),
                "day_time_interval",
                None,
                VeinDataType::Interval(crate::IntervalUnit::DayTime),
            )),
            Arc::new(ResolvedColumn::new(
                ColumnId(6),
                "month_day_nano_interval",
                None,
                VeinDataType::Interval(crate::IntervalUnit::MonthDayNano),
            )),
        ]);

        let arrow_schema: Schema = (&schema).into();
        assert_eq!(arrow_schema.fields().len(), 7);
        assert_eq!(arrow_schema.field(0).name(), "id");
        assert_eq!(arrow_schema.field(1).name(), "name");
        assert!(arrow_schema.field(1).is_nullable());

        let back = VeinSchema::try_from(&arrow_schema).expect("supported Arrow schema");
        assert_eq!(schema.fields.len(), back.fields.len());
        assert_eq!(schema.fields[0].name, back.fields[0].name);
        assert_eq!(schema.fields[0].data_type, back.fields[0].data_type);
        assert_eq!(schema.fields[1].name, back.fields[1].name);
        assert_eq!(schema.fields[1].nullable, back.fields[1].nullable);
        assert_eq!(schema.fields[2].data_type, back.fields[2].data_type);
        assert_eq!(schema.fields[3].data_type, back.fields[3].data_type);
        assert_eq!(schema.fields[4].data_type, back.fields[4].data_type);
        assert_eq!(schema.fields[5].data_type, back.fields[5].data_type);
        assert_eq!(schema.fields[6].data_type, back.fields[6].data_type);
    }

    #[test]
    fn unsupported_arrow_field_type_fails_schema_conversion() {
        let arrow_schema = Schema::new(vec![Field::new(
            "payload",
            arrow::datatypes::DataType::Dictionary(
                Box::new(arrow::datatypes::DataType::Int8),
                Box::new(arrow::datatypes::DataType::Utf8),
            ),
            true,
        )]);

        assert!(VeinSchema::try_from(&arrow_schema).is_err());
    }
}
