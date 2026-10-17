//! Reuses validated column estimates for consumers of a bound scan schema.

use super::{ColumnStatisticsLookup, columns::ColumnEstimate};
use plan::{ColumnId, ColumnStatistics, LogicalLiteral, TableStatistics, VeinSchema};

/// Borrowed catalog statistics mapped to the scan's query-local column IDs.
/// Validation and synopsis eligibility are shared with cardinality estimation.
pub struct ScanStatistics<'a> {
    schema: &'a VeinSchema,
    statistics: Option<&'a TableStatistics>,
}

impl<'a> ScanStatistics<'a> {
    /// Binds catalog column names to resolved scan IDs without copying statistics.
    pub fn new(schema: &'a VeinSchema, statistics: Option<&'a TableStatistics>) -> Self {
        Self { schema, statistics }
    }

    fn column(&self, id: ColumnId) -> Option<ColumnEstimate<'a>> {
        let field = self.schema.fields.iter().find(|field| field.id == id)?;
        let table = self.statistics?;
        ColumnEstimate::from_statistics(
            table.column_statistics.get(&field.name)?, table.num_rows?, field.nullable,
        ).ok()
    }
}

impl ColumnStatisticsLookup for ScanStatistics<'_> {
    fn distribution(&self, id: ColumnId) -> Option<(Option<f64>, Option<f64>)> {
        self.column(id).map(|column| (column.distinct, column.null_fraction))
    }

    fn bounds(&self, id: ColumnId) -> Option<(Option<&LogicalLiteral>, Option<&LogicalLiteral>)> {
        self.column(id).map(|column| (column.min, column.max))
    }

    fn synopsis(&self, id: ColumnId) -> Option<&ColumnStatistics> {
        self.column(id)?.synopsis
    }

    fn synopsis_population(&self, id: ColumnId) -> Option<u64> {
        self.column(id)?.synopsis_population
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use plan::{ResolvedColumn, VeinDataType};

    #[test]
    fn maps_scan_ids_and_rejects_invalid_or_missing_statistics() {
        let schema = VeinSchema::new(vec![Arc::new(ResolvedColumn::new(
            ColumnId(42), "value", None, VeinDataType::Int64,
        ))]);
        let mut table = TableStatistics { num_rows: Some(100), ..Default::default() };
        let stats = ColumnStatistics {
            distinct_count: Some(10),
            null_count: Some(0),
            min_value: Some(LogicalLiteral::Int64(1)),
            max_value: Some(LogicalLiteral::Int64(10)),
            ..Default::default()
        };
        table.column_statistics.insert("value".into(), stats);
        let lookup = ScanStatistics::new(&schema, Some(&table));
        assert_eq!(lookup.distribution(ColumnId(42)), Some((Some(10.0), Some(0.0))));
        assert!(lookup.distribution(ColumnId(0)).is_none());
        assert!(std::ptr::eq(lookup.bounds(ColumnId(42)).unwrap().0.unwrap(),
                             table.column_statistics["value"].min_value.as_ref().unwrap()));
        table.column_statistics.get_mut("value").unwrap().null_count = Some(101);
        assert!(ScanStatistics::new(&schema, Some(&table)).distribution(ColumnId(42)).is_none());
        assert!(ScanStatistics::new(&schema, None).distribution(ColumnId(42)).is_none());
    }
}
