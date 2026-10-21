//! Tests for query-scoped statistics context registration.

use std::sync::Arc;

use plan::{LogicalPlan, TableMetadata, TableStatistics, VeinSchema};

use super::query_statistics_context;

#[test]
fn query_statistics_context_registers_catalog_resolved_external_sources() {
    let metadata = Arc::new(
        TableMetadata::new("external_events", Arc::new(VeinSchema::empty()))
            .with_statistics(TableStatistics {
                num_rows: Some(23),
                ..TableStatistics::default()
            }),
    );
    let external = LogicalPlan::ExternalScan {
        url: "file:///external_events".into(),
        options: vec![],
        files: vec![],
        schema: Box::new(metadata.schema.as_ref().clone()),
        source_metadata: Some(metadata),
    };

    let context = query_statistics_context(&external);
    assert_eq!(context.sources().len(), 1);
    assert_eq!(context.sources()[0].table_name, "external_events");
    assert_eq!(
        context.sources()[0]
            .statistics
            .as_deref()
            .and_then(|statistics| statistics.population_rows()),
        Some(23)
    );
}
