use plan::LogicalPlan;
use plan::schema::VeinSchema;

pub(crate) const TEST_DATA_FILE_FORMAT: &str = "test";

pub(crate) fn external_scan(schema: &VeinSchema) -> LogicalPlan {
    LogicalPlan::ExternalScan {
        url: "memory://physical-test".to_owned(),
        options: vec![("format".to_owned(), TEST_DATA_FILE_FORMAT.to_owned())],
        files: Vec::new(),
        source_metadata: None,
        schema: Box::new(schema.clone()),
    }
}
