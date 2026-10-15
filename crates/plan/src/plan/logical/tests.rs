use super::LogicalPlan;
use crate::schema::VeinSchema;

#[test]
fn operator_name_is_stable_and_does_not_format_plan_contents() {
    let plan = LogicalPlan::Explain {
        input: Box::new(LogicalPlan::Call {
            input: Box::new(LogicalPlan::Scan {
                table: std::sync::Arc::new(catalog::TableMetadata::new(
                    "sensitive_table_name",
                    std::sync::Arc::new(VeinSchema::empty()),
                )),
                projection: None,
                filters: Vec::new(),
                schema: Box::new(VeinSchema::empty()),
            }),
            name: "custom_function".to_string(),
            args: Vec::new(),
            schema: Box::new(VeinSchema::empty()),
        }),
        schema: Box::new(VeinSchema::empty()),
    };

    assert_eq!(plan.operator_name(), "Explain");
}
