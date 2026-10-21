use super::{AggregateOperation, aggregate_operation};

#[test]
fn registered_aggregate_names_survive_physical_operation_lowering() {
    assert_eq!(
        aggregate_operation("count", false).expect("built-in aggregate"),
        AggregateOperation::Count
    );

    let custom = aggregate_operation("median_test", false).expect("registered aggregate name");
    assert_eq!(custom, AggregateOperation::Custom("median_test".to_owned()));
    assert_eq!(custom.name(), "median_test");
}

#[test]
fn distinct_remains_limited_to_supported_count_semantics() {
    assert!(aggregate_operation("median_test", true).is_err());
    assert_eq!(
        aggregate_operation("count", true).expect("COUNT DISTINCT"),
        AggregateOperation::CountDistinct
    );
}
