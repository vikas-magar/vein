use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use common::protocol::SortKey;

use crate::pipeline::resumable::{
    InputProperties, OperatorCapabilities, OperatorChild, OperatorMetadata, OperatorNodeId, OperatorRole,
    OrderingGuarantee, OrderingRequirement, ParallelismCapability, PartitioningGuarantee,
    PartitioningRequirement,
};

fn metadata(role: OperatorRole) -> OperatorMetadata {
    let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
    let children = if role == OperatorRole::Source {
        Vec::new()
    } else {
        vec![OperatorChild { node: OperatorNodeId::new(1), schema: Arc::clone(&schema) }]
    };
    let output = (role != OperatorRole::Sink).then_some(schema);
    OperatorMetadata::new(OperatorNodeId::new(2), role, children, output).expect("metadata")
}

#[test]
fn capability_validation_rejects_missing_input_and_output_columns() {
    let child_column = InputProperties {
        ordering: OrderingRequirement::Ordered(Arc::from([SortKey {
            column: 1,
            ascending: true,
            nulls_first: false,
        }])),
        partitioning: PartitioningRequirement::Any,
    };
    let input_invalid = OperatorCapabilities::new(
        vec![child_column],
        OrderingGuarantee::Unknown,
        PartitioningGuarantee::Unknown,
        ParallelismCapability::Serial,
    );
    assert!(input_invalid.validate_for(&metadata(OperatorRole::Transform)).is_err());

    let output_invalid = OperatorCapabilities::new(
        vec![],
        OrderingGuarantee::Ordered(Arc::from([SortKey {
            column: 2,
            ascending: false,
            nulls_first: true,
        }])),
        PartitioningGuarantee::Hash(Arc::from([0])),
        ParallelismCapability::Independent,
    );
    assert!(output_invalid.validate_for(&metadata(OperatorRole::Source)).is_err());
}

#[test]
fn capability_validation_accepts_conservative_unknown_properties() {
    let capabilities = OperatorCapabilities::new(
        vec![InputProperties::any()],
        OrderingGuarantee::Unknown,
        PartitioningGuarantee::Unknown,
        ParallelismCapability::Serial,
    );
    assert!(capabilities.validate_for(&metadata(OperatorRole::Transform)).is_ok());
}

#[test]
fn partitioning_keys_must_exist_on_the_declared_schema() {
    let input = OperatorCapabilities::new(
        vec![InputProperties {
            ordering: OrderingRequirement::Any,
            partitioning: PartitioningRequirement::Hash(Arc::from([3])),
        }],
        OrderingGuarantee::Unknown,
        PartitioningGuarantee::Unknown,
        ParallelismCapability::Serial,
    );
    assert!(input.validate_for(&metadata(OperatorRole::Transform)).is_err());

    let output = OperatorCapabilities::new(
        vec![],
        OrderingGuarantee::Unknown,
        PartitioningGuarantee::Hash(Arc::from([2])),
        ParallelismCapability::Independent,
    );
    assert!(output.validate_for(&metadata(OperatorRole::Source)).is_err());
}
