use super::super::ExecutablePipelinePlan;
use super::fixtures::*;
use crate::pipeline::resumable::{
    InputProperties, OperatorFactoryRegistry, OrderingRequirement, PartitioningGuarantee,
    PartitioningRequirement,
};
use common::protocol::SortKey;
use std::sync::Arc;

fn ordered() -> InputProperties {
    InputProperties {
        ordering: OrderingRequirement::Ordered(
            vec![SortKey {
                column: 0,
                ascending: true,
                nulls_first: false,
            }]
            .into(),
        ),
        partitioning: PartitioningRequirement::Any,
    }
}

#[test]
fn sorted_output_satisfies_ordered_input_without_changing_the_canonical_plan() {
    let sorted = sort(source(schema()));
    let limited = limit(Arc::clone(&sorted));
    let mut provider = Provider::default();
    provider.requirements.insert(pointer(&limited), ordered());
    let plan = client(Arc::clone(&limited));
    let executable =
        ExecutablePipelinePlan::prepare(&plan, &OperatorFactoryRegistry::new(), &provider).unwrap();
    assert!(executable.properties(&limited).unwrap().ordering.is_some());
    assert_eq!(
        executable.properties(&limited).unwrap().ordering,
        executable.properties(&sorted).unwrap().ordering
    );
    for (ascending, nulls_first) in [(false, false), (true, true)] {
        provider.requirements.insert(
            pointer(&limited),
            InputProperties {
                ordering: OrderingRequirement::Ordered(
                    vec![SortKey {
                        column: 0,
                        ascending,
                        nulls_first,
                    }]
                    .into(),
                ),
                partitioning: PartitioningRequirement::Any,
            },
        );
        assert!(
            ExecutablePipelinePlan::prepare(&plan, &OperatorFactoryRegistry::new(), &provider)
                .is_err()
        );
    }
}

#[test]
fn unknown_ordering_and_partitioning_never_satisfy_stronger_requirements() {
    let input = source(schema());
    let limited = limit(Arc::clone(&input));
    let plan = client(Arc::clone(&limited));
    let mut provider = Provider::default();
    provider.requirements.insert(pointer(&limited), ordered());
    assert!(
        ExecutablePipelinePlan::prepare(&plan, &OperatorFactoryRegistry::new(), &provider).is_err()
    );
    provider.requirements.insert(
        pointer(&limited),
        InputProperties {
            ordering: OrderingRequirement::Any,
            partitioning: PartitioningRequirement::Single,
        },
    );
    assert!(
        ExecutablePipelinePlan::prepare(&plan, &OperatorFactoryRegistry::new(), &provider).is_err()
    );
    provider
        .partitions
        .insert(pointer(&input), PartitioningGuarantee::Single);
    assert!(
        ExecutablePipelinePlan::prepare(&plan, &OperatorFactoryRegistry::new(), &provider).is_ok()
    );
}

#[test]
fn hash_partition_requirements_compare_the_complete_key_list() {
    use types::{ColumnId, ResolvedColumn, VeinDataType, VeinSchema};
    let mut fields = schema().fields.clone();
    fields.push(Arc::new(ResolvedColumn::new(
        ColumnId(2),
        "w",
        None,
        VeinDataType::Int64,
    )));
    let input = source(Arc::new(VeinSchema::new(fields)));
    let limited = limit(Arc::clone(&input));
    let plan = client(Arc::clone(&limited));
    let mut provider = Provider::default();
    provider.requirements.insert(
        pointer(&limited),
        InputProperties {
            ordering: OrderingRequirement::Any,
            partitioning: PartitioningRequirement::Hash(vec![0, 1].into()),
        },
    );
    provider
        .partitions
        .insert(pointer(&input), PartitioningGuarantee::Hash(vec![0].into()));
    assert!(
        ExecutablePipelinePlan::prepare(&plan, &OperatorFactoryRegistry::new(), &provider).is_err()
    );
    provider.partitions.insert(
        pointer(&input),
        PartitioningGuarantee::Hash(vec![0, 1].into()),
    );
    assert!(
        ExecutablePipelinePlan::prepare(&plan, &OperatorFactoryRegistry::new(), &provider).is_ok()
    );
}
