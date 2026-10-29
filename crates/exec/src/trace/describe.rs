use super::*;

pub(super) fn operator_name(operator: &PhysicalPlanOperator) -> &'static str { operator.data_flow().operator }

pub(super) fn operator_config(operator: &PhysicalPlanOperator) -> String {
    match operator {
        PhysicalPlanOperator::Extension { descriptor, inputs } => format!(
            "factory={}, codec_version={}, role={:?}, inputs={}, configuration_bytes={}",
            descriptor.factory_id, descriptor.configuration_version, descriptor.role, inputs.len(), descriptor.configuration.len()),

        PhysicalPlanOperator::Pipeline {
            source, program, ..
        } => {
            let source_kind = match source.as_ref() {
                common::protocol::DataSource::File(_) => "file",
                common::protocol::DataSource::Stream(_) => "stream",
                common::protocol::DataSource::Memory => "memory",
                common::protocol::DataSource::Intermediate(_) => "intermediate",
            };
            format!(
                "source={source_kind}, expression_steps={}",
                program.steps.len()
            )
        }
        PhysicalPlanOperator::Transform { program, .. } => {
            format!("expression_steps={}", program.steps.len())
        }
        PhysicalPlanOperator::Join { plan, .. } => format!(
            "join_type={:?}, probe_keys={:?}, build_keys={:?}, outputs={:?}",
            plan.join_type, plan.probe_keys, plan.build_keys, plan.outputs
        ),
        PhysicalPlanOperator::Aggregate { plan, .. }
        | PhysicalPlanOperator::Distinct { plan, .. } => format!(
            "groups={}, aggregate_count={}, grouping_sets={}",
            plan.groups,
            plan.aggs.len(),
            plan.grouping_sets.as_ref().map_or(0, Vec::len)
        ),
        PhysicalPlanOperator::Sort {
            plan,
            key_program,
            output_columns,
            ..
        } => format!(
            "keys={:?}, computed_key_steps={}, output_columns={output_columns}",
            plan.keys,
            key_program
                .as_ref()
                .map_or(0, |program| program.steps.len())
        ),
        PhysicalPlanOperator::Limit { spec, .. } => {
            format!("offset={}, count={}", spec.offset, spec.count)
        }
        PhysicalPlanOperator::Sink { sink, .. } => {
            let sink = match sink.as_ref() {
                DataSink::Client { .. } => "client",
                DataSink::Storage(_) => "storage",
                DataSink::Discard => "discard",
            };
            format!("sink={sink}")
        }
    }
}
