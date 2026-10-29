use super::*;
use crate::protocol::JoinPlan;
pub(super) fn validate(node: &PhysicalPlanNode, probe: &PhysicalPlanNode, build: &PhysicalPlanNode, plan: &JoinPlan, path: &str) -> Result<()> {
    validate_node(probe, &format!("{path}.probe"), false)?;
    validate_node(build, &format!("{path}.build"), false)?;

    if plan.probe_keys.len() != plan.build_keys.len() {
        return Err(invalid(path, "probe and build key counts differ"));
    }
    if plan.probe_types.len() != probe.output_schema.fields.len()
        || plan.build_types.len() != build.output_schema.fields.len()
    {
        return Err(invalid(
            path,
            "join side type metadata does not match the child schemas",
        ));
    }
    for (side, declared, schema) in [
        ("probe", &plan.probe_types, &probe.output_schema),
        ("build", &plan.build_types, &build.output_schema),
    ] {
        for (index, (declared, field)) in declared.iter().zip(&schema.fields).enumerate() {
            if declared != &field.data_type {
                return Err(invalid(
                    path,
                    format!(
                        "join {side} type at column {index} is {declared:?}, child schema declares {:?}",
                        field.data_type
                    ),
                ));
            }
        }
    }
    for (&probe_key, &build_key) in plan.probe_keys.iter().zip(&plan.build_keys) {
        check_column_index(
            probe_key,
            probe.output_schema.fields.len(),
            path,
            "probe key",
        )?;
        check_column_index(
            build_key,
            build.output_schema.fields.len(),
            path,
            "build key",
        )?;
    }
    if let Some(residual) = &plan.residual {
        let mut fields = Vec::with_capacity(residual.input_columns.len());
        for &column in &residual.input_columns {
            let field = match column {
                JoinOutput::Probe(index) => {
                    check_column_index(
                        index,
                        probe.output_schema.fields.len(),
                        path,
                        "residual probe column",
                    )?;
                    &probe.output_schema.fields[index as usize]
                }
                JoinOutput::Build(index) => {
                    check_column_index(
                        index,
                        build.output_schema.fields.len(),
                        path,
                        "residual build column",
                    )?;
                    &build.output_schema.fields[index as usize]
                }
            };
            fields.push(std::sync::Arc::clone(field));
        }
        let residual_schema = VeinSchema::new(fields);
        validate_program(
            &residual.program,
            &residual_schema,
            Some(residual_schema.fields.len()),
            &format!("{path}.residual"),
        )?;
    }
    if plan.outputs.len() != node.output_schema.fields.len() {
        return Err(invalid(
            path,
            format!(
                "join declares {} output columns but its schema has {}",
                plan.outputs.len(),
                node.output_schema.fields.len()
            ),
        ));
    }
    for (output_index, source) in plan.outputs.iter().enumerate() {
        let source_field = match *source {
            JoinOutput::Probe(index) => {
                check_column_index(
                    index,
                    probe.output_schema.fields.len(),
                    path,
                    "probe output",
                )?;
                &probe.output_schema.fields[index as usize]
            }
            JoinOutput::Build(index) => {
                check_column_index(
                    index,
                    build.output_schema.fields.len(),
                    path,
                    "build output",
                )?;
                &build.output_schema.fields[index as usize]
            }
        };
        if source_field.data_type != node.output_schema.fields[output_index].data_type {
            return Err(invalid(
                path,
                format!(
                    "join output column {output_index} has type {:?}, selected input column has type {:?}",
                    node.output_schema.fields[output_index].data_type,
                    source_field.data_type
                ),
            ));
        }
        let output_is_null_extended = match plan.join_type {
            crate::protocol::JoinKind::Left | crate::protocol::JoinKind::Single => {
                matches!(*source, JoinOutput::Build(_))
            }
            crate::protocol::JoinKind::Right => matches!(*source, JoinOutput::Probe(_)),
            crate::protocol::JoinKind::Full => true,
            crate::protocol::JoinKind::Inner
            | crate::protocol::JoinKind::Cross
            | crate::protocol::JoinKind::Semi
            | crate::protocol::JoinKind::Anti
            | crate::protocol::JoinKind::RightSemi
            | crate::protocol::JoinKind::RightAnti
            | crate::protocol::JoinKind::NullAwareAnti => false,
        };
        let expected_nullable = source_field.nullable || output_is_null_extended;
        if node.output_schema.fields[output_index].nullable != expected_nullable {
            return Err(invalid(
                path,
                format!(
                    "{:?} join output column {output_index} mapped from {source:?} to input column '{}#{}' requires nullable {expected_nullable}, but the output schema declares nullable {}",
                    plan.join_type,
                    source_field.name,
                    source_field.id.0,
                    node.output_schema.fields[output_index].nullable,
                ),
            ));
        }
        if matches!(
        plan.join_type,
        crate::protocol::JoinKind::Semi | crate::protocol::JoinKind::Anti
    ) && matches!(source, JoinOutput::Build(_))
        {
            return Err(invalid(
                path,
                format!(
                    "{:?} join cannot emit build-side output column {output_index}",
                    plan.join_type
                ),
            ));
        }
        if matches!(plan.join_type,
        crate::protocol::JoinKind::RightSemi | crate::protocol::JoinKind::RightAnti
    ) && matches!(source, JoinOutput::Probe(_)) {
            return Err(invalid(path, format!(
                "{:?} join cannot emit probe-side output column {output_index}", plan.join_type)));
        }
    }
    Ok(())
}
