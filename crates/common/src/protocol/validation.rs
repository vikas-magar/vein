//! Structural validation for the canonical physical plan.

use std::collections::HashSet;

use crate::{Result, VeinError};

use super::{
    ir::{MicroOp, MicroProgram, RegId},
    task::{
        DataSink, DataSource, JoinOutput, PhysicalPlan, PhysicalPlanNode, PhysicalPlanOperator,
    },
};
use types::VeinSchema;

impl PhysicalPlan {
    /// Validates operator inputs, register references, and schema boundaries
    /// throughout this plan before it is scheduled or executed.
    pub fn validate(&self) -> Result<()> {
        if !matches!(self.root.operator, PhysicalPlanOperator::Sink { .. }) {
            return Err(invalid("root", "the root operator must be a sink"));
        }
        validate_node(&self.root, "root", true)
    }
}

fn validate_node(node: &PhysicalPlanNode, path: &str, root: bool) -> Result<()> {
    match &node.operator {
        PhysicalPlanOperator::Pipeline {
            source,
            input_schema,
            program,
        } => {
            if let DataSource::File(scan) = source.as_ref()
                && scan.projected.as_ref() != input_schema.as_ref() {
                    return Err(invalid(
                        path,
                        format!(
                            "file source schema {} does not match pipeline input schema {}",
                            scan.projected, input_schema
                        ),
                    ));
                }
            validate_program(
                program,
                input_schema,
                Some(node.output_schema.fields.len()),
                path,
            )?;
        }
        PhysicalPlanOperator::Transform { input, program } => {
            validate_node(input, &format!("{path}.input"), false)?;
            validate_program(
                program,
                &input.output_schema,
                Some(node.output_schema.fields.len()),
                path,
            )?;
        }
        PhysicalPlanOperator::Join { probe, build, plan } => {
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
                    | crate::protocol::JoinKind::Anti => false,
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
            }
        }
        PhysicalPlanOperator::Aggregate { input, plan } => {
            validate_node(input, &format!("{path}.input"), false)?;
            let groups = usize::try_from(plan.groups)
                .map_err(|_| invalid(path, "aggregate group count exceeds addressable memory"))?;
            let input_width = groups
                .checked_add(plan.aggs.len())
                .ok_or_else(|| invalid(path, "aggregate input column count overflowed"))?;
            let output_width = groups
                .checked_add(plan.aggs.len())
                .ok_or_else(|| invalid(path, "aggregate output column count overflowed"))?;
            if input_width != input.output_schema.fields.len() {
                return Err(invalid(
                    path,
                    format!(
                        "aggregate consumes {input_width} key/argument columns but child schema has {}",
                        input.output_schema.fields.len()
                    ),
                ));
            }
            if plan.aggregate_input_types.len() != plan.aggs.len()
                || plan.types.len() != output_width
                || node.output_schema.fields.len() != output_width
            {
                return Err(invalid(
                    path,
                    "aggregate operation, input type, output type, and schema widths disagree",
                ));
            }
            for (index, (declared, field)) in plan
                .types
                .iter()
                .zip(&node.output_schema.fields)
                .enumerate()
            {
                if declared != &field.data_type {
                    return Err(invalid(
                        path,
                        format!(
                            "aggregate output type at column {index} is {declared:?}, schema declares {:?}",
                            field.data_type
                        ),
                    ));
                }
            }
            if let Some(grouping_sets) = &plan.grouping_sets {
                for (set_index, set) in grouping_sets.iter().enumerate() {
                    for &column in set {
                        check_column_index(
                            column,
                            groups,
                            path,
                            &format!("grouping set {set_index}"),
                        )?;
                    }
                }
            }
        }
        PhysicalPlanOperator::Distinct { input, plan } => {
            validate_node(input, &format!("{path}.input"), false)?;
            let input_width = input.output_schema.fields.len();
            let groups = usize::try_from(plan.groups)
                .map_err(|_| invalid(path, "distinct group count exceeds addressable memory"))?;
            let expected_keys = (0..input_width)
                .map(|index| {
                    u32::try_from(index)
                        .map_err(|_| invalid(path, "distinct input has too many columns"))
                })
                .collect::<Result<Vec<_>>>()?;
            if groups != input_width
                || !plan.aggs.is_empty()
                || !plan.aggregate_input_types.is_empty()
                || plan.grouping_sets.as_ref() != Some(&vec![expected_keys])
                || plan.emit_empty_group_on_empty_input
            {
                return Err(invalid(
                    path,
                    "distinct must group every input column without aggregates or an empty-input group",
                ));
            }
            if plan.types.len() != input_width
                || plan
                    .types
                    .iter()
                    .zip(&input.output_schema.fields)
                    .any(|(data_type, field)| data_type != &field.data_type)
            {
                return Err(invalid(
                    path,
                    "distinct output types must match its input schema",
                ));
            }
            if plan
                .types
                .iter()
                .zip(&node.output_schema.fields)
                .any(|(data_type, field)| data_type != &field.data_type)
            {
                return Err(invalid(
                    path,
                    "distinct output types must match its output schema",
                ));
            }
            ensure_same_schema(&node.output_schema, &input.output_schema, path, "distinct")?;
        }
        PhysicalPlanOperator::Sort {
            input,
            plan,
            key_program,
            output_columns,
        } => {
            validate_node(input, &format!("{path}.input"), false)?;
            let visible = usize::try_from(*output_columns)
                .map_err(|_| invalid(path, "sort output width exceeds addressable memory"))?;
            if visible != input.output_schema.fields.len()
                || visible != node.output_schema.fields.len()
            {
                return Err(invalid(
                    path,
                    "sort visible output width must match its child and node schemas",
                ));
            }
            let key_width = if let Some(program) = key_program {
                validate_program(program, &input.output_schema, None, &format!("{path}.keys"))?
                    .unwrap_or(input.output_schema.fields.len())
            } else {
                input.output_schema.fields.len()
            };
            for key in &plan.keys {
                check_column_index(key.column, key_width, path, "sort key")?;
            }
        }
        PhysicalPlanOperator::Limit { input, .. } => {
            validate_node(input, &format!("{path}.input"), false)?;
            ensure_same_schema(&node.output_schema, &input.output_schema, path, "limit")?;
        }
        PhysicalPlanOperator::Sink { input, sink } => {
            if !root {
                return Err(invalid(
                    path,
                    "sink operators are only valid at the plan root",
                ));
            }
            validate_node(input, &format!("{path}.input"), false)?;
            match sink.as_ref() {
                DataSink::Client { schema } => {
                    ensure_same_schema(schema, &input.output_schema, path, "client sink")?;
                    ensure_same_schema(&node.output_schema, schema, path, "sink node")?;
                }
                DataSink::Storage(target) => {
                    ensure_same_schema(&target.schema, &input.output_schema, path, "storage sink")?;
                    ensure_same_schema(&node.output_schema, &target.schema, path, "sink node")?;
                }
                DataSink::Discard => {}
            }
        }
    }
    Ok(())
}

/// Checks register bounds and initialization without allocating by the declared
/// register count. The set grows only for registers actually written by code.
fn validate_program(
    program: &MicroProgram,
    input_schema: &VeinSchema,
    expected_output_width: Option<usize>,
    path: &str,
) -> Result<Option<usize>> {
    let input_width = u32::try_from(input_schema.fields.len()).map_err(|_| {
        invalid(
            path,
            "input schema has more columns than Micro-IR can address",
        )
    })?;
    if !program.instructions.is_empty() && program.register_count < input_width {
        return Err(invalid(
            path,
            format!(
                "program declares {} registers for {} input columns ({input_schema})",
                program.register_count,
                input_schema.fields.len()
            ),
        ));
    }
    let mut initialized: HashSet<RegId> = if program.instructions.is_empty() {
        HashSet::new()
    } else {
        (0..input_width).collect()
    };
    let mut projected_width = None;

    for (index, instruction) in program.instructions.iter().enumerate() {
        let op_path = format!("{path}.program[{index}]");
        match instruction {
            MicroOp::LoadConstant { dst, .. } => {
                write_register(*dst, program, &mut initialized, &op_path)?
            }
            MicroOp::CopyRegister { dst, src }
            | MicroOp::Cast { dst, src, .. }
            | MicroOp::Neg { dst, src }
            | MicroOp::Not { dst, src }
            | MicroOp::IsNull { dst, src }
            | MicroOp::IsNotNull { dst, src } => {
                read_register(*src, program, &initialized, &op_path)?;
                write_register(*dst, program, &mut initialized, &op_path)?;
            }
            MicroOp::Add { dst, src1, src2 }
            | MicroOp::Sub { dst, src1, src2 }
            | MicroOp::Mul { dst, src1, src2 }
            | MicroOp::Div { dst, src1, src2 }
            | MicroOp::Mod { dst, src1, src2 }
            | MicroOp::CmpEq { dst, src1, src2 }
            | MicroOp::CmpNeq { dst, src1, src2 }
            | MicroOp::CmpLt { dst, src1, src2 }
            | MicroOp::CmpLte { dst, src1, src2 }
            | MicroOp::CmpGt { dst, src1, src2 }
            | MicroOp::CmpGte { dst, src1, src2 }
            | MicroOp::And { dst, src1, src2 }
            | MicroOp::Or { dst, src1, src2 }
            | MicroOp::VectorCosineDistance { dst, src1, src2 }
            | MicroOp::VectorL2Distance { dst, src1, src2 }
            | MicroOp::VectorDotProduct { dst, src1, src2 } => {
                read_register(*src1, program, &initialized, &op_path)?;
                read_register(*src2, program, &initialized, &op_path)?;
                write_register(*dst, program, &mut initialized, &op_path)?;
            }
            MicroOp::Like {
                dst, src, pattern, ..
            } => {
                read_register(*src, program, &initialized, &op_path)?;
                read_register(*pattern, program, &initialized, &op_path)?;
                write_register(*dst, program, &mut initialized, &op_path)?;
            }
            MicroOp::InList {
                dst,
                value,
                candidates,
                ..
            } => {
                read_register(*value, program, &initialized, &op_path)?;
                for &candidate in candidates {
                    read_register(candidate, program, &initialized, &op_path)?;
                }
                write_register(*dst, program, &mut initialized, &op_path)?;
            }
            MicroOp::Case {
                dst,
                when_regs,
                then_regs,
                else_reg,
                ..
            } => {
                if when_regs.len() != then_regs.len() {
                    return Err(invalid(&op_path, "CASE condition and result counts differ"));
                }
                for &reg in when_regs.iter().chain(then_regs) {
                    read_register(reg, program, &initialized, &op_path)?;
                }
                if let Some(reg) = else_reg {
                    read_register(*reg, program, &initialized, &op_path)?;
                }
                write_register(*dst, program, &mut initialized, &op_path)?;
            }
            MicroOp::CallUdf { dst, arg_regs, .. } => {
                for &reg in arg_regs {
                    read_register(reg, program, &initialized, &op_path)?;
                }
                write_register(*dst, program, &mut initialized, &op_path)?;
            }
            MicroOp::FilterMask { bool_reg } => {
                read_register(*bool_reg, program, &initialized, &op_path)?;
            }
            MicroOp::ProjectRegisters { out_regs } => {
                for &reg in out_regs {
                    read_register(reg, program, &initialized, &op_path)?;
                }
                projected_width = Some(out_regs.len());
            }
            MicroOp::StateBuild {
                key_regs,
                payload_regs,
                ..
            } => {
                for &reg in key_regs.iter().chain(payload_regs) {
                    read_register(reg, program, &initialized, &op_path)?;
                }
            }
            MicroOp::StateProbe {
                key_regs, out_regs, ..
            } => {
                for &reg in key_regs {
                    read_register(reg, program, &initialized, &op_path)?;
                }
                for &reg in out_regs {
                    write_register(reg, program, &mut initialized, &op_path)?;
                }
            }
            MicroOp::StateFold { agg_regs, .. } => {
                for &reg in agg_regs {
                    read_register(reg, program, &initialized, &op_path)?;
                }
            }
        }
    }

    let output_width = projected_width.unwrap_or(input_schema.fields.len());
    if let Some(expected) = expected_output_width
        && output_width != expected {
            return Err(invalid(
                path,
                format!("program emits {output_width} columns but node schema has {expected}"),
            ));
        }
    Ok(projected_width)
}

fn read_register(
    register: RegId,
    program: &MicroProgram,
    initialized: &HashSet<RegId>,
    path: &str,
) -> Result<()> {
    if register >= program.register_count {
        return Err(invalid(
            path,
            format!(
                "read of register {register} exceeds register count {}",
                program.register_count
            ),
        ));
    }
    if !initialized.contains(&register) {
        return Err(invalid(
            path,
            format!("read of uninitialized register {register}"),
        ));
    }
    Ok(())
}

fn write_register(
    register: RegId,
    program: &MicroProgram,
    initialized: &mut HashSet<RegId>,
    path: &str,
) -> Result<()> {
    if register >= program.register_count {
        return Err(invalid(
            path,
            format!(
                "write of register {register} exceeds register count {}",
                program.register_count
            ),
        ));
    }
    initialized.insert(register);
    Ok(())
}

fn check_column_index(index: u32, width: usize, path: &str, role: &str) -> Result<()> {
    if usize::try_from(index).map_or(true, |index| index >= width) {
        return Err(invalid(
            path,
            format!("{role} column {index} is outside schema width {width}"),
        ));
    }
    Ok(())
}

fn ensure_same_schema(
    actual: &VeinSchema,
    expected: &VeinSchema,
    path: &str,
    operator: &str,
) -> Result<()> {
    if actual.fields.len() != expected.fields.len()
        || actual
            .fields
            .iter()
            .zip(&expected.fields)
            .any(|(actual, expected)| {
                actual.data_type != expected.data_type || actual.nullable != expected.nullable
            })
    {
        return Err(invalid(
            path,
            format!("{operator} schema does not match its input schema"),
        ));
    }
    Ok(())
}

fn invalid(path: &str, message: impl std::fmt::Display) -> VeinError {
    VeinError::planning(format!("invalid physical plan at {path}: {message}"), None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{
        AggregatePlan, DataSource, JoinKind, JoinPlan, LimitSpec, PhysicalPlanNode, SortPlan,
    };
    use std::sync::Arc;
    use types::{ColumnId, ResolvedColumn, VeinDataType};

    fn schema(name: &str) -> Arc<VeinSchema> {
        Arc::new(VeinSchema::new(vec![Arc::new(ResolvedColumn::new(
            ColumnId(0),
            name,
            None,
            VeinDataType::Int64,
        ))]))
    }

    fn node(schema: Arc<VeinSchema>, operator: PhysicalPlanOperator) -> Arc<PhysicalPlanNode> {
        Arc::new(PhysicalPlanNode {
            output_schema: schema,
            operator,
        })
    }

    fn pipeline(schema: Arc<VeinSchema>, program: MicroProgram) -> Arc<PhysicalPlanNode> {
        node(
            Arc::clone(&schema),
            PhysicalPlanOperator::Pipeline {
                source: Arc::new(DataSource::Memory),
                input_schema: schema,
                program: Arc::new(program),
            },
        )
    }

    fn client_plan(input: Arc<PhysicalPlanNode>, schema: Arc<VeinSchema>) -> PhysicalPlan {
        PhysicalPlan {
            root: node(
                Arc::clone(&schema),
                PhysicalPlanOperator::Sink {
                    input,
                    sink: Arc::new(DataSink::Client { schema }),
                },
            ),
        }
    }

    #[test]
    fn validates_recursive_passthrough_and_sink_schema_contracts() {
        let schema = schema("value");
        let source = pipeline(Arc::clone(&schema), MicroProgram::new(1, Vec::new()));
        let limited = node(
            Arc::clone(&schema),
            PhysicalPlanOperator::Limit {
                input: source,
                spec: LimitSpec {
                    offset: 2,
                    count: 5,
                },
            },
        );

        client_plan(limited, schema)
            .validate()
            .expect("valid recursive plan should pass schema validation");
    }

    #[test]
    fn accepts_zero_register_program_as_input_passthrough() {
        let schema = schema("value");
        let source = pipeline(Arc::clone(&schema), MicroProgram::empty());

        client_plan(source, Arc::clone(&schema))
            .validate()
            .expect("an empty program passes through its input without registers");
    }

    #[test]
    fn accepts_vm_instructions_after_projection() {
        let schema = schema("value");
        let source = pipeline(
            Arc::clone(&schema),
            MicroProgram::new(
                3,
                vec![
                    MicroOp::ProjectRegisters { out_regs: vec![0] },
                    MicroOp::LoadConstant {
                        dst: 1,
                        value: types::LogicalLiteral::Int64(1),
                    },
                    MicroOp::Add {
                        dst: 2,
                        src1: 0,
                        src2: 1,
                    },
                ],
            ),
        );

        client_plan(source, schema)
            .validate()
            .expect("projection updates the output layout without ending register execution");
    }

    #[test]
    fn reports_uninitialized_source_register_with_recursive_operator_path() {
        let schema = schema("available_column");
        let source = pipeline(
            Arc::clone(&schema),
            MicroProgram::new(2, vec![MicroOp::ProjectRegisters { out_regs: vec![1] }]),
        );
        let limited = node(
            Arc::clone(&schema),
            PhysicalPlanOperator::Limit {
                input: source,
                spec: LimitSpec {
                    offset: 0,
                    count: 1,
                },
            },
        );

        let error = client_plan(limited, schema)
            .validate()
            .expect_err("a program cannot project an unproduced input column");
        let message = error.to_string();
        assert!(message.contains("root.input.input.program[0]"), "{message}");
        assert!(message.contains("uninitialized register 1"), "{message}");
    }

    #[test]
    fn rejects_join_key_positions_outside_recursive_child_schema() {
        let side_schema = schema("key");
        let probe = pipeline(Arc::clone(&side_schema), MicroProgram::new(1, Vec::new()));
        let build = pipeline(Arc::clone(&side_schema), MicroProgram::new(1, Vec::new()));
        let joined = node(
            Arc::clone(&side_schema),
            PhysicalPlanOperator::Join {
                probe,
                build,
                plan: JoinPlan {
                    join_type: JoinKind::Inner,
                    probe_keys: vec![1],
                    build_keys: vec![0],
                    residual: None,
                    outputs: vec![JoinOutput::Probe(0)],
                    probe_types: vec![VeinDataType::Int64],
                    build_types: vec![VeinDataType::Int64],
                },
            },
        );

        let error = client_plan(joined, side_schema)
            .validate()
            .expect_err("join key must be inside the probe input schema");
        assert!(error.to_string().contains("probe key column 1"));
    }

    #[test]
    fn validates_join_output_positions_nullability_and_repeated_columns() {
        let input_schema = schema("key");
        let probe = pipeline(Arc::clone(&input_schema), MicroProgram::new(1, Vec::new()));
        let build = pipeline(Arc::clone(&input_schema), MicroProgram::new(1, Vec::new()));
        let output_schema = Arc::new(VeinSchema::new(vec![
            Arc::new(ResolvedColumn::new(
                ColumnId(1),
                "left_key",
                None,
                VeinDataType::Int64,
            )),
            Arc::new(
                ResolvedColumn::new(ColumnId(2), "right_key", None, VeinDataType::Int64)
                    .with_nullable(true),
            ),
        ]));

        let valid_left_join = node(
            Arc::clone(&output_schema),
            PhysicalPlanOperator::Join {
                probe: Arc::clone(&probe),
                build: Arc::clone(&build),
                plan: JoinPlan {
                    join_type: JoinKind::Left,
                    probe_keys: vec![0],
                    build_keys: vec![0],
                    residual: None,
                    outputs: vec![JoinOutput::Probe(0), JoinOutput::Build(0)],
                    probe_types: vec![VeinDataType::Int64],
                    build_types: vec![VeinDataType::Int64],
                },
            },
        );
        client_plan(valid_left_join, Arc::clone(&output_schema))
            .validate()
            .expect("left join must make unmatched build-side outputs nullable");

        let invalid_output = node(
            Arc::clone(&input_schema),
            PhysicalPlanOperator::Join {
                probe: Arc::clone(&probe),
                build: Arc::clone(&build),
                plan: JoinPlan {
                    join_type: JoinKind::Inner,
                    probe_keys: vec![0],
                    build_keys: vec![0],
                    residual: None,
                    outputs: vec![JoinOutput::Build(1)],
                    probe_types: vec![VeinDataType::Int64],
                    build_types: vec![VeinDataType::Int64],
                },
            },
        );
        let error = client_plan(invalid_output, Arc::clone(&input_schema))
            .validate()
            .expect_err("join output positions must resolve on the declared child side");
        assert!(error.to_string().contains("build output column 1"));

        let repeated_output_schema = Arc::new(VeinSchema::new(vec![
            Arc::new(ResolvedColumn::new(
                ColumnId(3),
                "first_copy",
                None,
                VeinDataType::Int64,
            )),
            Arc::new(ResolvedColumn::new(
                ColumnId(4),
                "second_copy",
                None,
                VeinDataType::Int64,
            )),
        ]));
        let repeated_output = node(
            Arc::clone(&repeated_output_schema),
            PhysicalPlanOperator::Join {
                probe,
                build,
                plan: JoinPlan {
                    join_type: JoinKind::Inner,
                    probe_keys: vec![0],
                    build_keys: vec![0],
                    residual: None,
                    outputs: vec![JoinOutput::Probe(0), JoinOutput::Probe(0)],
                    probe_types: vec![VeinDataType::Int64],
                    build_types: vec![VeinDataType::Int64],
                },
            },
        );
        client_plan(repeated_output, repeated_output_schema)
            .validate()
            .expect("repeated output positions are valid projections");
    }

    #[test]
    fn rejects_aggregate_group_and_sort_key_positions_outside_schema() {
        let schema = schema("value");
        let aggregate = node(
            Arc::clone(&schema),
            PhysicalPlanOperator::Aggregate {
                input: pipeline(Arc::clone(&schema), MicroProgram::new(1, Vec::new())),
                plan: AggregatePlan {
                    groups: 1,
                    estimated_groups: None,
                    grouping_sets: Some(vec![vec![1]]),
                    emit_empty_group_on_empty_input: true,
                    aggs: Vec::new(),
                    aggregate_input_types: Vec::new(),
                    types: vec![VeinDataType::Int64],
                },
            },
        );
        let error = client_plan(aggregate, Arc::clone(&schema))
            .validate()
            .expect_err("grouping set positions must be within the declared key width");
        assert!(error.to_string().contains("grouping set 0 column 1"));

        let sorted = node(
            Arc::clone(&schema),
            PhysicalPlanOperator::Sort {
                input: pipeline(Arc::clone(&schema), MicroProgram::new(1, Vec::new())),
                plan: SortPlan {
                    keys: vec![crate::protocol::SortKey {
                        column: 1,
                        ascending: true,
                        nulls_first: false,
                    }],
                },
                key_program: None,
                output_columns: 1,
            },
        );
        let error = client_plan(sorted, schema)
            .validate()
            .expect_err("sort keys must resolve against the key schema");
        assert!(error.to_string().contains("sort key column 1"));
    }

    #[test]
    fn rejects_non_sink_root_and_malformed_aggregate_widths() {
        let schema = schema("value");
        let source = pipeline(Arc::clone(&schema), MicroProgram::new(1, Vec::new()));
        let rootless = PhysicalPlan {
            root: node(
                Arc::clone(&schema),
                PhysicalPlanOperator::Sort {
                    input: source,
                    plan: SortPlan { keys: Vec::new() },
                    key_program: None,
                    output_columns: 1,
                },
            ),
        };
        assert!(
            rootless
                .validate()
                .unwrap_err()
                .to_string()
                .contains("root operator must be a sink")
        );

        let aggregate = node(
            Arc::clone(&schema),
            PhysicalPlanOperator::Aggregate {
                input: pipeline(Arc::clone(&schema), MicroProgram::new(1, Vec::new())),
                plan: AggregatePlan {
                    groups: 1,
                    estimated_groups: None,
                    grouping_sets: None,
                    emit_empty_group_on_empty_input: true,
                    aggs: Vec::new(),
                    aggregate_input_types: Vec::new(),
                    types: Vec::new(),
                },
            },
        );
        let error = client_plan(aggregate, schema)
            .validate()
            .expect_err("aggregate output metadata must match its declared schema");
        assert!(
            error.to_string().contains(
                "aggregate operation, input type, output type, and schema widths disagree"
            )
        );
    }

    #[test]
    fn validates_distinct_grouping_contract_and_empty_schema() {
        let schema = schema("value");
        let valid = node(
            Arc::clone(&schema),
            PhysicalPlanOperator::Distinct {
                input: pipeline(Arc::clone(&schema), MicroProgram::new(1, Vec::new())),
                plan: AggregatePlan {
                    groups: 1,
                    estimated_groups: None,
                    grouping_sets: Some(vec![vec![0]]),
                    emit_empty_group_on_empty_input: false,
                    aggs: Vec::new(),
                    aggregate_input_types: Vec::new(),
                    types: vec![VeinDataType::Int64],
                },
            },
        );
        client_plan(valid, Arc::clone(&schema))
            .validate()
            .expect("distinct should group its complete input schema");

        let empty_schema = Arc::new(VeinSchema::empty());
        let zero_column_distinct = node(
            Arc::clone(&empty_schema),
            PhysicalPlanOperator::Distinct {
                input: pipeline(Arc::clone(&empty_schema), MicroProgram::new(0, Vec::new())),
                plan: AggregatePlan {
                    groups: 0,
                    estimated_groups: None,
                    grouping_sets: Some(vec![vec![]]),
                    emit_empty_group_on_empty_input: false,
                    aggs: Vec::new(),
                    aggregate_input_types: Vec::new(),
                    types: Vec::new(),
                },
            },
        );
        client_plan(zero_column_distinct, empty_schema)
            .validate()
            .expect("zero-column distinct should preserve its empty grouping set");

        let malformed = node(
            Arc::clone(&schema),
            PhysicalPlanOperator::Distinct {
                input: pipeline(Arc::clone(&schema), MicroProgram::new(1, Vec::new())),
                plan: AggregatePlan {
                    groups: 1,
                    estimated_groups: None,
                    grouping_sets: Some(vec![vec![]]),
                    emit_empty_group_on_empty_input: false,
                    aggs: Vec::new(),
                    aggregate_input_types: Vec::new(),
                    types: vec![VeinDataType::Int64],
                },
            },
        );
        let error = client_plan(malformed, schema)
            .validate()
            .expect_err("distinct must not omit input columns from its grouping keys");
        assert!(
            error
                .to_string()
                .contains("distinct must group every input column")
        );
    }
}
