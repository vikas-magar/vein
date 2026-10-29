use std::sync::Arc;

use common::Result;
use common::protocol::{JoinPlan, PhysicalPlanNode, PhysicalPlanOperator};
use error::VeinError;
use parser::ast::pipe_op::JoinType;
use plan::schema::VeinSchema;
use plan::LogicalPlan;

use super::{
    DefaultPipelineStageBuilder, PipelineCompiler, available_join_outputs, extract_equi_join_keys,
    flatten_conjunction, join_kind, position,
};

impl DefaultPipelineStageBuilder {
    pub(super) fn compile_join_input(
        &self,
        plan: &LogicalPlan,
    ) -> Result<(Arc<PhysicalPlanNode>, VeinSchema)> {
        let LogicalPlan::Join {
            join_type,
            left,
            right,
            on,
            using,
            schema,
            ..
        } = plan
        else {
            return self.lower_node(plan);
        };

        let (mut probe, probe_schema) = self.lower_node(left)?;
        let (mut build, build_schema) = self.lower_node(right)?;

        let mut probe_keys = Vec::new();
        let mut build_keys = Vec::new();
        let residual;
        if *join_type == JoinType::Cross {
            if on.is_some() || !using.is_empty() {
                return Err(VeinError::planning(
                    "CROSS JOIN cannot contain an ON or USING condition",
                    None,
                ));
            }
            residual = None;
        } else if using.is_empty() {
            if on.is_none() {
                return Err(VeinError::planning("non-CROSS JOIN requires ON or USING", None));
            }
            let mut conjuncts = Vec::new();
            if let Some(on) = on {
                flatten_conjunction(on, &mut conjuncts);
            }
            let (keys, residual_conjuncts) =
                extract_equi_join_keys(&conjuncts, &probe_schema, &build_schema)?;
            (probe_keys, build_keys) = keys;
            if probe_keys.is_empty() && *join_type == JoinType::NullAwareAnti {
                return Err(VeinError::unsupported(
                    "NULL-aware anti joins require a supported equality key", None,
                ));
            }
            residual = PipelineCompiler::compile_join_residual(
                &residual_conjuncts,
                &probe_schema,
                &build_schema,
                probe_keys.first().copied().unwrap_or_default(),
            )?;
        } else {
            for column in using {
                probe_keys.push(position(&probe_schema, &column.name)?);
                build_keys.push(position(&build_schema, &column.name)?);
            }
            let mut conjuncts = Vec::new();
            if let Some(on) = on {
                flatten_conjunction(on, &mut conjuncts);
            }
            residual = PipelineCompiler::compile_join_residual(
                &conjuncts.into_iter().cloned().collect::<Vec<_>>(),
                &probe_schema,
                &build_schema,
                probe_keys.first().copied().unwrap_or_default(),
            )?;
        }

        let (outputs, output_schema) =
            available_join_outputs(schema, &probe_schema, &build_schema)?;
        let mut join = JoinPlan {
            join_type: join_kind(join_type),
            probe_keys,
            build_keys,
            residual,
            outputs,
            probe_types: probe_schema
                .fields
                .iter()
                .map(|field| field.data_type.clone())
                .collect(),
            build_types: build_schema
                .fields
                .iter()
                .map(|field| field.data_type.clone())
                .collect(),
        };
        if (matches!(join_type, JoinType::Inner | JoinType::Cross)
            || (matches!(join_type, JoinType::Semi | JoinType::Anti)
                && on.as_ref().is_some_and(plan::LogicalExpr::is_reorder_safe)))
            && let Some((left_rows, right_rows)) = self.observed_rows(left).zip(self.observed_rows(right)) {
                if left_rows < right_rows {
                    join.join_type = join.join_type.inverse().ok_or_else(||
                        VeinError::internal("join orientation policy selected a non-invertible join"))?;
                    std::mem::swap(&mut probe, &mut build);
                    std::mem::swap(&mut join.probe_keys, &mut join.build_keys);
                    std::mem::swap(&mut join.probe_types, &mut join.build_types);
                    for output in &mut join.outputs { swap_output(output); }
                    if let Some(residual) = &mut join.residual {
                        for input in &mut residual.input_columns { swap_output(input); }
                    }
                }
                tracing::debug!(target: "vein.adaptive", event = "physical_join_build_orientation",
                    left_rows, right_rows, swapped = left_rows < right_rows,
                    "physical join builds the smaller estimated population");
            }
        let output_schema = Arc::new(output_schema);
        Ok((
            Arc::new(PhysicalPlanNode {
                output_schema: Arc::clone(&output_schema),
                operator: PhysicalPlanOperator::Join {
                    probe,
                    build,
                    plan: join,
                },
            }),
            (*output_schema).clone(),
        ))
    }
}

fn swap_output(output: &mut common::protocol::JoinOutput) {
    *output = match *output {
        common::protocol::JoinOutput::Probe(index) => common::protocol::JoinOutput::Build(index),
        common::protocol::JoinOutput::Build(index) => common::protocol::JoinOutput::Probe(index),
    };
}
