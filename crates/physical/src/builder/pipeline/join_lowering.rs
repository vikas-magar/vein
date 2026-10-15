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

        let (probe, probe_schema) = self.lower_node(left)?;
        let (build, build_schema) = self.lower_node(right)?;

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
            let mut conjuncts = Vec::new();
            if let Some(on) = on {
                flatten_conjunction(on, &mut conjuncts);
            }
            let (keys, residual_conjuncts) =
                extract_equi_join_keys(&conjuncts, &probe_schema, &build_schema)?;
            (probe_keys, build_keys) = keys;
            residual = PipelineCompiler::compile_join_residual(
                &residual_conjuncts,
                &probe_schema,
                &build_schema,
                probe_keys[0],
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
                probe_keys[0],
            )?;
        }

        let (outputs, output_schema) =
            available_join_outputs(schema, &probe_schema, &build_schema)?;
        let join = JoinPlan {
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
