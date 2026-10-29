//! Structural validation for the canonical physical plan.

use crate::{Result, VeinError};

use super::{
    expression::ExpressionProgram,
    task::{
        DataSink, DataSource, JoinOutput, PhysicalPlan, PhysicalPlanNode, PhysicalPlanOperator,
    },
};
use types::VeinSchema;

#[path = "expression.rs"]
mod expression_validation;

impl PhysicalPlan {
    /// Validates operator inputs, expression references, and schema boundaries
    /// throughout this plan before it is scheduled or executed.
    pub fn validate(&self) -> Result<()> {
        if !matches!(&self.root.operator, PhysicalPlanOperator::Sink { .. })
            && !matches!(&self.root.operator, PhysicalPlanOperator::Extension { descriptor, .. }
                if descriptor.role == crate::protocol::extension::OperatorRole::Sink) {
            return Err(invalid("root", "the root operator must be a sink"));
        }
        validate_node(&self.root, "root", true)
    }
}

mod join;
mod reduction;
mod order;
mod extension;
mod utilities;
use utilities::*;

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
        PhysicalPlanOperator::Join { probe, build, plan } => join::validate(node, probe, build, plan, path)?,
        PhysicalPlanOperator::Aggregate { input, plan } => reduction::aggregate(node, input, plan, path)?,
        PhysicalPlanOperator::Distinct { input, plan } => reduction::distinct(node, input, plan, path)?,
        PhysicalPlanOperator::Sort { input, plan, key_program, output_columns } =>
            order::validate(node, input, plan, key_program.as_deref(), *output_columns, path)?,
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
        PhysicalPlanOperator::Extension { descriptor, inputs } =>
            extension::validate(descriptor, inputs, path, root)?,
    }
    Ok(())
}

#[cfg(test)]
mod tests;
