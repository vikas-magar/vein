//! Join residual expression attachment shared by recursive join builders.

use super::*;

pub(super) fn install_join_residual(
    builder: &dyn ExecutorBuilder,
    plan: &JoinPlan,
    join: HashJoin,
) -> Result<HashJoin> {
    let Some(residual) = &plan.residual else {
        return Ok(join);
    };
    let collector = JoinFilterCollector::default();
    let expression =
        builder.build_expression_executor(&residual.program, Box::new(collector.clone()))?;
    Ok(join.with_residual_filter(
        join_outputs(&residual.input_columns),
        expression,
        collector,
    ))
}
