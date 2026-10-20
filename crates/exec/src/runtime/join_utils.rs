use arrow::datatypes::DataType;

use common::protocol::{
    DataSource, JoinKind, JoinOutput, JoinPlan, PhysicalPlanNode, PhysicalPlanOperator,
};
use common::identifiers::TaskId;
use common::Result;

use crate::operators::join::{JoinKeys, JoinType, OutputColumn};
use types::VeinDataType;

/// A plan's key columns as chunk column positions.
pub(super) fn direct_scan_probe_columns<'a>(
    probe: &'a PhysicalPlanNode,
    plan: &JoinPlan,
) -> Option<(&'a DataSource, Vec<types::ColumnId>)> {
    let PhysicalPlanOperator::Pipeline {
        source,
        input_schema,
        program,
    } = &probe.operator
    else {
        return None;
    };
    if !program.is_empty() {
        return None;
    }
    let DataSource::File(scan) = source.as_ref() else {
        return None;
    };
    if plan.probe_keys.is_empty() || plan.probe_keys.len() != plan.build_keys.len() {
        return None;
    }

    let mut columns = Vec::with_capacity(plan.probe_keys.len());
    for (probe_key, build_key) in plan.probe_keys.iter().zip(&plan.build_keys) {
        let probe_index = usize::try_from(*probe_key).ok()?;
        let build_index = usize::try_from(*build_key).ok()?;
        let output_column = probe.output_schema.fields.get(probe_index)?;
        let input_column = input_schema
            .fields
            .iter()
            .find(|column| column.id == output_column.id)?;
        let source_column = scan
            .projected
            .fields
            .iter()
            .find(|column| column.id == output_column.id)?;
        let probe_type = plan.probe_types.get(probe_index)?;
        let build_type = plan.build_types.get(build_index)?;
        if output_column.data_type != *probe_type
            || input_column.data_type != output_column.data_type
            || source_column.data_type != output_column.data_type
            || probe_type != build_type
        {
            return None;
        }
        columns.push(output_column.id);
    }
    Some((source.as_ref(), columns))
}

pub(super) fn join_keys(columns: &[u32]) -> JoinKeys {
    JoinKeys::new(columns.iter().map(|column| *column as usize).collect())
}

/// One source per file, so a join's build side can be read in parallel.
///
/// An ordinary scan spreads a table's files across the worker pool; the build
/// side of a join was read as a single source, so its files were walked one
/// after another — the largest serial cost in a join. The split mirrors the
/// physical builder's, which is `pub(crate)` there.
pub(crate) fn split_scan_by_file(source: &DataSource) -> Vec<DataSource> {
    let DataSource::File(scan) = source else {
        return vec![source.clone()];
    };
    if scan.files.len() <= 1 {
        return vec![source.clone()];
    }
    scan.files
        .iter()
        .map(|file| {
            let mut one = scan.clone();
            one.files = vec![file.clone()];
            one.task_id = TaskId::new();
            DataSource::File(one)
        })
        .collect()
}

/// A plan's join flavour as the operator's.
///
/// Semi and anti emit each qualifying probe row once and expose no build columns.
pub(super) fn join_type(kind: JoinKind) -> Result<JoinType> {
    Ok(match kind {
        JoinKind::Inner => JoinType::Inner,
        JoinKind::Left => JoinType::Left,
        JoinKind::Right => JoinType::Right,
        JoinKind::Full => JoinType::Full,
        JoinKind::Semi => JoinType::Semi,
        JoinKind::Anti => JoinType::Anti,
        JoinKind::NullAwareAnti => JoinType::NullAwareAnti,
        JoinKind::Single => JoinType::Single,
        JoinKind::Cross => JoinType::Cross,
    })
}

/// A plan's output layout as the operator's.
pub(super) fn join_outputs(outputs: &[JoinOutput]) -> Vec<OutputColumn> {
    outputs
        .iter()
        .map(|output| match output {
            JoinOutput::Probe(index) => OutputColumn::Probe(*index as usize),
            JoinOutput::Build(index) => OutputColumn::Build(*index as usize),
        })
        .collect()
}

/// Arrow types for one side's columns; a joined null column needs one.
pub(super) fn join_types(types: &[VeinDataType]) -> Vec<DataType> {
    types.iter().map(DataType::from).collect()
}
