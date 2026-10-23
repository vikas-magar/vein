use arrow::datatypes::DataType;

use common::Result;
use common::protocol::{
    JoinKind, JoinOutput,
};

use crate::operators::join::{JoinKeys, JoinType, OutputColumn};
use types::VeinDataType;


pub(super) fn join_keys(columns: &[u32]) -> JoinKeys {
    JoinKeys::new(columns.iter().map(|column| *column as usize).collect())
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
        JoinKind::RightSemi => JoinType::RightSemi,
        JoinKind::RightAnti => JoinType::RightAnti,
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
