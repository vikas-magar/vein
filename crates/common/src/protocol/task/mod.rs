//! Physical task definitions, execution modes, sources, and sinks.

use catalog::{CatalogSnapshotToken, DataFileFormatId, StatisticsPredicate, StorageFileMetadata};
use serde::{Deserialize, Serialize};
use std::ops::Range;
use std::sync::Arc;
use types::VeinSchemaRef;

use super::encoding::ColumnEncodingHint;
use super::expression::{ExpressionProgram, PhysicalExpr};
use crate::identifiers::{IntermediateSourceId, QueryId, RequestId, StageId, TaskId};
#[cfg(test)]
use crate::protocol::{AggOpCode, JoinKind};
use crate::statistics::QueryStatisticsContext;

mod source;
mod tree;
mod inputs;
mod checkpoint;
mod data_flow;
mod output;
mod execution;
mod reduction;

pub use source::{DataSource, FileScanTask, RuntimeStatisticsScan, ScanPredicate, ScanPredicateStage, StreamSource};
pub use tree::{CheckpointPlanFragments, PhysicalPlan, PhysicalPlanNode, PhysicalPlanOperator};
pub use inputs::PhysicalInputs;
pub use data_flow::{InputMaterialization, OperatorDataFlow, OutputRepresentation};
pub use output::{DataSink, KeyValue, StorageTarget};
pub use execution::{ExecuteTask, ExecutionMode};
pub use reduction::{AggregatePlan, JoinOutput, JoinPlan, JoinResidualPlan, LimitSpec, SortKey, SortPlan};

#[cfg(test)]
mod tests;
