use super::*;
use crate::operators::source::MemorySource;
use crate::spill::{LocalSpillStorage, SpillStorage};
use arrow::array::Array;
use common::identifiers::{QueryId, RequestId, StageId, TaskId};
use common::protocol::{
    AggOpCode, AggregatePlan, ExecutionMode, FileFormat, FileScanTask, JoinKind, JoinOutput,
    JoinPlan, LimitSpec, MicroOp, MicroProgram, PhysicalPlan, PhysicalPlanNode, PhysicalPlanOperator,
    SortKey, SortPlan,
};
use std::sync::atomic::AtomicUsize;

mod fixtures;
mod fan_in;
mod join_execution;
mod join_semantics;
mod memory;
mod operator_contract;
mod parallel_execution;
mod recursive_joins;
mod recursive_pipeline;
mod scan;
mod spill;
mod task_output;
mod view_contract;
mod view_streaming;
mod zero_copy_e2e;
mod vm_fusion;

pub(in crate::runtime::tests) use fixtures::*;
