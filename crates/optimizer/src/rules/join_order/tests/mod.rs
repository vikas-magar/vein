use std::sync::Arc;

use parser::ast::{expr::BinaryOp, pipe_op::JoinType};
use catalog::{DataUnitStatistics, StatisticsUnitKind};
use plan::{
    ColumnId, ColumnStatistics, LogicalExpr, LogicalPlan, ResolvedColumn,
    TableStatistics, VeinDataType,
};

use super::*;
use crate::{OptimizerConfig, OptimizerPipeline};


mod support;
use support::*;
mod checkpoints;
mod costing;
mod boundaries;
mod greedy;
