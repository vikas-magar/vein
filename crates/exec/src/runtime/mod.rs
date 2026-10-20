//! The executor's execution seam: the traits that decouple pipeline
//! construction and task execution from the wire protocol and the concrete
//! stage implementations.
//!
//! These are the abstractions the rest of the engine programs against:
//! a [`ExecutorBuilder`] selects concrete [`Source`]/[`Transform`]/[`Sink`]
//! implementations from a wire task, and a [`TaskExecutor`] runs a single
//! task end-to-end (see `crate::pipeline`).

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::Instrument;

use self::filter::{RuntimeFilterBuildInput, RuntimeFilterSet};
use crate::memory::{ExecutionResourceLimits, QueryMemoryTracker};
use crate::operators::aggregate;
use crate::operators::join::hash_join::BuildResult;
use crate::operators::join::spill::{
    JoinSpillRun, JoinSpillSchema, JoinSpillWriter, open_reader, partition_runs, payload_view,
    remove_run,
};
use crate::operators::join::{BuildSink, HashJoin, HashTable, JoinFilterCollector};
use crate::operators::sink::ClientOutput;
use crate::operators::{LimitTransform, SortTransform};
use crate::pipeline::{DataChunkView, PipelineNode, Sink, Source, Transform};
use crate::spill::{LocalSpillStorageFactory, SpillStorageFactory};
use crate::trace::ExecutionTrace;
use common::Result;
use common::cancellation::CancellationToken;
use common::protocol::{
    DataSink, DataSource, ExecuteTask, JoinKind, JoinPlan, PhysicalPlanNode, PhysicalPlanOperator,
    PipelineBoundaryKind,
};
use common::statistics::QueryStatisticsContext;
use error::VeinError;

#[cfg(test)]
use crate::operators::join::{JoinKeys, JoinType};
#[cfg(test)]
use arrow::datatypes::DataType;
#[cfg(test)]
use async_trait::async_trait;

mod result;
pub use result::{TaskMetrics, TaskResult};

mod resources;
pub use resources::QueryExecutionResources;

mod dependency_guard;
mod join_utils;
mod schedule;
use dependency_guard::PipelineDependencyGuard;
pub(crate) use join_utils::split_scan_by_file;
use join_utils::{direct_scan_probe_columns, join_keys, join_outputs, join_type, join_types};
use schedule::PipelineExecutionSchedule;

mod builder;
pub mod checkpoint;
pub use builder::ExecutorBuilder;
mod task_executor;
pub use task_executor::TaskExecutor;
mod default_task_executor;
mod execution;
mod executor_config;
pub mod filter;
mod join_residual;
use join_residual::install_join_residual;

mod join_pipeline;
mod pipeline_adapters;
mod recursive;
use join_pipeline::{JoinProbeCounter, SharedBuildSink, SharedJoinOutput, SpilledJoinProbe};
use pipeline_adapters::{
    CancelGuard, ChannelPipelineNode, SinkHandle, SuppressFlush, TransformHandle,
};

/// Default task executor using an injected builder and query-scoped resources.
pub struct DefaultTaskExecutor {
    builder: Arc<dyn ExecutorBuilder>,
    resource_limits: ExecutionResourceLimits,
    spill_storage_factory: Arc<dyn SpillStorageFactory>,
    intermediate_sources: checkpoint::IntermediateSourceRegistry,
}

#[cfg(test)]
mod tests;
