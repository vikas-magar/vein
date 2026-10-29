use super::*;

/// Mode of execution for a task.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum ExecutionMode {
    /// Finite batch execution (e.g., reading Parquet files).
    Batch,
    /// Infinite stream execution (DBSP incremental multi-epoch).
    Streaming,
}

/// Task dispatched from Master to Executor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteTask {
    /// Stable request lineage identifier shared by coordinator, scheduler, and executors.
    pub request_id: RequestId,
    /// Execution mode (Batch or Streaming).
    pub mode: ExecutionMode,
    /// Unique task identifier.
    pub task_id: TaskId,
    /// Retry attempt for this task, starting at zero.
    pub attempt: u32,
    /// Query this task belongs to.
    pub query_id: QueryId,
    /// Stage this task is part of.
    pub stage_id: StageId,
    /// The authoritative physical operator tree for this task.
    pub plan: PhysicalPlan,
    /// In-process query statistics shared by planning and local execution.
    ///
    /// This is deliberately excluded from the serialized task payload. A local
    /// coordinator attaches the context after planning; remote executors receive
    /// the self-contained plan and can provide their own local context.
    #[serde(skip)]
    pub statistics_context: Option<Arc<QueryStatisticsContext>>,
}
