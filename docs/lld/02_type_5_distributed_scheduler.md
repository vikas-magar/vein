# LLD: Type 5 Distributed Scheduler (Elasticity) & VFS Sink
**Component**: `crates/master`, `crates/exec`, `crates/common`
**Epic**: 2. Type 5 Distributed Scheduler
**Date**: 2026-08-05

## 1. Overview
This document details the low-level design for Vein's Type 5 Distributed Elasticity. While Epic 1 established local lock-free execution (Micro-Morsels), Epic 2 establishes the global network layer (Macro-Morsels). It enables the Master node to preempt slow workers, reallocate boundaries dynamically, and allows Compute nodes to push Hash Join partitions directly to the Virtual File System (VFS) to remain memory-bounded.

## 2. Core RPC Protocol Extensions

### 2.1 `Telemetry` & `Preempt` (`crates/common/src/protocol.rs`)
Compute nodes must stream execution metrics to the Master to facilitate Type 5 work-stealing.
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskTelemetry {
    pub task_id: TaskId,
    pub total_morsels_assigned: usize,
    pub morsels_completed: usize,
    pub rows_processed_per_sec: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MasterCommand {
    /// Dispatches a new Macro-Morsel to the worker
    DispatchTask(MorselTask),
    /// Forces the worker to pause and relinquish un-claimed Micro-Morsels
    PreemptTask { task_id: TaskId },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreemptResponse {
    pub task_id: TaskId,
    pub morsels_completed: usize,
    /// The boundary of work that the worker yielded back to the master
    pub morsels_yielded: usize, 
}
```

## 3. The Master Elasticity Engine

### 3.1 `ElasticityController` (`crates/master/src/scheduler/elasticity.rs`)
The logic running on the Master node that monitors telemetry and initiates Type 5 stealing.
```rust
pub struct ElasticityController {
    /// Tracks expected completion times for all active tasks
    active_tasks: HashMap<TaskId, TaskStatus>,
}

impl ElasticityController {
    /// Evaluates telemetry and identifies "straggler" nodes.
    /// A node is a straggler if its ETA > (Median ETA * 1.5).
    pub fn evaluate_stragglers(&mut self) -> Vec<TaskId> { ... }
    
    /// Re-allocates yielded work to idle or fast nodes.
    pub fn reallocate_yielded_work(&mut self, yielded: PreemptResponse) -> Vec<MorselTask> { ... }
}
```

## 4. The VFS Sink (Distributed Shuffle)

### 4.1 `VfsSink` (`crates/exec/src/operators/sink/vfs_sink.rs`)
To prevent OOM on massive Hash Joins, intermediate partitions are spilled directly to the VFS.
```rust
use crate::daemon::pipeline::{PipelineBreaker, VeinBatch};
use opendal::Operator;

pub struct VfsSink {
    /// The VFS storage interface
    fs: Operator,
    /// Partition buffers (e.g., 64 partitions for a Hash Join)
    buffers: Vec<Vec<VeinBatch>>,
    /// 16MB threshold before pushing a buffer over QUIC
    flush_threshold_bytes: usize,
}

impl VfsSink {
    /// Pushes a serialized Arrow block to the VFS over QUIC
    async fn flush_partition_to_vfs(&mut self, partition_id: usize) -> common::Result<()> { ... }
}

impl PipelineBreaker for VfsSink {
    fn push(&mut self, batch: VeinBatch) -> common::Result<()> {
        // 1. Hash the keys in the VeinBatch
        // 2. Route rows to appropriate partition buffer
        // 3. If buffer > flush_threshold_bytes, trigger async flush to VFS
        Ok(())
    }

    fn finalize(&mut self) -> common::Result<()> {
        // Flush all remaining non-empty buffers to the VFS
        // Signal the Master that this Pipeline is fully materialized on disk
        Ok(())
    }
}
```

## 5. Sequence Diagram: Type 5 Work Stealing
1. `Master` assigns Task A (Macro-Morsel: 10M rows) to `Worker 1`.
2. `Worker 1` begins lock-free processing and streams `TaskTelemetry` via QUIC.
3. `Master` detects `Worker 1` has hit extreme data skew (rows/sec is dropping).
4. `Master` sends `MasterCommand::PreemptTask(Task A)` to `Worker 1`.
5. `Worker 1` intercepts command. The local `WorkerPool` caps the `total_morsels` in `SharedScanState`.
6. `Worker 1` finishes its current in-flight SIMD loops, and replies with `PreemptResponse` (Yielded: 6M rows).
7. `Master` splits the 6M rows into Task B and Task C.
8. `Master` assigns Task B to `Worker 2` and Task C to `Worker 3`.
