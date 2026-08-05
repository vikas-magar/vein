# LLD: Vectorized Execution Engine & Morsel Dispatcher
**Component**: `crates/exec` and `crates/common`
**Epic**: 1. The Vectorized Type 4 Engine
**Date**: 2026-08-05

## 1. Overview
This document details the low-level design for Vein's transition to a Morsel-driven, Vectorized execution engine. It replaces the naive `RecordBatch` pushing mechanism with SIMD-friendly `DataChunk`s, `SelectionVector`s, and a Two-Tier Distributed lock-free morsel dispatcher.

## 2. Core Data Structures

### 2.1 `SelectionVector` (`crates/common/src/arrow.rs`)
Used to avoid copying data during filter operations.
```rust
/// A mask indicating which row indices in a ValueVector are valid.
#[derive(Debug, Clone)]
pub struct SelectionVector {
    /// Array of valid indices. If None, all rows [0..size) are valid.
    indices: Option<Vec<u32>>,
    /// Number of valid rows currently selected.
    size: usize,
}

impl SelectionVector {
    pub fn new_dense(size: usize) -> Self { ... }
    pub fn new_sparse(indices: Vec<u32>) -> Self { ... }
    
    /// Returns the true physical index for a given logical index.
    #[inline(always)]
    pub fn get_index(&self, logical_idx: usize) -> usize { ... }
    
    pub fn size(&self) -> usize { self.size }
}
```

### 2.2 `DataChunk` & `VeinBatch` (`crates/exec/src/daemon/pipeline.rs`)
The primary unit of data passed between operators.
```rust
use arrow::array::ArrayRef;
use common::arrow::SelectionVector;

/// A physical column of data (wraps Arrow Arrays for now).
#[derive(Debug, Clone)]
pub struct ValueVector {
    pub data: ArrayRef,
}

/// A horizontal slice of data processed in a pipeline.
#[derive(Debug, Clone)]
pub struct DataChunk {
    pub vectors: Vec<ValueVector>,
    pub selection: SelectionVector,
}

/// Prepares the engine for Phase 3 (Factorization).
pub enum VeinBatch {
    Flat(DataChunk),
    Factorized(()), // Placeholder for Epic 3
}
```

## 3. The Pipeline Abstraction

### 3.1 `PipelineNode` Trait (`crates/exec/src/daemon/pipeline.rs`)
Operators are strictly synchronous to guarantee pure CPU execution without `.await` yielding.
```rust
pub trait PipelineNode: Send + Sync {
    /// Pushes a chunk of data into the operator. 
    /// Returns `Ok(())` if the pipeline should continue processing this morsel.
    fn push(&mut self, batch: VeinBatch) -> common::Result<()>;
    
    /// Signals the end of a morsel, forcing operators to flush internal state.
    fn flush(&mut self) -> common::Result<()>;
}
```

## 4. Two-Tier Distributed Scheduling (Type 5)

### 4.1 Local Lock-Free Dispatcher (`crates/exec/src/operators/source/scan.rs`)
Once a Compute node receives a Macro-Morsel from the Master, it uses hardware atomics to distribute work among its physical cores without OS lock contention.
```rust
use std::sync::atomic::{AtomicUsize, Ordering};

/// Shared state across all local workers executing a specific Macro-Morsel.
pub struct SharedScanState {
    /// Total number of micro-morsels (e.g., 100k row chunks) available locally.
    pub total_morsels: usize,
    /// The next micro-morsel index to be claimed (Lock-Free).
    pub current_morsel_idx: AtomicUsize,
}

impl SharedScanState {
    /// Hardware atomic increment. No Mutex, no waiting, no OS involvement.
    #[inline(always)]
    pub fn next_morsel(&self) -> Option<usize> {
        let claimed = self.current_morsel_idx.fetch_add(1, Ordering::Relaxed);
        if claimed < self.total_morsels {
            Some(claimed)
        } else {
            None
        }
    }
}
```

### 4.2 `WorkerPool` (Dual-Pool Architecture) (`crates/exec/src/daemon/worker.rs`)
```rust
pub struct WorkerPool {
    /// Pure compute threads pinned to CPU cores via Tokio LocalSets.
    cpu_handles: Vec<std::thread::JoinHandle<()>>,
    /// Multi-threaded I/O runtime for network (QUIC) and disk (VFS).
    io_runtime: Option<tokio::runtime::Runtime>,
}
```
**Concurrency Flow**:
1. `Master` assigns a 1GB **Macro-Morsel** to the `WorkerPool` over the network.
2. `WorkerPool` initializes `SharedScanState` with 10,000 Micro-Morsels.
3. 128 OS threads concurrently call `fetch_add()` to steal Micro-Morsels.
4. Threads read VFS blocks (yielding to `io_runtime` for physical I/O).
5. Threads run the synchronous `PipelineNode::push()` SIMD loop strictly in CPU memory.
6. Local state is pushed back to VFS over QUIC.
7. If the node falls behind, Master cancels the Task, takes back remaining Macro-Morsel boundaries, and re-dispatches to idle nodes.

## 5. Sequence Diagram: Distributed Morsel Execution
1. `Master` dispatches `ExecuteTask` (Macro-Morsel: VFS Blocks 0-100) to Worker Node A.
2. Worker `io_runtime` receives Task via QUIC, creates `SharedScanState`.
3. Idle `cpu_handle` (Core 1) pulls Task.
4. Core 1 calls `fetch_add(1)`, atomically claims Micro-Morsel 0.
5. Core 2 calls `fetch_add(1)`, atomically claims Micro-Morsel 1.
6. Core 1 applies `FilterNode` predicates, updates `SelectionVector` (zero-copy).
7. Core 1 `VfsSink` pushes final filtered `VeinBatch` to VFS.
8. Core 1 loops back to step 4 instantly until `fetch_add` exceeds bounds.
9. Node A notifies Master: "Macro-Morsel Complete".
