# LLD: Vectorized Execution Engine & Morsel Dispatcher
**Component**: `crates/exec` and `crates/common`
**Epic**: 1. The Vectorized Type 4 Engine
**Date**: 2026-08-05

## 1. Overview
This document details the low-level design for Vein's transition to a Morsel-driven, Vectorized execution engine. It replaces the naive `RecordBatch` pushing mechanism with SIMD-friendly `DataChunk`s, `SelectionVector`s, and a Type 4 `SharedState` morsel dispatcher.

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

## 4. Morsel Dispatch & Concurrency (Type 4 Scheduler)

### 4.1 `SharedScanState` (`crates/exec/src/operators/source/scan.rs`)
The mechanism for cooperative multitasking across threads reading the same table.
```rust
use std::sync::Mutex;

/// Shared state across all workers executing a specific ScanNode.
pub struct SharedScanState {
    /// Total number of morsels (e.g., Parquet row groups) available.
    pub total_morsels: usize,
    /// The next morsel index to be claimed.
    pub current_morsel_idx: Mutex<usize>,
}

impl SharedScanState {
    /// Atomically claims the next morsel. Returns None if all morsels are claimed.
    pub fn next_morsel(&self) -> Option<usize> {
        let mut idx = self.current_morsel_idx.lock().unwrap();
        if *idx < self.total_morsels {
            let claimed = *idx;
            *idx += 1;
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
1. `WorkerPool` spins up N OS threads for Compute.
2. An idle CPU thread pulls a `MorselTask`.
3. The thread calls `SharedScanState::next_morsel()`.
4. It reads the VFS block (yielding to the `io_runtime` for the physical I/O).
5. It runs the synchronous `PipelineNode::push()` loop entirely in CPU memory.
6. Pipeline breakers (or `VfsSink`) yield to `io_runtime` to flush over QUIC.

## 5. Sequence Diagram: Morsel Execution
1. `Master` dispatches `ExecuteTask` to Worker Node.
2. Worker `io_runtime` receives Task via QUIC.
3. Task is queued into `flume::Receiver<MorselTask>`.
4. Idle `cpu_handle` pulls `MorselTask`.
5. Thread locks `SharedScanState`, gets `morsel_idx = 0`.
6. Thread scans row group 0 -> emits `DataChunk`.
7. `FilterNode` applies predicates, updates `SelectionVector` (zero-copy).
8. `VfsSink` pushes final filtered `VeinBatch` to VFS.
9. Thread loops back to step 5 until `next_morsel()` returns `None`.
