# Vein Next-Gen Architecture Implementation Plan (Phase 1.1)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor the `WorkerPool` to use the DuckDB dual-pool pattern (CPU + Async I/O) and introduce `VeinBatch` to support future factorized execution.

**Architecture:** We are transitioning to a Type 4 Work-Stealing Morsel Executor. To ensure pure CPU compute threads never block on I/O, we will split the current `WorkerPool` into two distinct Tokio runtimes: a `LocalSet` CPU pool and a standard multi-threaded I/O pool. We also prepare the pipeline for Factorized execution by wrapping `RecordBatch` in a `VeinBatch` enum.

**Tech Stack:** Rust, Tokio, Flume, Arrow

## Global Constraints

- No `unwrap()` or `expect()` in production code (except in initialization).
- All existing tests must continue to pass.
- Code should be clean, properly documented.

---

### Task 1: Introduce VeinBatch in Pipeline

**Files:**
- Modify: `crates/exec/src/daemon/pipeline.rs`
- Test: `crates/exec/src/daemon/pipeline_tests.rs` (new)

**Interfaces:**
- Produces: `enum VeinBatch { Flat(RecordBatch), Factorized(()) }`
- Produces: Updated `PipelineNode` trait accepting `VeinBatch`.

- [ ] **Step 1: Write the failing test**

```rust
// crates/exec/src/daemon/pipeline_tests.rs
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    struct MockNode;
    impl PipelineNode for MockNode {
        fn push(&mut self, batch: VeinBatch) -> common::Result<()> { Ok(()) }
        fn flush(&mut self) -> common::Result<()> { Ok(()) }
    }

    #[test]
    fn test_vein_batch_enum() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
        
        let v_batch = VeinBatch::Flat(batch);
        let mut node = MockNode;
        assert!(node.push(v_batch).is_ok());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p exec pipeline_tests`
Expected: FAIL (unresolved import `VeinBatch`)

- [ ] **Step 3: Write minimal implementation**

Modify `crates/exec/src/daemon/pipeline.rs`:
```rust
// Add to top of file
pub enum VeinBatch {
    Flat(RecordBatch),
    Factorized(()), // Placeholder for Phase 3
}

// Modify PipelineNode trait
pub trait PipelineNode: Send + Sync {
    fn push(&mut self, batch: VeinBatch) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p exec`
Expected: PASS (Note: This may break downstream operators like Filter; fix those compilation errors trivially by changing `RecordBatch` to `VeinBatch` and matching on `Flat`).

- [ ] **Step 5: Commit**

```bash
git add crates/exec/src/daemon/pipeline.rs crates/exec/src/daemon/pipeline_tests.rs
git commit -m "feat(exec): introduce VeinBatch for future factorized execution"
```

---

### Task 2: Dual-Pool Worker Architecture

**Files:**
- Modify: `crates/exec/src/daemon/worker.rs`

**Interfaces:**
- Consumes: `MorselTask`
- Produces: `WorkerPool` struct with distinct CPU and I/O runtimes.

- [ ] **Step 1: Write the failing test**

```rust
// crates/exec/src/daemon/worker_tests.rs
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_worker_pool_initialization() {
        let (tx, rx) = flume::unbounded();
        let pool = WorkerPool::start(2, rx);
        assert_eq!(pool.cpu_handles.len(), 2);
        assert!(pool.io_runtime.is_some());
        pool.join();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p exec worker_tests`
Expected: FAIL (no `cpu_handles` or `io_runtime` on `WorkerPool`)

- [ ] **Step 3: Write minimal implementation**

Modify `crates/exec/src/daemon/worker.rs`:
```rust
use tokio::runtime::Runtime;

pub struct WorkerPool {
    pub cpu_handles: Vec<JoinHandle<()>>,
    pub io_runtime: Option<Runtime>,
}

impl WorkerPool {
    pub fn start(cores: usize, queue: flume::Receiver<MorselTask>) -> Self {
        let io_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4) // Configurable later
            .thread_name("vein-io-worker")
            .enable_all()
            .build()
            .expect("Failed to build IO runtime");

        let mut cpu_handles = Vec::with_capacity(cores);

        for core_id in 0..cores {
            let queue_clone = queue.clone();
            
            let handle = thread::Builder::new()
                .name(format!("vein-cpu-{}", core_id))
                .spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
                    let local = LocalSet::new();
                    local.block_on(&rt, async move {
                        while let Ok(task) = queue_clone.recv_async().await {
                            // ... existing task execution logic ...
                        }
                    });
                })
                .unwrap();
            cpu_handles.push(handle);
        }

        Self { cpu_handles, io_runtime: Some(io_runtime) }
    }

    pub fn join(mut self) {
        for handle in self.cpu_handles {
            let _ = handle.join();
        }
        if let Some(rt) = self.io_runtime.take() {
            rt.shutdown_background();
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p exec`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/exec/src/daemon/worker.rs
git commit -m "feat(exec): implement DuckDB dual-pool threading pattern"
```
