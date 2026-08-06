# LLD: eBPF Pushdown (Kernel-Level Filtering)
**Component**: `crates/ebpf`, `crates/vfs`, `crates/common`
**Epic**: 4. eBPF Pushdown
**Date**: 2026-08-06

## 1. Overview
This document outlines the architecture for **eBPF Pushdown**, the defining feature of Vein. 

In traditional distributed databases, storage nodes read blocks from disk and send them over the network to compute nodes, which then apply `WHERE` filters. If a query filters out 99% of the rows, 99% of the network bandwidth was wasted.

Vein solves this by dynamically compiling SQL predicates into **eBPF (Extended Berkeley Packet Filter) bytecode**. This bytecode is sent to the VFS Storage Node via QUIC and injected directly into the Linux Kernel. The kernel itself evaluates the filter as the bytes are read from the NVMe SSD, ensuring only valid rows ever cross the network.

## 2. Core Components

### 2.1 The eBPF Compiler (`crates/ebpf/src/compiler.rs`)
Located on the Master or Compute node, this component traverses the physical expression tree and emits eBPF instructions.
```rust
use common::Result;

pub struct EbpfCompiler;

impl EbpfCompiler {
    /// Compiles a SQL physical expression into raw eBPF bytecode.
    /// In a production environment, this would use `aya` or LLVM bindings.
    pub fn compile_predicate(expr_ast: &str) -> Result<Vec<u8>> {
        // Implementation will generate raw eBPF ELF object bytes
        Ok(vec![])
    }
}
```

### 2.2 VFS eBPF Loader (`crates/vfs/src/ebpf/loader.rs`)
Located on the VFS Storage Daemon. It receives the bytecode over QUIC and safely loads it into the Linux Kernel using the `aya` crate.
```rust
use common::Result;

pub struct EbpfLoader {
    /// Tracks active eBPF programs loaded in the kernel
    active_programs: std::collections::HashMap<String, usize>,
}

impl EbpfLoader {
    pub fn new() -> Self {
        Self { active_programs: Default::default() }
    }

    /// Safely loads bytecode into the Linux Kernel via the `bpf()` syscall.
    pub fn load_filter(&mut self, filter_id: String, bytecode: &[u8]) -> Result<()> {
        // 1. Validate bytecode safely
        // 2. Load into kernel via Aya
        // 3. Attach to XDP or block device tracepoint
        Ok(())
    }
}
```

### 2.3 RPC Protocol Extension (`crates/common/src/protocol.rs`)
We must update our QUIC protocol to allow Compute nodes to push bytecode to VFS nodes.
```rust
#[derive(Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum VfsRequest {
    /// Read a block of data, applying the provided eBPF bytecode in the kernel.
    ReadWithFilter {
        file_path: String,
        /// The raw eBPF ELF bytecode
        ebpf_bytecode: Vec<u8>, 
    }
}
```

## 3. Sequence Diagram: The Pushdown Execution
1. **Master Node**: Receives `SELECT * FROM table WHERE age > 30`.
2. **EbpfCompiler**: Compiles `age > 30` into a 256-byte eBPF ELF binary.
3. **Master Node**: Dispatches `MorselTask` (containing the bytecode) to `Compute Node 1`.
4. **Compute Node 1**: Opens a QUIC stream to `VFS Node 1` and sends `VfsRequest::ReadWithFilter(bytecode)`.
5. **VFS Node 1**: Receives the bytecode, uses `EbpfLoader` to inject it into the Linux Kernel.
6. **Linux Kernel**: Reads NVMe blocks. Executes the eBPF program on every row natively. Only rows matching `age > 30` are returned to the VFS user-space daemon.
7. **VFS Node 1**: Sends the highly compressed, pre-filtered Arrow blocks back to `Compute Node 1`.
8. **Compute Node 1**: Executes the remainder of the query (e.g., Aggregations, Joins) on the 1% of data that survived the kernel filter.
