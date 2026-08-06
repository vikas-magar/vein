# LLD: Factorized Execution Engine
**Component**: `crates/exec`
**Epic**: 3. Factorized Execution
**Date**: 2026-08-06

## 1. Overview
This document outlines the architecture for **Factorized Execution**, directly implementing the research from Kuzu and LadybugDB. 

In a traditional database, joining a `Users` table with a `Purchases` table "flattens" the data. If a user has 1,000 purchases, the user's name and email are duplicated in RAM 1,000 times. For complex multi-way joins (Graph queries), this causes a catastrophic combinatorial explosion, wasting RAM and CPU cycles.

Factorized Execution solves this by keeping the data compressed in its naturally factored state during execution. The engine passes pointers/references instead of duplicating raw values.

## 2. Core Data Structures

### 2.1 `DataBlock` (Replacing flat ValueVectors)
We must shift from a flat `Vec<ValueVector>` into a grouped structure.
```rust
use crate::daemon::pipeline::ValueVector;
use common::arrow::SelectionVector;

/// A group of vectors that share the same cardinality and selection mask.
#[derive(Debug, Clone)]
pub struct DataBlock {
    pub vectors: Vec<ValueVector>,
    pub selection: SelectionVector,
}
```

### 2.2 `FactorizedChunk`
A `FactorizedChunk` represents a set of columns that may have different cardinalities (e.g., 1 User mapped to 1000 Purchases).
```rust
/// A factorized chunk containing multiple independent data blocks.
#[derive(Debug, Clone)]
pub struct FactorizedChunk {
    /// Each DataBlock represents a "factor" in the join.
    /// Block 0 might be Users (size 1). Block 1 might be Purchases (size 1000).
    pub blocks: Vec<DataBlock>,
}
```

### 2.3 `VeinBatch` Updates (`crates/exec/src/daemon/pipeline.rs`)
Update the `VeinBatch` enum we defined in Epic 1 to fully utilize the Factorized layout.
```rust
pub enum VeinBatch {
    Flat(crate::daemon::pipeline::DataChunk),
    /// The new factorized representation
    Factorized(FactorizedChunk), 
}
```

## 3. Operator Impacts

### 3.1 `HashJoinBuild`
When building a hash table on a factorized chunk, the operator no longer flattens the chunk into a single contiguous byte array. Instead, the Hash Table stores physical pointers to the individual `DataBlock` components.

### 3.2 `HashJoinProbe`
When probing, the operator evaluates the join condition. If there is a match, instead of copying all columns into a new flat block, it simply appends the matched `DataBlock` to the existing `FactorizedChunk`. 

This guarantees zero-copy joins for payload columns.

## 4. Implementation Phasing
*   **Task 3.1**: Implement `DataBlock` and `FactorizedChunk` structures in `crates/exec/src/daemon/pipeline.rs`.
*   **Task 3.2**: Create a `FactorizedTable` in-memory structure to serve as the storage backend for Hash Tables.
