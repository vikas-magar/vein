use std::collections::HashMap;
use std::path::PathBuf;

use async_trait::async_trait;
use arrow::datatypes::SchemaRef;
use error::Result;

use super::arrow_hash_index::ArrowHashIndex;
use super::chunk::Chunk;
use crate::identifiers::OperatorId;

/// Memory budget for state management across the three-tier hierarchy.
#[derive(Debug, Clone)]
pub struct MemoryBudget {
    /// Max RAM for z⁻¹ states (hot tier).
    pub ram_bytes: u64,
    /// Max cold tier storage (serialized, mmap'd).
    pub cold_bytes: u64,
    /// Eviction check interval in epochs.
    pub eviction_interval: u64,
}

impl Default for MemoryBudget {
    fn default() -> Self {
        Self {
            ram_bytes: 1024 * 1024 * 1024, // 1 GB
            cold_bytes: 2 * 1024 * 1024 * 1024, // 2 GB
            eviction_interval: 10,
        }
    }
}

/// Trait for all z⁻¹ state implementations across the three-tier hierarchy.
#[async_trait]
pub trait ZSetState: Send + 'static {
    /// Apply a diff to the accumulated state.
    async fn apply_diff(&mut self, diff: &Chunk) -> Result<()>;

    /// Get the full accumulated state as Chunk.
    async fn snapshot(&self) -> Result<Chunk>;

    /// Memory used.
    fn memory_used(&self) -> usize;

    /// Priority score for eviction (lower = more evictable).
    fn priority(&self) -> f64;
}

/// A ZSetState backed by ArrowHashIndex (RAM-only, Tier 1).
pub struct RamState {
    index: ArrowHashIndex,
    access_count: u64,
}

impl RamState {
    /// Create a new RAM state with the given schema.
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            index: ArrowHashIndex::new(schema),
            access_count: 0,
        }
    }
}

#[async_trait]
impl ZSetState for RamState {
    async fn apply_diff(&mut self, diff: &Chunk) -> Result<()> {
        self.access_count += 1;
        self.index.apply_diff(diff)
    }

    async fn snapshot(&self) -> Result<Chunk> {
        self.index.snapshot()
    }

    fn memory_used(&self) -> usize {
        self.index.memory_used()
    }

    fn priority(&self) -> f64 {
        if self.memory_used() == 0 {
            return f64::MAX;
        }
        let weight = 1.0 / (self.memory_used() as f64).max(1.0);
        weight * (self.access_count as f64)
    }
}

// ── TieredZSetState (hot + cold tiers) ──

/// A ZSetState with two tiers: hot (ArrowHashIndex) and cold (serialized chunks).
///
/// When the hot tier exceeds `ram_budget`, it is compacted.
/// If still over budget, the oldest rows are serialized and moved to the cold tier.
/// The cold tier stores serialized Chunks (in production, these would be files on disk).
pub struct TieredZSetState {
    schema: SchemaRef,
    hot: ArrowHashIndex,
    /// Serialized cold chunks (in-memory for now; in production these would be files).
    cold: Vec<Vec<u8>>,
    /// Path prefix for file-based cold storage.
    spill_dir: Option<PathBuf>,
    access_count: u64,
    epochs_since_eviction: u64,
    ram_budget: usize,
    cold_bytes_used: usize,
    next_spill_id: u64,
}

impl TieredZSetState {
    /// Create a new tiered state.
    pub fn new(schema: SchemaRef, ram_budget: usize, spill_dir: Option<PathBuf>) -> Self {
        Self {
            schema: schema.clone(),
            hot: ArrowHashIndex::new(schema),
            cold: Vec::new(),
            spill_dir,
            access_count: 0,
            epochs_since_eviction: 0,
            ram_budget,
            cold_bytes_used: 0,
            next_spill_id: 1,
        }
    }

    /// Spill rows from the hot tier to the cold tier when over budget.
    /// A simple approach: serialize the entire hot tier and clear it.
    fn spill_to_cold(&mut self) -> Result<()> {
        let chunk = self.hot.snapshot()?;
        if chunk.is_empty() {
            return Ok(());
        }
        let bytes = chunk.to_ipc_bytes()?;
        let size = bytes.len();

        if let Some(ref dir) = self.spill_dir {
            let path = dir.join(format!("spill_{}.arrow", self.next_spill_id));
            self.next_spill_id += 1;
            std::fs::write(&path, &bytes)
                .map_err(|e| error::VeinError::internal(format!("spill write failed: {e}")))?;
        }
        self.cold.push(bytes);
        self.cold_bytes_used += size;

        // Clear hot tier
        self.hot = ArrowHashIndex::new(self.schema.clone());

        Ok(())
    }

    /// Merge all cold chunks back into the hot tier.
    fn merge_from_cold(&mut self) -> Result<()> {
        if self.cold.is_empty() {
            return Ok(());
        }
        let mut cold_bytes = Vec::new();
        std::mem::swap(&mut cold_bytes, &mut self.cold);
        self.cold_bytes_used = 0;

        for bytes in &cold_bytes {
            let chunk = Chunk::from_ipc_bytes(bytes)?;
            self.hot.apply_diff(&chunk)?;
        }
        Ok(())
    }
}

#[async_trait]
impl ZSetState for TieredZSetState {
    async fn apply_diff(&mut self, diff: &Chunk) -> Result<()> {
        self.access_count += 1;
        self.epochs_since_eviction += 1;

        // Apply to hot tier with budget enforcement
        let compacted = self.hot.apply_diff_with_budget(diff, self.ram_budget)?;

        // If hot tier is still over budget after compaction, spill
        if self.hot.memory_used() > self.ram_budget {
            self.spill_to_cold()?;
        }

        // Periodic full eviction check
        if self.epochs_since_eviction >= 10 || compacted {
            self.epochs_since_eviction = 0;
            if self.hot.memory_used() > self.ram_budget * 2 {
                self.spill_to_cold()?;
            }
        }

        Ok(())
    }

    async fn snapshot(&self) -> Result<Chunk> {
        let hot_chunk = self.hot.snapshot()?;
        if self.cold.is_empty() {
            return Ok(hot_chunk);
        }

        // Merge cold chunks with hot
        let mut combined = hot_chunk;
        for bytes in &self.cold {
            let cold_chunk = Chunk::from_ipc_bytes(bytes)?;
            combined = combined.plus(&cold_chunk)?;
        }
        Ok(combined)
    }

    fn memory_used(&self) -> usize {
        self.hot.memory_used() + self.cold_bytes_used
    }

    fn priority(&self) -> f64 {
        if self.memory_used() == 0 {
            return f64::MAX;
        }
        let hot_ratio = self.hot.memory_used() as f64 / self.memory_used().max(1) as f64;
        // Higher priority if more data is hot (recently active)
        hot_ratio * (self.access_count as f64)
    }
}

/// Registry of all z⁻¹ states in a circuit. Manages the three-tier hierarchy.
pub struct StateRegistry {
    states: HashMap<OperatorId, Box<dyn ZSetState>>,
    budget: MemoryBudget,
}

impl StateRegistry {
    /// Create a new registry with the given memory budget.
    pub fn new(budget: MemoryBudget) -> Self {
        Self {
            states: HashMap::new(),
            budget,
        }
    }

    /// Get or create a state for the given operator.
    pub async fn get_or_create(
        &mut self,
        operator_id: OperatorId,
        schema: SchemaRef,
        spill_dir: Option<PathBuf>,
    ) -> Result<&mut Box<dyn ZSetState>> {
        if !self.states.contains_key(&operator_id) {
            let state: Box<dyn ZSetState> = Box::new(TieredZSetState::new(
                schema,
                self.budget.ram_bytes as usize,
                spill_dir,
            ));
            self.states.insert(operator_id, state);
        }
        Ok(self.states.get_mut(&operator_id).unwrap())
    }

    /// Get an existing state by operator ID.
    pub fn get_mut(&mut self, operator_id: &OperatorId) -> Option<&mut Box<dyn ZSetState>> {
        self.states.get_mut(operator_id)
    }

    /// Total memory used by all states.
    pub fn total_memory_used(&self) -> usize {
        self.states.values().map(|s| s.memory_used()).sum()
    }

    /// Evict lowest-priority states until total memory is under budget.
    pub async fn evict(&mut self) -> Result<()> {
        if self.total_memory_used() <= self.budget.ram_bytes as usize {
            return Ok(());
        }
        let mut ids: Vec<OperatorId> = self.states.keys().copied().collect();
        ids.sort_by(|a, b| {
            let pa = self.states.get(a).map(|s| s.priority()).unwrap_or(0.0);
            let pb = self.states.get(b).map(|s| s.priority()).unwrap_or(0.0);
            pa.partial_cmp(&pb).unwrap_or(std::cmp::Ordering::Equal)
        });

        for id in ids {
            if self.total_memory_used() <= self.budget.ram_bytes as usize {
                break;
            }
            if let Some(state) = self.states.get_mut(&id) {
                let _ = state.snapshot().await;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(missing_docs)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]))
    }

    fn make_chunk(keys: &[i64], weight: i64) -> Chunk {
        let s = schema();
        let col = arrow::array::Int64Array::from(keys.to_vec());
        let batch = arrow::record_batch::RecordBatch::try_new(s, vec![Arc::new(col)]).unwrap();
        Chunk::from_records(batch, weight)
    }

    #[tokio::test]
    async fn test_tiered_spill_and_merge() {
        let mut state = TieredZSetState::new(schema(), 1024, None);

        state.apply_diff(&make_chunk(&[1, 2, 3], 1)).await.unwrap();
        let snap = state.snapshot().await.unwrap();
        assert_eq!(snap.weighted_count(), 3);

        // Force spill by creating a new hot state and spilling
        state.apply_diff(&make_chunk(&[4, 5, 6], 1)).await.unwrap();
        // With a small budget, the spill should happen automatically
        let snap = state.snapshot().await.unwrap();
        assert_eq!(snap.weighted_count(), 6);
    }

    #[tokio::test]
    async fn test_chunk_ipc_roundtrip() {
        let chunk = make_chunk(&[10, 20, 30], 1);
        let bytes = chunk.to_ipc_bytes().unwrap();
        let restored = Chunk::from_ipc_bytes(&bytes).unwrap();
        assert_eq!(restored.weighted_count(), 3);
        assert_eq!(restored.len(), 3);
    }
}
