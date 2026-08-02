use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};

// TODO: Phase 5 — intermediate node representation (MerkleNode) will be added
// when the HEAL engine needs to expose per-node proofs for partial tree sync.

/// A Merkle Tree representation for anti-entropy sync in HEAL engine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MerkleTree {
    pub root_hash: [u8; 32],
    pub leaves: Vec<[u8; 32]>,
}

impl MerkleTree {
    /// Constructs a new `MerkleTree` from a list of raw block byte payloads.
    pub fn new(blocks: Vec<Vec<u8>>) -> Self {
        let leaves: Vec<[u8; 32]> = blocks
            .iter()
            .map(|block| *blake3::hash(block).as_bytes())
            .collect();

        let root_hash = if leaves.is_empty() {
            *blake3::hash(b"").as_bytes()
        } else {
            let mut hasher = blake3::Hasher::new();
            for leaf in &leaves {
                hasher.update(leaf);
            }
            *hasher.finalize().as_bytes()
        };

        Self { root_hash, leaves }
    }

    /// Returns the set difference of leaf hashes present in `other` but missing in `self`.
    /// If root hashes are equal, returns an empty list immediately.
    pub fn diff(&self, other: &MerkleTree) -> Vec<[u8; 32]> {
        if self.root_hash == other.root_hash {
            return Vec::new();
        }

        let self_set: std::collections::HashSet<&[u8; 32]> = self.leaves.iter().collect();
        other
            .leaves
            .iter()
            .filter(|hash| !self_set.contains(hash))
            .copied()
            .collect()
    }
}

/// Background daemon responsible for HEAL anti-entropy diffing.
pub struct HealDaemon {
    running: AtomicBool,
}

impl HealDaemon {
    pub fn new() -> Self {
        Self {
            running: AtomicBool::new(false),
        }
    }

    /// Returns `true` if `start()` has been called at least once.
    ///
    /// Note: this is fire-and-forget semantics. The flag is **not** reset when the
    /// backing task is aborted externally. For this stub, "running" means "has ever
    /// been started", which is sufficient for the double-start guard below.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Starts the HEAL anti-entropy background loop.
    ///
    /// Returns `Some(JoinHandle)` on the first call, and `None` on every subsequent
    /// call (double-start guard via `compare_exchange`). Callers must not start the
    /// daemon more than once; in production the handle is intentionally dropped
    /// (fire-and-forget — the task lives for the lifetime of the process).
    pub fn start(&self) -> Option<tokio::task::JoinHandle<()>> {
        // Atomically flip false → true. If the old value was already true another
        // caller beat us here; return None to signal "already running".
        if self
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return None;
        }
        Some(tokio::spawn(async move {
            loop {
                // TODO: Phase 5 — accept Arc<RwLock<ArchipelagoState>>, call diff() against peer roots.
                tracing::debug!("HEAL daemon anti-entropy check");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }))
    }
}

impl Default for HealDaemon {
    fn default() -> Self {
        Self::new()
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merkle_tree_construction() {
        let tree = MerkleTree::new(vec![b"block1".to_vec(), b"block2".to_vec()]);
        assert_eq!(tree.leaves.len(), 2);
        assert_ne!(tree.root_hash, [0u8; 32]);
    }

    #[test]
    fn test_merkle_tree_empty() {
        let tree = MerkleTree::new(vec![]);
        assert_eq!(tree.leaves.len(), 0);
        assert_eq!(tree.root_hash, *blake3::hash(b"").as_bytes());
    }

    #[test]
    fn test_merkle_tree_determinism_and_uniqueness() {
        let tree1 = MerkleTree::new(vec![b"block1".to_vec(), b"block2".to_vec()]);
        let tree2 = MerkleTree::new(vec![b"block1".to_vec(), b"block2".to_vec()]);
        let tree3 = MerkleTree::new(vec![b"block2".to_vec(), b"block1".to_vec()]);

        assert_eq!(tree1.root_hash, tree2.root_hash);
        assert_ne!(tree1.root_hash, tree3.root_hash);
    }

    #[test]
    fn test_merkle_tree_diff() {
        let tree1 = MerkleTree::new(vec![b"block1".to_vec(), b"block2".to_vec()]);
        let tree2 = MerkleTree::new(vec![b"block1".to_vec(), b"block3".to_vec()]); // block2 replaced

        let missing = tree1.diff(&tree2); // What tree2 has that tree1 doesn't
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0], *blake3::hash(b"block3").as_bytes());
    }

    #[test]
    fn test_merkle_tree_diff_identical() {
        let tree1 = MerkleTree::new(vec![b"block1".to_vec(), b"block2".to_vec()]);
        let tree2 = MerkleTree::new(vec![b"block1".to_vec(), b"block2".to_vec()]);

        let missing = tree1.diff(&tree2);
        assert!(missing.is_empty());
    }

    #[test]
    fn test_merkle_tree_diff_disjoint() {
        let tree1 = MerkleTree::new(vec![b"block1".to_vec()]);
        let tree2 = MerkleTree::new(vec![b"block2".to_vec(), b"block3".to_vec()]);

        let missing = tree1.diff(&tree2);
        assert_eq!(missing.len(), 2);
        // diff() makes no ordering guarantee — use set-membership assertions.
        assert!(missing.contains(&*blake3::hash(b"block2").as_bytes()));
        assert!(missing.contains(&*blake3::hash(b"block3").as_bytes()));
    }

    #[tokio::test]
    async fn test_heal_daemon_spawns() {
        let daemon = HealDaemon::new();
        assert!(!daemon.is_running());
        let handle = daemon.start().expect("first start should return a handle");
        assert!(daemon.is_running());
        handle.abort();
    }

    #[tokio::test]
    async fn test_heal_daemon_no_double_start() {
        let daemon = HealDaemon::new();
        let first = daemon.start();
        assert!(first.is_some(), "first call must return a handle");
        let second = daemon.start();
        assert!(second.is_none(), "second call must be a no-op");
        first.unwrap().abort();
    }
}

