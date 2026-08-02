use serde::{Deserialize, Serialize};

/// A node within a HEAL Merkle Tree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MerkleNode {
    pub hash: [u8; 32],
}

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
}
