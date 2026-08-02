use serde::{Deserialize, Serialize};

/// Represents a node in the CRUSH cluster map.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Node {
    /// Unique identifier for the node.
    pub id: String,
    /// Weight assigned to the node for placement scoring.
    pub weight: f64,
    /// IP address or network address of the node.
    pub ip: String,
}

/// Represents the cluster topology map containing all member nodes.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ClusterMap {
    /// List of nodes in the cluster.
    pub nodes: Vec<Node>,
}

impl ClusterMap {
    /// Creates a new empty `ClusterMap`.
    pub fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    /// Adds a `Node` to the cluster map.
    pub fn add_node(&mut self, node: Node) {
        self.nodes.push(node);
    }
}

/// Deterministically hashes a combination of `node_id` and `block_id` into an `f64` in `[0.0, 1.0)`.
pub fn hash_node_block(node_id: &str, block_id: &str) -> f64 {
    let input = format!("{}:{}", node_id, block_id);
    let hash = blake3::hash(input.as_bytes());
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&hash.as_bytes()[..8]);
    let val = u64::from_le_bytes(bytes);
    val as f64 / u64::MAX as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_hashing_stability() {
        let h1 = hash_node_block("node1", "blockA");
        let h2 = hash_node_block("node1", "blockA");
        assert_eq!(h1, h2);
        assert!(h1 >= 0.0 && h1 <= 1.0);
    }
}
