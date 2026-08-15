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
    /// TCP/UDP port the node listens on (used by QUIC PeerPool).
    pub port: u16,
}

impl Node {
    /// Returns the address string `"ip:port"` for this node.
    pub fn addr(&self) -> String {
        format!("{}:{}", self.ip, self.port)
    }
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

    /// Determines the target `Node` for a given `block_id` using the Straw2 algorithm.
    pub fn get_owner(&self, block_id: &str) -> crate::Result<Node> {
        if self.nodes.is_empty() {
            return Err(crate::VfsError::Internal("No nodes in cluster map".into()));
        }

        let mut max_score = f64::NEG_INFINITY;
        let mut best_node = None;

        for node in &self.nodes {
            if node.weight <= 0.0 {
                continue;
            }

            let h = hash_node_block(&node.id, block_id);
            let one_minus_h = (1.0_f64 - h).max(f64::EPSILON);
            // Standard Ceph Straw2 score calculation: draw = ln(r) / weight where r = (1 - h) in (0, 1].
            // Since ln(r) <= 0, dividing by a larger node weight produces a score closer to 0 (i.e. higher score),
            // correctly favoring nodes with higher weights proportionally.
            let score = one_minus_h.ln() / node.weight;

            if score > max_score {
                max_score = score;
                best_node = Some(node);
            }
        }

        best_node
            .cloned()
            .ok_or_else(|| crate::VfsError::Internal("No valid nodes in cluster map".into()))
    }

    /// Returns all nodes whose `id` differs from `node_id`.
    /// Used by the QUIC PeerPool to enumerate broadcast targets.
    pub fn all_nodes_except<'a>(&'a self, node_id: &str) -> Vec<&'a Node> {
        self.nodes.iter().filter(|n| n.id != node_id).collect()
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

    #[test]
    fn test_straw2_placement() {
        let mut map = ClusterMap::new();
        map.add_node(Node {
            id: "node1".into(),
            weight: 1.0,
            ip: "127.0.0.1".into(),
            port: 4001,
        });
        map.add_node(Node {
            id: "node2".into(),
            weight: 2.0,
            ip: "127.0.0.2".into(),
            port: 4002,
        });

        let owner = map.get_owner("block_x").unwrap();
        assert!(owner.id == "node1" || owner.id == "node2");
    }

    #[test]
    fn test_straw2_empty_map() {
        let map = ClusterMap::new();
        assert!(map.get_owner("block_x").is_err());
    }

    #[test]
    fn test_straw2_deterministic_placement() {
        let mut map = ClusterMap::new();
        map.add_node(Node {
            id: "node1".into(),
            weight: 1.0,
            ip: "127.0.0.1".into(),
            port: 4001,
        });
        map.add_node(Node {
            id: "node2".into(),
            weight: 2.0,
            ip: "127.0.0.2".into(),
            port: 4002,
        });

        let owner1 = map.get_owner("block_test_123").unwrap();
        let owner2 = map.get_owner("block_test_123").unwrap();
        assert_eq!(owner1, owner2);
    }

    #[test]
    fn test_straw2_zero_weight_node() {
        let mut map = ClusterMap::new();
        map.add_node(Node {
            id: "node_zero".into(),
            weight: 0.0,
            ip: "127.0.0.1".into(),
            port: 4001,
        });
        map.add_node(Node {
            id: "node_active".into(),
            weight: 1.0,
            ip: "127.0.0.2".into(),
            port: 4002,
        });

        for i in 0..100 {
            let block_id = format!("block_{}", i);
            let owner = map.get_owner(&block_id).unwrap();
            assert_eq!(owner.id, "node_active");
        }
    }

    #[test]
    fn test_straw2_weight_distribution() {
        let mut map = ClusterMap::new();
        map.add_node(Node {
            id: "node_w1".into(),
            weight: 1.0,
            ip: "127.0.0.1".into(),
            port: 4001,
        });
        map.add_node(Node {
            id: "node_w3".into(),
            weight: 3.0,
            ip: "127.0.0.2".into(),
            port: 4002,
        });

        let mut w1_count = 0;
        let mut w3_count = 0;

        for i in 0..1000 {
            let block_id = format!("block_sample_{}", i);
            let owner = map.get_owner(&block_id).unwrap();
            if owner.id == "node_w1" {
                w1_count += 1;
            } else if owner.id == "node_w3" {
                w3_count += 1;
            }
        }

        // Higher weight node (weight 3.0) should get significantly more blocks than node with weight 1.0
        assert!(w3_count > w1_count);
        assert!(w3_count > 600); // Expect ~750 out of 1000
    }

    #[test]
    fn test_straw2_boundary_and_edge_cases() {
        let mut map = ClusterMap::new();
        map.add_node(Node {
            id: "node_neg".into(),
            weight: -1.0,
            ip: "127.0.0.1".into(),
            port: 4001,
        });
        map.add_node(Node {
            id: "node_zero".into(),
            weight: 0.0,
            ip: "127.0.0.2".into(),
            port: 4002,
        });
        map.add_node(Node {
            id: "node_pos".into(),
            weight: 0.5,
            ip: "127.0.0.3".into(),
            port: 4003,
        });

        let owner = map.get_owner("block_edge").unwrap();
        assert_eq!(owner.id, "node_pos");
    }

    #[test]
    fn test_straw2_all_zero_or_negative_weights() {
        let mut map = ClusterMap::new();
        map.add_node(Node {
            id: "node_neg".into(),
            weight: -1.0,
            ip: "127.0.0.1".into(),
            port: 4001,
        });
        map.add_node(Node {
            id: "node_zero".into(),
            weight: 0.0,
            ip: "127.0.0.2".into(),
            port: 4002,
        });

        assert!(map.get_owner("block_x").is_err());
    }

    #[test]
    fn test_all_nodes_except_only_node() {
        let mut map = ClusterMap::new();
        map.add_node(Node {
            id: "solo".into(),
            weight: 1.0,
            ip: "10.0.0.1".into(),
            port: 8080,
        });
        let peers = map.all_nodes_except("solo");
        assert!(peers.is_empty());
    }

    #[test]
    fn test_all_nodes_except_excludes_self() {
        let mut map = ClusterMap::new();
        map.add_node(Node {
            id: "a".into(),
            weight: 1.0,
            ip: "10.0.0.1".into(),
            port: 8080,
        });
        map.add_node(Node {
            id: "b".into(),
            weight: 1.0,
            ip: "10.0.0.2".into(),
            port: 8081,
        });
        map.add_node(Node {
            id: "c".into(),
            weight: 1.0,
            ip: "10.0.0.3".into(),
            port: 8082,
        });
        let peers = map.all_nodes_except("a");
        assert_eq!(peers.len(), 2);
        assert!(peers.iter().all(|n| n.id != "a"));
    }

    #[test]
    fn test_all_nodes_except_includes_others() {
        let mut map = ClusterMap::new();
        map.add_node(Node {
            id: "x".into(),
            weight: 1.0,
            ip: "10.0.0.1".into(),
            port: 9000,
        });
        map.add_node(Node {
            id: "y".into(),
            weight: 1.0,
            ip: "10.0.0.2".into(),
            port: 9001,
        });
        let peers = map.all_nodes_except("x");
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].id, "y");
    }

    #[test]
    fn test_node_addr() {
        let node = Node {
            id: "n1".into(),
            weight: 1.0,
            ip: "192.168.1.10".into(),
            port: 4433,
        };
        assert_eq!(node.addr(), "192.168.1.10:4433");
    }
}
