use serde::{Deserialize, Serialize};

use crate::block::{BlockKey, SegmentKey};

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

    /// Determines the target `Node` for a given `block_id` or canonical path using Straw2.
    ///
    /// If the `block_id` is a canonical `.exo` block path (e.g. `tbl_1/part_000002/col.exo.addr`),
    /// this automatically extracts its segment affinity token (`seg:1:000002`) so that all planes
    /// and columns for that segment are strictly co-located on the same node.
    pub fn get_owner(&self, block_id: &str) -> crate::Result<Node> {
        let token = Self::resolve_affinity_token(block_id);
        self.get_owner_by_token(&token)
    }

    /// Determines the primary target `Node` for a `SegmentKey` using Straw2 segment affinity.
    ///
    /// Guarantees that Address Plane (`.exo.addr`), Payload Vault (`.exo.vault`),
    /// and Tombstone mask (`.exo.tomb`) for the segment land on the exact same physical node.
    pub fn get_segment_owner(&self, segment: &SegmentKey) -> crate::Result<Node> {
        self.get_owner_by_token(&segment.affinity_token())
    }

    /// Determines the target `Node` for a typed `BlockKey` using Straw2 segment affinity.
    ///
    /// Delegates to `get_segment_owner(&key.segment)`, guaranteeing physical co-location
    /// across all columns and planes within the partition.
    pub fn get_block_owner(&self, key: &BlockKey) -> crate::Result<Node> {
        self.get_segment_owner(&key.segment)
    }

    /// Selects `count` distinct replica nodes for a `SegmentKey` ranked by Straw2 score.
    ///
    /// The primary owner is at index 0. All planes for this segment share identical replica placement.
    pub fn get_segment_replicas(&self, segment: &SegmentKey, count: usize) -> crate::Result<Vec<Node>> {
        self.get_replicas_by_token(&segment.affinity_token(), count)
    }

    /// Selects `count` distinct replica nodes for a `BlockKey` ranked by Straw2 score.
    pub fn get_block_replicas(&self, key: &BlockKey, count: usize) -> crate::Result<Vec<Node>> {
        self.get_segment_replicas(&key.segment, count)
    }

    /// Resolves an affinity token from an arbitrary path or block identifier.
    /// If the path is a canonical block path, extracts the segment affinity token.
    fn resolve_affinity_token(block_id: &str) -> String {
        let trimmed = block_id.strip_prefix('/').unwrap_or(block_id);
        if let Ok(key) = BlockKey::parse_canonical_path(trimmed) {
            key.segment.affinity_token()
        } else {
            block_id.to_string()
        }
    }

    /// Internal Straw2 placement calculation for a resolved affinity token.
    fn get_owner_by_token(&self, token: &str) -> crate::Result<Node> {
        if self.nodes.is_empty() {
            return Err(crate::VfsError::Internal("No nodes in cluster map".into()));
        }

        let mut max_score = f64::NEG_INFINITY;
        let mut best_node = None;

        for node in &self.nodes {
            if node.weight <= 0.0 {
                continue;
            }

            let h = hash_node_block(&node.id, token);
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

    /// Selects `count` distinct nodes ranked by Straw2 score for the given affinity token.
    fn get_replicas_by_token(&self, token: &str, count: usize) -> crate::Result<Vec<Node>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        if self.nodes.is_empty() {
            return Err(crate::VfsError::Internal("No nodes in cluster map".into()));
        }

        let mut scored: Vec<(f64, &Node)> = Vec::new();
        for node in &self.nodes {
            if node.weight <= 0.0 {
                continue;
            }
            let h = hash_node_block(&node.id, token);
            let one_minus_h = (1.0_f64 - h).max(f64::EPSILON);
            let score = one_minus_h.ln() / node.weight;
            scored.push((score, node));
        }

        if scored.is_empty() {
            return Err(crate::VfsError::Internal("No valid nodes in cluster map".into()));
        }

        // Sort descending by score (highest score first)
        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        let take_count = count.min(scored.len());
        Ok(scored.iter().take(take_count).map(|(_, n)| (*n).clone()).collect())
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
        assert!((0.0..=1.0).contains(&h1));
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

    #[test]
    fn test_straw2_dual_plane_co_location() -> crate::Result<()> {
        let mut map = ClusterMap::new();
        for i in 1..=5 {
            map.add_node(Node {
                id: format!("node_{}", i),
                weight: 1.0,
                ip: format!("10.0.0.{}", i),
                port: 8000 + i as u16,
            });
        }

        // For partition 42/7, test Address Plane, Payload Vault, and Tombstone
        let seg = SegmentKey::new(42, 7);
        let seg_owner = map.get_segment_owner(&seg)?;

        let addr_key1 = BlockKey::address(42, 7, "click_id");
        let addr_key2 = BlockKey::address(42, 7, "user_id");
        let vault_key1 = BlockKey::vault(42, 7, "url");
        let vault_key2 = BlockKey::vault(42, 7, "payload");
        let tomb_key = BlockKey::tombstone(42, 7);

        // Typed block owner must match segment owner
        assert_eq!(map.get_block_owner(&addr_key1)?.id, seg_owner.id);
        assert_eq!(map.get_block_owner(&addr_key2)?.id, seg_owner.id);
        assert_eq!(map.get_block_owner(&vault_key1)?.id, seg_owner.id);
        assert_eq!(map.get_block_owner(&vault_key2)?.id, seg_owner.id);
        assert_eq!(map.get_block_owner(&tomb_key)?.id, seg_owner.id);

        // Path resolution via get_owner must also match segment owner
        assert_eq!(map.get_owner(&addr_key1.to_canonical_path())?.id, seg_owner.id);
        assert_eq!(map.get_owner(&vault_key1.to_canonical_path())?.id, seg_owner.id);
        assert_eq!(map.get_owner(&tomb_key.to_canonical_path())?.id, seg_owner.id);
        assert_eq!(map.get_owner(&format!("/{}", addr_key2.to_canonical_path()))?.id, seg_owner.id);

        Ok(())
    }

    #[test]
    fn test_straw2_segment_replicas_co_location() -> crate::Result<()> {
        let mut map = ClusterMap::new();
        for i in 1..=6 {
            map.add_node(Node {
                id: format!("node_{}", i),
                weight: 1.0,
                ip: format!("10.0.0.{}", i),
                port: 9000 + i as u16,
            });
        }

        let seg = SegmentKey::new(100, 3);
        let replicas = map.get_segment_replicas(&seg, 3)?;
        assert_eq!(replicas.len(), 3);
        // Ensure all 3 replicas are distinct
        assert_ne!(replicas[0].id, replicas[1].id);
        assert_ne!(replicas[1].id, replicas[2].id);
        assert_ne!(replicas[0].id, replicas[2].id);

        // Primary owner must match the first replica
        assert_eq!(map.get_segment_owner(&seg)?.id, replicas[0].id);

        // All block replicas must match segment replicas exactly
        let addr_key = BlockKey::address(100, 3, "score");
        let vault_key = BlockKey::vault(100, 3, "blob");
        let addr_reps = map.get_block_replicas(&addr_key, 3)?;
        let vault_reps = map.get_block_replicas(&vault_key, 3)?;

        assert_eq!(addr_reps, replicas);
        assert_eq!(vault_reps, replicas);

        // Edge cases
        assert_eq!(map.get_segment_replicas(&seg, 0)?.len(), 0);
        assert_eq!(map.get_segment_replicas(&seg, 10)?.len(), 6);

        Ok(())
    }
}
