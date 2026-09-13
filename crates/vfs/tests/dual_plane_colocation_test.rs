use std::collections::HashSet;
use tempfile::tempdir;
use vfs::block::{BlockKey, PlaneType, SegmentKey};
use vfs::block::mmap_manager::MmapBlockManager;
use vfs::topology::crush::{ClusterMap, Node};
use vfs::{Result, VfsError};

#[test]
fn test_dual_plane_colocation_invariants() -> Result<()> {
    // 1. Setup a realistic heterogeneous 5-node storage cluster
    let mut cluster = ClusterMap::new();
    cluster.add_node(Node {
        id: "storage-node-us-east-1".into(),
        weight: 1.0,
        ip: "10.100.1.1".into(),
        port: 9100,
    });
    cluster.add_node(Node {
        id: "storage-node-us-east-2".into(),
        weight: 1.0,
        ip: "10.100.1.2".into(),
        port: 9100,
    });
    cluster.add_node(Node {
        id: "storage-node-us-west-1".into(),
        weight: 2.0, // High capacity NVMe node
        ip: "10.100.2.1".into(),
        port: 9100,
    });
    cluster.add_node(Node {
        id: "storage-node-us-west-2".into(),
        weight: 2.0, // High capacity NVMe node
        ip: "10.100.2.2".into(),
        port: 9100,
    });
    cluster.add_node(Node {
        id: "storage-node-eu-central-1".into(),
        weight: 1.5,
        ip: "10.100.3.1".into(),
        port: 9100,
    });

    // 2. Test 90 partitions across 3 different tables
    let tables = [101u32, 202u32, 303u32];
    let columns = ["id", "user_uuid", "event_type", "payload_blob", "metadata_json"];

    let mut placed_nodes = HashSet::new();

    for &table_id in &tables {
        for part_id in 0..30 {
            let seg = SegmentKey::new(table_id, part_id);
            let seg_owner = cluster.get_segment_owner(&seg)?;
            placed_nodes.insert(seg_owner.id.clone());

            // Primary Tombstone check
            let tomb_key = BlockKey::tombstone(table_id, part_id);
            assert_eq!(
                cluster.get_block_owner(&tomb_key)?.id,
                seg_owner.id,
                "Tombstone mask must be co-located with segment owner"
            );
            assert_eq!(
                cluster.get_owner(&tomb_key.to_canonical_path())?.id,
                seg_owner.id,
                "Canonical path resolution must yield identical owner for tombstone"
            );

            // For every column, both Address Plane and Payload Vault must match the segment owner!
            for col in &columns {
                let addr_key = BlockKey::address(table_id, part_id, *col);
                let vault_key = BlockKey::vault(table_id, part_id, *col);

                let addr_node = cluster.get_block_owner(&addr_key)?;
                let vault_node = cluster.get_block_owner(&vault_key)?;

                assert_eq!(
                    addr_node.id, seg_owner.id,
                    "Address plane for col '{}' partition '{}/{}' broke co-location invariant!",
                    col, table_id, part_id
                );
                assert_eq!(
                    vault_node.id, seg_owner.id,
                    "Payload vault for col '{}' partition '{}/{}' broke co-location invariant!",
                    col, table_id, part_id
                );

                // String canonical path resolution via get_owner
                let path_addr_node = cluster.get_owner(&addr_key.to_canonical_path())?;
                let path_vault_node = cluster.get_owner(&vault_key.to_canonical_path())?;

                assert_eq!(path_addr_node.id, seg_owner.id);
                assert_eq!(path_vault_node.id, seg_owner.id);
            }

            // Replica Co-location: 3-way replication
            let seg_replicas = cluster.get_segment_replicas(&seg, 3)?;
            assert_eq!(seg_replicas.len(), 3);
            assert_eq!(seg_replicas[0].id, seg_owner.id);

            for col in &columns {
                let addr_key = BlockKey::address(table_id, part_id, *col);
                let vault_key = BlockKey::vault(table_id, part_id, *col);

                let addr_reps = cluster.get_block_replicas(&addr_key, 3)?;
                let vault_reps = cluster.get_block_replicas(&vault_key, 3)?;

                assert_eq!(addr_reps, seg_replicas);
                assert_eq!(vault_reps, seg_replicas);
            }
        }
    }

    // Ensure Straw2 distributed the 90 partitions across all cluster nodes
    assert_eq!(placed_nodes.len(), 5, "All cluster nodes should receive partition segments");

    Ok(())
}

#[test]
fn test_plane_cache_advice_zero_copy() -> Result<()> {
    let dir = tempdir().map_err(|e| VfsError::Internal(e.to_string()))?;
    let test_file_path = dir.path().join("segment_mmap_advice_test.bin");

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&test_file_path)
        .map_err(|e| VfsError::Internal(e.to_string()))?;

    // Allocate 64KB
    file.set_len(65536).map_err(|e| VfsError::Internal(e.to_string()))?;

    // Mutable mmap
    let mmap_mut = unsafe { memmap2::MmapMut::map_mut(&file) }
        .map_err(|e| VfsError::Internal(e.to_string()))?;

    // Exercise kernel advice for each plane
    MmapBlockManager::advise_mmap_mut(&mmap_mut, PlaneType::AddressPlane)?;
    MmapBlockManager::advise_mmap_mut(&mmap_mut, PlaneType::PayloadVault)?;
    MmapBlockManager::advise_mmap_mut(&mmap_mut, PlaneType::Tombstone)?;
    drop(mmap_mut);

    // Read-only mmap
    let mmap_ro = unsafe { memmap2::Mmap::map(&file) }
        .map_err(|e| VfsError::Internal(e.to_string()))?;

    MmapBlockManager::advise_mmap(&mmap_ro, PlaneType::AddressPlane)?;
    MmapBlockManager::advise_mmap(&mmap_ro, PlaneType::PayloadVault)?;
    MmapBlockManager::advise_mmap(&mmap_ro, PlaneType::Tombstone)?;

    Ok(())
}
