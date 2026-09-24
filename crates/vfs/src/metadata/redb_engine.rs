use crate::config::HashAlgo;
use crate::metadata::{Extent, MetadataEngine, MetadataFileUpdate};
use crate::{Result, VfsConfig, VfsError};
use async_trait::async_trait;
use redb::{Database, ReadableTable, TableDefinition, TableHandle};

/// Computes deterministic SipHash-2-4 of input bytes using fixed keys for persistent key indexing.
pub(crate) fn siphash24(bytes: &[u8]) -> u64 {
    let k0 = 0x0706050403020100u64;
    let k1 = 0x0f0e0d0c0b0a0908u64;

    let mut v0 = k0 ^ 0x736f6d6570736575u64;
    let mut v1 = k1 ^ 0x646f72616d706c79u64;
    let mut v2 = k0 ^ 0x6c7967656e657261u64;
    let mut v3 = k1 ^ 0x7465646279746573u64;

    macro_rules! sipround {
        () => {
            v0 = v0.wrapping_add(v1);
            v1 = v1.rotate_left(13);
            v1 ^= v0;
            v0 = v0.rotate_left(32);
            v2 = v2.wrapping_add(v3);
            v3 = v3.rotate_left(16);
            v3 ^= v2;
            v0 = v0.wrapping_add(v3);
            v3 = v3.rotate_left(21);
            v3 ^= v0;
            v2 = v2.wrapping_add(v1);
            v1 = v1.rotate_left(17);
            v1 ^= v2;
            v2 = v2.rotate_left(32);
        };
    }

    let length = bytes.len();
    let mut i = 0;
    while i + 8 <= length {
        let word = u64::from_le_bytes([
            bytes[i],
            bytes[i + 1],
            bytes[i + 2],
            bytes[i + 3],
            bytes[i + 4],
            bytes[i + 5],
            bytes[i + 6],
            bytes[i + 7],
        ]);
        v3 ^= word;
        sipround!();
        sipround!();
        v0 ^= word;
        i += 8;
    }

    let mut last = (length as u64) << 56;
    let tail = &bytes[i..];
    for (idx, &b) in tail.iter().enumerate() {
        last |= (b as u64) << (idx * 8);
    }

    v3 ^= last;
    sipround!();
    sipround!();
    v0 ^= last;

    v2 ^= 0xff;
    sipround!();
    sipround!();
    sipround!();
    sipround!();

    v0 ^ v1 ^ v2 ^ v3
}

/// Constructs a 16-byte compound key `[SipHash(File_Path): 8 bytes] + [Logical_Offset: 8 bytes]`.
pub(crate) fn compound_key(path: &str, offset: u64, algo: &HashAlgo) -> Vec<u8> {
    match algo {
        HashAlgo::SipHash24 => {
            let path_hash = siphash24(path.as_bytes());
            let mut key = vec![0u8; 16];
            key[0..8].copy_from_slice(&path_hash.to_be_bytes());
            key[8..16].copy_from_slice(&offset.to_be_bytes());
            key
        }
        HashAlgo::Blake3 => {
            let path_hash = blake3::hash(path.as_bytes());
            let hash_bytes = path_hash.as_bytes();
            let mut key = vec![0u8; 40];
            key[0..32].copy_from_slice(hash_bytes);
            key[32..40].copy_from_slice(&offset.to_be_bytes());
            key
        }
    }
}

fn logical_offset_from_compound_key(key: &[u8]) -> Result<u64> {
    let offset_bytes = key
        .get(key.len().saturating_sub(8)..)
        .ok_or_else(|| VfsError::Metadata("Malformed extent key: missing logical offset".to_string()))?;
    let offset = u64::from_be_bytes(
        offset_bytes
            .try_into()
            .map_err(|_| VfsError::Metadata("Malformed extent key: invalid logical offset".to_string()))?,
    );
    Ok(offset)
}

const EXTENTS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("vfs_extents_v2");
const CONSENSUS_TABLE: TableDefinition<[u8; 16], &[u8]> =
    TableDefinition::new("consensus_proposals");
const SEQUENCE_TABLE: TableDefinition<&str, u64> = TableDefinition::new("sequence_counters");

/// Implementation of the MetadataEngine using `redb` for zero-copy, transactional storage.
pub struct RedbMetadataEngine {
    db: std::sync::Arc<Database>,
    hash_algo: HashAlgo,
}

#[async_trait]
impl MetadataEngine for RedbMetadataEngine {
    async fn init(config: &VfsConfig) -> Result<Self> {
        if let Some(parent) = config.metadata_db_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let db = Database::create(&config.metadata_db_path)
            .map_err(|e| VfsError::Metadata(format!("Failed to create redb: {e}")))?;

        let write_txn = db
            .begin_write()
            .map_err(|e| VfsError::Metadata(format!("Write txn failed: {e}")))?;
        {
            // Migration
            let old_table_name = "vfs_extents";
            let mut table_exists = false;
            if let Ok(tables) = write_txn.list_tables() {
                for handle in tables {
                    if handle.name() == old_table_name {
                        table_exists = true;
                        break;
                    }
                }
            }

            if table_exists {
                let old_table_def = TableDefinition::<&str, &[u8]>::new(old_table_name);
                let mut v2_table = write_txn
                    .open_table(EXTENTS_TABLE)
                    .map_err(|e| VfsError::Metadata(format!("Table creation failed: {e}")))?;
                let old_table = write_txn
                    .open_table(old_table_def)
                    .map_err(|e| VfsError::Metadata(e.to_string()))?;
                let range = old_table
                    .range::<&str>(..)
                    .map_err(|e| VfsError::Metadata(e.to_string()))?;
                for item in range {
                    let (k, v) = item.map_err(|e| VfsError::Metadata(e.to_string()))?;
                    let path = k.value();
                    let extents: Vec<Extent> = bincode::deserialize(v.value())
                        .map_err(|e| VfsError::Metadata(e.to_string()))?;
                    let mut logical_offset = 0u64;
                    for ext in extents {
                        let new_key = compound_key(path, logical_offset, &config.metadata_hash_algo);
                        let serialized = bincode::serialize(&ext)
                            .map_err(|e| VfsError::Metadata(e.to_string()))?;
                        v2_table
                            .insert(new_key.as_slice(), serialized.as_slice())
                            .map_err(|e| VfsError::Metadata(e.to_string()))?;
                        logical_offset = logical_offset.saturating_add(ext.length);
                    }
                }
                drop(old_table);
                write_txn
                    .delete_table(old_table_def)
                    .map_err(|e| VfsError::Metadata(e.to_string()))?;
            } else {
                write_txn
                    .open_table(EXTENTS_TABLE)
                    .map_err(|e| VfsError::Metadata(format!("Table creation failed: {e}")))?;
            }

            write_txn
                .open_table(CONSENSUS_TABLE)
                .map_err(|e| VfsError::Metadata(format!("Consensus table creation failed: {e}")))?;
            write_txn
                .open_table(SEQUENCE_TABLE)
                .map_err(|e| VfsError::Metadata(format!("Sequence table creation failed: {e}")))?;
        }
        write_txn
            .commit()
            .map_err(|e| VfsError::Metadata(format!("Commit failed: {e}")))?;

        Ok(Self {
            db: std::sync::Arc::new(db),
            hash_algo: config.metadata_hash_algo.clone(),
        })
    }

    async fn append_extent(&self, path: &str, extent: Extent) -> Result<()> {
        let db = self.db.clone();
        let path = path.to_string();
        let hash_algo = self.hash_algo.clone();

        tokio::task::spawn_blocking(move || {
            let write_txn = db
                .begin_write()
                .map_err(|e| VfsError::Metadata(e.to_string()))?;
            {
                let mut table = write_txn
                    .open_table(EXTENTS_TABLE)
                    .map_err(|e| VfsError::Metadata(e.to_string()))?;
                let start_key = compound_key(&path, 0, &hash_algo);
                let end_key = compound_key(&path, u64::MAX, &hash_algo);
                let mut logical_offset = 0u64;
                let range = table
                    .range::<&[u8]>(start_key.as_slice()..=end_key.as_slice())
                    .map_err(|e| VfsError::Metadata(e.to_string()))?;
                for item in range {
                    let (key, value) = item.map_err(|e| VfsError::Metadata(e.to_string()))?;
                    let existing: Extent = bincode::deserialize(value.value())
                        .map_err(|e| VfsError::Metadata(e.to_string()))?;
                    let existing_offset = logical_offset_from_compound_key(key.value())?;
                    logical_offset = logical_offset.max(existing_offset.saturating_add(existing.length));
                }

                let key = compound_key(&path, logical_offset, &hash_algo);
                let serialized =
                    bincode::serialize(&extent).map_err(|e| VfsError::Metadata(e.to_string()))?;
                table
                    .insert(key.as_slice(), serialized.as_slice())
                    .map_err(|e| VfsError::Metadata(e.to_string()))?;
            }
            write_txn
                .commit()
                .map_err(|e| VfsError::Metadata(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| VfsError::Internal(format!("Tokio spawn blocking failed: {}", e)))?
    }

    async fn get_file(&self, path: &str) -> Result<Vec<Extent>> {
        let db = self.db.clone();
        let path = path.to_string();
        let hash_algo = self.hash_algo.clone();

        tokio::task::spawn_blocking(move || {
            let read_txn = db
                .begin_read()
                .map_err(|e| VfsError::Metadata(e.to_string()))?;
            let table = read_txn
                .open_table(EXTENTS_TABLE)
                .map_err(|e| VfsError::Metadata(e.to_string()))?;

            let start_key = compound_key(&path, 0, &hash_algo);
            let end_key = compound_key(&path, u64::MAX, &hash_algo);

            let mut extents = Vec::new();
            let range = table
                .range::<&[u8]>(start_key.as_slice()..=end_key.as_slice())
                .map_err(|e| VfsError::Metadata(e.to_string()))?;
            for item in range {
                let (_, value) = item.map_err(|e| VfsError::Metadata(e.to_string()))?;
                let ext: Extent = bincode::deserialize(value.value())
                    .map_err(|e| VfsError::Metadata(e.to_string()))?;
                extents.push(ext);
            }

            if extents.is_empty() {
                return Err(VfsError::NotFound(path));
            }
            Ok(extents)
        })
        .await
        .map_err(|e| VfsError::Internal(format!("Tokio spawn blocking failed: {}", e)))?
    }

    async fn delete_file(&self, path: &str) -> Result<()> {
        let db = self.db.clone();
        let path = path.to_string();
        let hash_algo = self.hash_algo.clone();

        tokio::task::spawn_blocking(move || {
            let write_txn = db
                .begin_write()
                .map_err(|e| VfsError::Metadata(e.to_string()))?;
            {
                let mut table = write_txn
                    .open_table(EXTENTS_TABLE)
                    .map_err(|e| VfsError::Metadata(e.to_string()))?;
                let start_key = compound_key(&path, 0, &hash_algo);
                let end_key = compound_key(&path, u64::MAX, &hash_algo);

                let mut keys_to_delete = Vec::new();
                let range = table
                    .range::<&[u8]>(start_key.as_slice()..=end_key.as_slice())
                    .map_err(|e| VfsError::Metadata(e.to_string()))?;
                for item in range {
                    let (key, _) = item.map_err(|e| VfsError::Metadata(e.to_string()))?;
                    keys_to_delete.push(key.value().to_vec());
                }

                for key in keys_to_delete {
                    table
                        .remove(key.as_slice())
                        .map_err(|e| VfsError::Metadata(e.to_string()))?;
                }
            }
            write_txn
                .commit()
                .map_err(|e| VfsError::Metadata(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| VfsError::Internal(format!("Tokio spawn blocking failed: {}", e)))?
    }

    async fn replace_files_atomically(&self, updates: Vec<MetadataFileUpdate>) -> Result<()> {
        let db = self.db.clone();
        let hash_algo = self.hash_algo.clone();

        tokio::task::spawn_blocking(move || {
            let write_txn = db
                .begin_write()
                .map_err(|e| VfsError::Metadata(e.to_string()))?;

            {
                let mut table = write_txn
                    .open_table(EXTENTS_TABLE)
                    .map_err(|e| VfsError::Metadata(e.to_string()))?;

                for update in updates {
                    let start_key = compound_key(&update.path, 0, &hash_algo);
                    let end_key = compound_key(&update.path, u64::MAX, &hash_algo);
                    let mut keys_to_delete = Vec::new();

                    {
                        let range = table
                            .range::<&[u8]>(start_key.as_slice()..=end_key.as_slice())
                            .map_err(|e| VfsError::Metadata(e.to_string()))?;
                        for item in range {
                            let (key, _) = item.map_err(|e| VfsError::Metadata(e.to_string()))?;
                            keys_to_delete.push(key.value().to_vec());
                        }
                    }

                    for key in keys_to_delete {
                        table
                            .remove(key.as_slice())
                            .map_err(|e| VfsError::Metadata(e.to_string()))?;
                    }

                    let mut logical_offset = 0u64;
                    for extent in update.extents {
                        let key = compound_key(&update.path, logical_offset, &hash_algo);
                        let serialized =
                            bincode::serialize(&extent).map_err(|e| VfsError::Metadata(e.to_string()))?;
                        table
                            .insert(key.as_slice(), serialized.as_slice())
                            .map_err(|e| VfsError::Metadata(e.to_string()))?;
                        logical_offset = logical_offset
                            .checked_add(extent.length)
                            .ok_or_else(|| {
                                VfsError::Metadata(format!(
                                    "Metadata extent length overflow for '{}'",
                                    update.path
                                ))
                            })?;
                    }
                }
            }

            write_txn
                .commit()
                .map_err(|e| VfsError::Metadata(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| VfsError::Internal(format!("Tokio spawn blocking failed: {e}")))?
    }

    async fn persist_proposal(
        &self,
        pid_bytes: [u8; 16],
        status: u8,
        op_bytes: &[u8],
    ) -> Result<()> {
        let db = self.db.clone();
        let op_bytes = op_bytes.to_vec();

        tokio::task::spawn_blocking(move || {
            let write_txn = db
                .begin_write()
                .map_err(|e| VfsError::Metadata(e.to_string()))?;
            {
                let mut table = write_txn
                    .open_table(CONSENSUS_TABLE)
                    .map_err(|e| VfsError::Metadata(e.to_string()))?;
                let mut val = Vec::with_capacity(1 + op_bytes.len());
                val.push(status);
                val.extend_from_slice(&op_bytes);
                table
                    .insert(pid_bytes, val.as_slice())
                    .map_err(|e| VfsError::Metadata(e.to_string()))?;
            }
            write_txn
                .commit()
                .map_err(|e| VfsError::Metadata(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| VfsError::Internal(format!("Tokio spawn blocking failed: {}", e)))?
    }

    async fn load_consensus_log(&self) -> Result<Vec<([u8; 16], u8, Vec<u8>)>> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let read_txn = db
                .begin_read()
                .map_err(|e| VfsError::Metadata(e.to_string()))?;
            let table = read_txn
                .open_table(CONSENSUS_TABLE)
                .map_err(|e| VfsError::Metadata(e.to_string()))?;

            let mut entries = Vec::new();
            let range = table
                .range::<[u8; 16]>(..)
                .map_err(|e| VfsError::Metadata(e.to_string()))?;
            for item in range {
                let (key, value) = item.map_err(|e| VfsError::Metadata(e.to_string()))?;
                let pid_bytes: [u8; 16] = key.value();
                let val_slice = value.value();
                if val_slice.is_empty() {
                    continue;
                }
                let status = val_slice[0];
                let op_bytes = val_slice[1..].to_vec();
                entries.push((pid_bytes, status, op_bytes));
            }
            Ok(entries)
        })
        .await
        .map_err(|e| VfsError::Internal(format!("Tokio spawn blocking failed: {}", e)))?
    }

    async fn load_sequence(&self) -> Result<u64> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let read_txn = db
                .begin_read()
                .map_err(|e| VfsError::Metadata(e.to_string()))?;
            let table = read_txn
                .open_table(SEQUENCE_TABLE)
                .map_err(|e| VfsError::Metadata(e.to_string()))?;
            if let Some(access) = table
                .get("archipelago_seq")
                .map_err(|e| VfsError::Metadata(e.to_string()))?
            {
                Ok(access.value())
            } else {
                Ok(1)
            }
        })
        .await
        .map_err(|e| VfsError::Internal(format!("Tokio spawn blocking failed: {}", e)))?
    }

    async fn persist_sequence(&self, seq: u64) -> Result<()> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let write_txn = db
                .begin_write()
                .map_err(|e| VfsError::Metadata(e.to_string()))?;
            {
                let mut table = write_txn
                    .open_table(SEQUENCE_TABLE)
                    .map_err(|e| VfsError::Metadata(e.to_string()))?;
                table
                    .insert("archipelago_seq", seq)
                    .map_err(|e| VfsError::Metadata(e.to_string()))?;
            }
            write_txn
                .commit()
                .map_err(|e| VfsError::Metadata(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| VfsError::Internal(format!("Tokio spawn blocking failed: {}", e)))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::VfsConfig;
    use crate::metadata::{Extent, MetadataEngine};

    #[tokio::test]
    async fn test_table_initialization() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = VfsConfig {
            metadata_db_path: temp_dir.path().join("meta.db"),
            local_cache_dir: temp_dir.path().join("cache"),
            ..Default::default()
        };

        let engine = RedbMetadataEngine::init(&config).await.unwrap();
        assert!(engine.db.begin_read().is_ok());
    }

    #[test]
    fn test_siphash24_deterministic() {
        let path = "/var/vfs/data/chunk_1.bin";
        let hash1 = siphash24(path.as_bytes());
        let hash2 = siphash24(path.as_bytes());
        assert_eq!(hash1, hash2);
        assert_eq!(hash1, 418215382408935988u64);

        let key1 = compound_key(path, 4096, &HashAlgo::SipHash24);
        let key2 = compound_key(path, 4096, &HashAlgo::SipHash24);
        assert_eq!(key1, key2);
        assert_eq!(&key1[0..8], &hash1.to_be_bytes());
        assert_eq!(&key1[8..16], &4096u64.to_be_bytes());
    }

    #[tokio::test]
    async fn test_append_extent_o1() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = VfsConfig {
            metadata_db_path: temp_dir.path().join("meta2.db"),
            ..Default::default()
        };
        let engine = RedbMetadataEngine::init(&config).await.unwrap();

        let extent = Extent {
            block_id: 1,
            offset: 4096,
            length: 1024,
            node_ip: "127.0.0.1".into(),
        };
        engine
            .append_extent("/data.parquet", extent.clone())
            .await
            .unwrap();

        let read_txn = engine.db.begin_read().unwrap();
        let table = read_txn.open_table(EXTENTS_TABLE).unwrap();
        let key = compound_key("/data.parquet", 0, &engine.hash_algo);
        let access = table.get(key.as_slice()).unwrap().unwrap();
        let read_extent: Extent = bincode::deserialize(access.value()).unwrap();
        assert_eq!(read_extent, extent);
    }

    #[tokio::test]
    async fn test_deterministic_key_hash_persisted_roundtrip() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("meta.db");
        let config = VfsConfig {
            metadata_db_path: db_path.clone(),
            local_cache_dir: temp_dir.path().join("cache"),
            ..Default::default()
        };

        let extent = Extent {
            block_id: 42,
            offset: 1024,
            length: 2048,
            node_ip: "127.0.0.1".to_string(),
        };

        // Write extent in session 1
        {
            let engine = RedbMetadataEngine::init(&config).await.unwrap();
            engine
                .append_extent("/test/file.dat", extent.clone())
                .await
                .unwrap();
            let read_txn = engine.db.begin_read().unwrap();
            let table = read_txn.open_table(EXTENTS_TABLE).unwrap();
            let key = compound_key("/test/file.dat", 0, &engine.hash_algo);
            let access = table.get(key.as_slice()).unwrap().unwrap();
            let read_extent: Extent = bincode::deserialize(access.value()).unwrap();
            assert_eq!(read_extent, extent);
        }

        // Re-open DB in session 2 (simulating process restart) and verify deterministic hash lookup
        {
            let engine2 = RedbMetadataEngine::init(&config).await.unwrap();
            let read_txn = engine2.db.begin_read().unwrap();
            let table = read_txn.open_table(EXTENTS_TABLE).unwrap();
            let key = compound_key("/test/file.dat", 0, &engine2.hash_algo);
            let access = table.get(key.as_slice()).unwrap().unwrap();
            let read_extent: Extent = bincode::deserialize(access.value()).unwrap();
            assert_eq!(read_extent, extent);
        }
    }

    #[tokio::test]
    async fn test_get_and_delete_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = VfsConfig {
            metadata_db_path: temp_dir.path().join("meta3.db"),
            ..Default::default()
        };
        let engine = RedbMetadataEngine::init(&config).await.unwrap();

        let ext1 = Extent {
            block_id: 1,
            offset: 0,
            length: 1024,
            node_ip: "127.0.0.1".into(),
        };
        let ext2 = Extent {
            block_id: 1,
            offset: 1024,
            length: 512,
            node_ip: "127.0.0.1".into(),
        };

        engine
            .append_extent("/test.txt", ext2.clone())
            .await
            .unwrap();
        engine
            .append_extent("/test.txt", ext1.clone())
            .await
            .unwrap();
        let extents = engine.get_file("/test.txt").await.unwrap();
        assert_eq!(extents.len(), 2);
        assert_eq!(extents, vec![ext2, ext1]);

        engine.delete_file("/test.txt").await.unwrap();
        let err = engine.get_file("/test.txt").await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_append_extent_keys_by_logical_offset_not_physical_offset() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = VfsConfig {
            metadata_db_path: temp_dir.path().join("meta_logical_offsets.db"),
            ..Default::default()
        };
        let engine = RedbMetadataEngine::init(&config).await.unwrap();

        let first_block_extent = Extent {
            block_id: 10,
            offset: 0,
            length: 1024,
            node_ip: "127.0.0.1".into(),
        };
        let second_block_extent = Extent {
            block_id: 11,
            offset: 0,
            length: 512,
            node_ip: "127.0.0.1".into(),
        };

        engine
            .append_extent("/spans-blocks.bin", first_block_extent.clone())
            .await
            .unwrap();
        engine
            .append_extent("/spans-blocks.bin", second_block_extent.clone())
            .await
            .unwrap();

        let extents = engine.get_file("/spans-blocks.bin").await.unwrap();
        assert_eq!(extents, vec![first_block_extent, second_block_extent]);

        let read_txn = engine.db.begin_read().unwrap();
        let table = read_txn.open_table(EXTENTS_TABLE).unwrap();
        let logical_zero = compound_key("/spans-blocks.bin", 0, &engine.hash_algo);
        let logical_next = compound_key("/spans-blocks.bin", 1024, &engine.hash_algo);
        assert!(table.get(logical_zero.as_slice()).unwrap().is_some());
        assert!(table.get(logical_next.as_slice()).unwrap().is_some());
    }

    #[tokio::test]
    async fn test_persist_and_load_consensus_log() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = VfsConfig {
            metadata_db_path: temp_dir.path().join("meta_consensus.db"),
            ..Default::default()
        };
        let engine = RedbMetadataEngine::init(&config).await.unwrap();

        let pid_bytes1 = [1u8; 16];
        let pid_bytes2 = [2u8; 16];
        let op1 = b"op1_payload";
        let op2 = b"op2_payload";

        engine.persist_proposal(pid_bytes1, 0, op1).await.unwrap();
        engine.persist_proposal(pid_bytes2, 1, op2).await.unwrap();

        let entries = engine.load_consensus_log().await.unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries.contains(&(pid_bytes1, 0, op1.to_vec())));
        assert!(entries.contains(&(pid_bytes2, 1, op2.to_vec())));
    }

    #[tokio::test]
    async fn test_persist_and_load_sequence_across_restarts() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("meta_seq.db");
        let config = VfsConfig {
            metadata_db_path: db_path.clone(),
            ..Default::default()
        };

        // Session 1: Initial default sequence is 1, persist sequence 42
        {
            let engine = RedbMetadataEngine::init(&config).await.unwrap();
            let seq = engine.load_sequence().await.unwrap();
            assert_eq!(seq, 1);
            engine.persist_sequence(42).await.unwrap();
        }

        // Session 2: Reload and verify sequence survived restart
        {
            let engine2 = RedbMetadataEngine::init(&config).await.unwrap();
            let loaded_seq = engine2.load_sequence().await.unwrap();
            assert!(
                loaded_seq >= 42,
                "loaded_seq {} should be >= 42",
                loaded_seq
            );
            assert_eq!(loaded_seq, 42);
        }
    }
}
