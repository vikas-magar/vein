use crate::metadata::{Extent, MetadataEngine};
use crate::{Result, VfsConfig, VfsError};
use async_trait::async_trait;
use redb::{Database, TableDefinition};

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
pub(crate) fn compound_key(path: &str, offset: u64) -> [u8; 16] {
    let path_hash = siphash24(path.as_bytes());
    let mut key = [0u8; 16];
    key[0..8].copy_from_slice(&path_hash.to_be_bytes());
    key[8..16].copy_from_slice(&offset.to_be_bytes());
    key
}

/// Constructs a 16-byte key for path-level extent list indexing `[SipHash(File_Path): 8 bytes] + [0: 8 bytes]`.
pub(crate) fn path_key(path: &str) -> [u8; 16] {
    compound_key(path, 0)
}

const EXTENTS_TABLE: TableDefinition<&[u8; 16], &[u8]> = TableDefinition::new("vfs_extents");

/// Implementation of the MetadataEngine using `redb` for zero-copy, transactional storage.
pub struct RedbMetadataEngine {
    db: std::sync::Arc<Database>,
}

#[async_trait]
impl MetadataEngine for RedbMetadataEngine {
    async fn init(config: &VfsConfig) -> Result<Self> {
        let db = Database::create(&config.metadata_db_path)
            .map_err(|e| VfsError::Metadata(format!("Failed to create redb: {e}")))?;

        // Ensure the table exists by executing a fast write transaction on boot
        let write_txn = db.begin_write()
            .map_err(|e| VfsError::Metadata(format!("Write txn failed: {e}")))?;
        {
            write_txn.open_table(EXTENTS_TABLE)
                .map_err(|e| VfsError::Metadata(format!("Table creation failed: {e}")))?;
        }
        write_txn.commit()
            .map_err(|e| VfsError::Metadata(format!("Commit failed: {e}")))?;

        Ok(Self { db: std::sync::Arc::new(db) })
    }

    async fn append_extent(&self, path: &str, extent: Extent) -> Result<()> {
        let db = self.db.clone();
        let path = path.to_string();

        tokio::task::spawn_blocking(move || {
            let write_txn = db.begin_write().map_err(|e| VfsError::Metadata(e.to_string()))?;
            {
                let mut table = write_txn.open_table(EXTENTS_TABLE).map_err(|e| VfsError::Metadata(e.to_string()))?;
                let key = compound_key(&path, extent.offset);
                let serialized = bincode::serialize(&extent).map_err(|e| VfsError::Metadata(e.to_string()))?;
                table.insert(&key, serialized.as_slice()).map_err(|e| VfsError::Metadata(e.to_string()))?;
            }
            write_txn.commit().map_err(|e| VfsError::Metadata(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| VfsError::Internal(format!("Tokio spawn blocking failed: {}", e)))?
    }

    async fn get_file(&self, path: &str) -> Result<Vec<Extent>> {
        let db = self.db.clone();
        let path = path.to_string();

        tokio::task::spawn_blocking(move || {
            let read_txn = db.begin_read().map_err(|e| VfsError::Metadata(e.to_string()))?;
            let table = read_txn.open_table(EXTENTS_TABLE).map_err(|e| VfsError::Metadata(e.to_string()))?;
            let key = path_key(&path);

            if let Some(access) = table.get(&key).map_err(|e| VfsError::Metadata(e.to_string()))? {
                let extents = bincode::deserialize::<Vec<Extent>>(access.value()).map_err(|e| VfsError::Metadata(e.to_string()))?;
                Ok(extents)
            } else {
                Err(VfsError::NotFound(path.to_string()))
            }
        })
        .await
        .map_err(|e| VfsError::Internal(format!("Tokio spawn blocking failed: {}", e)))?
    }

    async fn delete_file(&self, path: &str) -> Result<()> {
        let db = self.db.clone();
        let path = path.to_string();

        tokio::task::spawn_blocking(move || {
            let write_txn = db.begin_write().map_err(|e| VfsError::Metadata(e.to_string()))?;
            {
                let mut table = write_txn.open_table(EXTENTS_TABLE).map_err(|e| VfsError::Metadata(e.to_string()))?;
                let key = path_key(&path);
                table.remove(&key).map_err(|e| VfsError::Metadata(e.to_string()))?;
            }
            write_txn.commit().map_err(|e| VfsError::Metadata(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| VfsError::Internal(format!("Tokio spawn blocking failed: {}", e)))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{Extent, MetadataEngine};
    use crate::VfsConfig;

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

        let key1 = compound_key(path, 4096);
        let key2 = compound_key(path, 4096);
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
        engine.append_extent("/data.parquet", extent.clone()).await.unwrap();

        let read_txn = engine.db.begin_read().unwrap();
        let table = read_txn.open_table(EXTENTS_TABLE).unwrap();
        let key = compound_key("/data.parquet", extent.offset);
        let access = table.get(&key).unwrap().unwrap();
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
            engine.append_extent("/test/file.dat", extent.clone()).await.unwrap();
            let read_txn = engine.db.begin_read().unwrap();
            let table = read_txn.open_table(EXTENTS_TABLE).unwrap();
            let key = compound_key("/test/file.dat", extent.offset);
            let access = table.get(&key).unwrap().unwrap();
            let read_extent: Extent = bincode::deserialize(access.value()).unwrap();
            assert_eq!(read_extent, extent);
        }

        // Re-open DB in session 2 (simulating process restart) and verify deterministic hash lookup
        {
            let engine2 = RedbMetadataEngine::init(&config).await.unwrap();
            let read_txn = engine2.db.begin_read().unwrap();
            let table = read_txn.open_table(EXTENTS_TABLE).unwrap();
            let key = compound_key("/test/file.dat", extent.offset);
            let access = table.get(&key).unwrap().unwrap();
            let read_extent: Extent = bincode::deserialize(access.value()).unwrap();
            assert_eq!(read_extent, extent);
        }
    }
}
