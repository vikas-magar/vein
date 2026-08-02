import re

with open('crates/vfs/src/metadata/redb_engine.rs', 'r') as f:
    content = f.read()

# 1. Add HashAlgo import
content = content.replace(
    'use crate::{Result, VfsConfig, VfsError};',
    'use crate::{Result, VfsConfig, VfsError};\nuse crate::config::HashAlgo;'
)

# 2. Update compound_key function
compound_key_old = """pub(crate) fn compound_key(path: &str, offset: u64) -> [u8; 16] {
    let path_hash = siphash24(path.as_bytes());
    let mut key = [0u8; 16];
    key[0..8].copy_from_slice(&path_hash.to_be_bytes());
    key[8..16].copy_from_slice(&offset.to_be_bytes());
    key
}"""

compound_key_new = """pub(crate) fn compound_key(path: &str, offset: u64, algo: &HashAlgo) -> Vec<u8> {
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
}"""
content = content.replace(compound_key_old, compound_key_new)

# 3. Update table definition
table_old = 'const EXTENTS_TABLE: TableDefinition<&[u8; 16], &[u8]> = TableDefinition::new("vfs_extents");'
table_new = 'const EXTENTS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("vfs_extents_v2");'
content = content.replace(table_old, table_new)

# 4. Update RedbMetadataEngine struct
struct_old = """pub struct RedbMetadataEngine {
    db: std::sync::Arc<Database>,
}"""
struct_new = """pub struct RedbMetadataEngine {
    db: std::sync::Arc<Database>,
    hash_algo: HashAlgo,
}"""
content = content.replace(struct_old, struct_new)

# 5. Update init method
init_old = """    async fn init(config: &VfsConfig) -> Result<Self> {
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
    }"""
init_new = """    async fn init(config: &VfsConfig) -> Result<Self> {
        let db = Database::create(&config.metadata_db_path)
            .map_err(|e| VfsError::Metadata(format!("Failed to create redb: {e}")))?;

        let write_txn = db.begin_write()
            .map_err(|e| VfsError::Metadata(format!("Write txn failed: {e}")))?;
        {
            // Migration
            let old_table_def = TableDefinition::<&str, &[u8]>::new("vfs_extents");
            
            if write_txn.open_table(old_table_def).is_ok() {
                let mut v2_table = write_txn.open_table(EXTENTS_TABLE)
                    .map_err(|e| VfsError::Metadata(format!("Table creation failed: {e}")))?;
                let old_table = write_txn.open_table(old_table_def).unwrap();
                let range = old_table.range::<&str>(..).map_err(|e| VfsError::Metadata(e.to_string()))?;
                for item in range {
                    let (k, v) = item.map_err(|e| VfsError::Metadata(e.to_string()))?;
                    let path = k.value();
                    let extents: Vec<Extent> = bincode::deserialize(v.value()).map_err(|e| VfsError::Metadata(e.to_string()))?;
                    for ext in extents {
                        let new_key = compound_key(path, ext.offset, &config.metadata_hash_algo);
                        let serialized = bincode::serialize(&ext).map_err(|e| VfsError::Metadata(e.to_string()))?;
                        v2_table.insert(new_key.as_slice(), serialized.as_slice()).map_err(|e| VfsError::Metadata(e.to_string()))?;
                    }
                }
                drop(old_table);
                write_txn.delete_table(old_table_def).map_err(|e| VfsError::Metadata(e.to_string()))?;
            } else {
                write_txn.open_table(EXTENTS_TABLE)
                    .map_err(|e| VfsError::Metadata(format!("Table creation failed: {e}")))?;
            }
        }
        write_txn.commit()
            .map_err(|e| VfsError::Metadata(format!("Commit failed: {e}")))?;

        Ok(Self { db: std::sync::Arc::new(db), hash_algo: config.metadata_hash_algo.clone() })
    }"""
content = content.replace(init_old, init_new)

# 6. Update append_extent
append_old = """    async fn append_extent(&self, path: &str, extent: Extent) -> Result<()> {
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
    }"""
append_new = """    async fn append_extent(&self, path: &str, extent: Extent) -> Result<()> {
        let db = self.db.clone();
        let path = path.to_string();
        let hash_algo = self.hash_algo.clone();

        tokio::task::spawn_blocking(move || {
            let write_txn = db.begin_write().map_err(|e| VfsError::Metadata(e.to_string()))?;
            {
                let mut table = write_txn.open_table(EXTENTS_TABLE).map_err(|e| VfsError::Metadata(e.to_string()))?;
                let key = compound_key(&path, extent.offset, &hash_algo);
                let serialized = bincode::serialize(&extent).map_err(|e| VfsError::Metadata(e.to_string()))?;
                table.insert(key.as_slice(), serialized.as_slice()).map_err(|e| VfsError::Metadata(e.to_string()))?;
            }
            write_txn.commit().map_err(|e| VfsError::Metadata(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| VfsError::Internal(format!("Tokio spawn blocking failed: {}", e)))?
    }"""
content = content.replace(append_old, append_new)

# 7. Update get_file
get_old = """    async fn get_file(&self, path: &str) -> Result<Vec<Extent>> {
        let db = self.db.clone();
        let path = path.to_string();

        tokio::task::spawn_blocking(move || {
            let read_txn = db.begin_read().map_err(|e| VfsError::Metadata(e.to_string()))?;
            let table = read_txn.open_table(EXTENTS_TABLE).map_err(|e| VfsError::Metadata(e.to_string()))?;
            
            let start_key = compound_key(&path, 0);
            let end_key = compound_key(&path, u64::MAX);

            let mut extents = Vec::new();
            let range = table.range::<&[u8; 16]>(&start_key..=&end_key).map_err(|e| VfsError::Metadata(e.to_string()))?;
            for item in range {
                let (_, value) = item.map_err(|e| VfsError::Metadata(e.to_string()))?;
                let ext: Extent = bincode::deserialize(value.value()).map_err(|e| VfsError::Metadata(e.to_string()))?;
                extents.push(ext);
            }

            if extents.is_empty() {
                return Err(VfsError::NotFound(path));
            }
            Ok(extents)
        })
        .await
        .map_err(|e| VfsError::Internal(format!("Tokio spawn blocking failed: {}", e)))?
    }"""
get_new = """    async fn get_file(&self, path: &str) -> Result<Vec<Extent>> {
        let db = self.db.clone();
        let path = path.to_string();
        let hash_algo = self.hash_algo.clone();

        tokio::task::spawn_blocking(move || {
            let read_txn = db.begin_read().map_err(|e| VfsError::Metadata(e.to_string()))?;
            let table = read_txn.open_table(EXTENTS_TABLE).map_err(|e| VfsError::Metadata(e.to_string()))?;
            
            let start_key = compound_key(&path, 0, &hash_algo);
            let end_key = compound_key(&path, u64::MAX, &hash_algo);

            let mut extents = Vec::new();
            let range = table.range::<&[u8]>(start_key.as_slice()..=end_key.as_slice()).map_err(|e| VfsError::Metadata(e.to_string()))?;
            for item in range {
                let (_, value) = item.map_err(|e| VfsError::Metadata(e.to_string()))?;
                let ext: Extent = bincode::deserialize(value.value()).map_err(|e| VfsError::Metadata(e.to_string()))?;
                extents.push(ext);
            }

            if extents.is_empty() {
                return Err(VfsError::NotFound(path));
            }
            Ok(extents)
        })
        .await
        .map_err(|e| VfsError::Internal(format!("Tokio spawn blocking failed: {}", e)))?
    }"""
content = content.replace(get_old, get_new)

# 8. Update delete_file
delete_old = """    async fn delete_file(&self, path: &str) -> Result<()> {
        let db = self.db.clone();
        let path = path.to_string();

        tokio::task::spawn_blocking(move || {
            let write_txn = db.begin_write().map_err(|e| VfsError::Metadata(e.to_string()))?;
            {
                let mut table = write_txn.open_table(EXTENTS_TABLE).map_err(|e| VfsError::Metadata(e.to_string()))?;
                let start_key = compound_key(&path, 0);
                let end_key = compound_key(&path, u64::MAX);

                let mut keys_to_delete = Vec::new();
                let range = table.range::<&[u8; 16]>(&start_key..=&end_key).map_err(|e| VfsError::Metadata(e.to_string()))?;
                for item in range {
                    let (key, _) = item.map_err(|e| VfsError::Metadata(e.to_string()))?;
                    keys_to_delete.push(*key.value());
                }

                for key in keys_to_delete {
                    table.remove(&key).map_err(|e| VfsError::Metadata(e.to_string()))?;
                }
            }
            write_txn.commit().map_err(|e| VfsError::Metadata(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| VfsError::Internal(format!("Tokio spawn blocking failed: {}", e)))?
    }"""
delete_new = """    async fn delete_file(&self, path: &str) -> Result<()> {
        let db = self.db.clone();
        let path = path.to_string();
        let hash_algo = self.hash_algo.clone();

        tokio::task::spawn_blocking(move || {
            let write_txn = db.begin_write().map_err(|e| VfsError::Metadata(e.to_string()))?;
            {
                let mut table = write_txn.open_table(EXTENTS_TABLE).map_err(|e| VfsError::Metadata(e.to_string()))?;
                let start_key = compound_key(&path, 0, &hash_algo);
                let end_key = compound_key(&path, u64::MAX, &hash_algo);

                let mut keys_to_delete = Vec::new();
                let range = table.range::<&[u8]>(start_key.as_slice()..=end_key.as_slice()).map_err(|e| VfsError::Metadata(e.to_string()))?;
                for item in range {
                    let (key, _) = item.map_err(|e| VfsError::Metadata(e.to_string()))?;
                    keys_to_delete.push(key.value().to_vec());
                }

                for key in keys_to_delete {
                    table.remove(key.as_slice()).map_err(|e| VfsError::Metadata(e.to_string()))?;
                }
            }
            write_txn.commit().map_err(|e| VfsError::Metadata(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| VfsError::Internal(format!("Tokio spawn blocking failed: {}", e)))?
    }"""
content = content.replace(delete_old, delete_new)

# 9. Update tests
tests_replacements = [
    ('let key1 = compound_key(path, 4096);', 'let key1 = compound_key(path, 4096, &HashAlgo::SipHash24);'),
    ('let key2 = compound_key(path, 4096);', 'let key2 = compound_key(path, 4096, &HashAlgo::SipHash24);'),
    ('let key = compound_key("/data.parquet", extent.offset);', 'let key = compound_key("/data.parquet", extent.offset, &engine.hash_algo);'),
    ('let key = compound_key("/test/file.dat", extent.offset);', 'let key = compound_key("/test/file.dat", extent.offset, &engine.hash_algo);'),
    ('table.get(&key)', 'table.get(key.as_slice())'),
    ('let access = table.get(key.as_slice()).unwrap().unwrap();', 'let access = table.get(key.as_slice()).unwrap().unwrap();'), # Noop if already done
]

for old, new in tests_replacements:
    content = content.replace(old, new)

# And in test_get_and_delete_file
content = content.replace(
    'engine.append_extent("/test.txt", ext1.clone()).await.unwrap();',
    'engine.append_extent("/test.txt", ext2.clone()).await.unwrap();\n        engine.append_extent("/test.txt", ext1.clone()).await.unwrap();'
)
content = content.replace(
    'engine.append_extent("/test.txt", ext2.clone()).await.unwrap();\n\n        let extents',
    'let extents'
)


with open('crates/vfs/src/metadata/redb_engine.rs', 'w') as f:
    f.write(content)
