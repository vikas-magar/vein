use crate::metadata::{Extent, MetadataEngine};
use crate::{Result, VfsConfig, VfsError};
use async_trait::async_trait;
use redb::{Database, ReadableTable, TableDefinition};

const EXTENTS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("vfs_extents");

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
                let mut current_extents = if let Some(access) = table.get(path.as_str()).map_err(|e| VfsError::Metadata(e.to_string()))? {
                    bincode::deserialize::<Vec<Extent>>(access.value()).map_err(|e| VfsError::Metadata(e.to_string()))?
                } else {
                    Vec::new()
                };

                current_extents.push(extent);
                let serialized = bincode::serialize(&current_extents).map_err(|e| VfsError::Metadata(e.to_string()))?;
                table.insert(path.as_str(), serialized.as_slice()).map_err(|e| VfsError::Metadata(e.to_string()))?;
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

            if let Some(access) = table.get(path.as_str()).map_err(|e| VfsError::Metadata(e.to_string()))? {
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
                table.remove(path.as_str()).map_err(|e| VfsError::Metadata(e.to_string()))?;
            }
            write_txn.commit().map_err(|e| VfsError::Metadata(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| VfsError::Internal(format!("Tokio spawn blocking failed: {}", e)))?
    }
}
