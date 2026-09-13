//! Typed Block Keys and Plane Categories for Vein Dual-Plane Storage.
//!
//! Enforces physical co-location and semantic I/O tiering:
//! - `PlaneType`: Distinguishes hot Address Planes from cold Payload Vaults and Tombstone masks.
//! - `SegmentKey`: Identifies the parent partition `(table_id, part_id)` used as the Straw2 affinity token.
//! - `BlockKey`: Full typed identifier mapping to canonical VFS paths.

use std::fmt;
use std::str::FromStr;
use serde::{Deserialize, Serialize};

use crate::error::VfsError;

/// Categorizes the physical storage plane of a block in Vein.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PlaneType {
    /// High-throughput canonical address array (`.exo.addr`).
    /// Kept in NVMe memory-maps; prioritized in page cache.
    AddressPlane,

    /// Cold, variable-length raw payloads (`.exo.vault`).
    /// Block-cached; eligible for background tiering to S3/GCS.
    PayloadVault,

    /// High-speed Roaring Bitmap tombstone mask (`.exo.tomb`).
    /// Pinned directly in RAM for zero-latency delete filtering.
    Tombstone,
}

impl PlaneType {
    /// Canonical file extension associated with this plane.
    #[inline(always)]
    pub fn extension(&self) -> &'static str {
        match self {
            PlaneType::AddressPlane => "exo.addr",
            PlaneType::PayloadVault => "exo.vault",
            PlaneType::Tombstone => "exo.tomb",
        }
    }

    /// Returns true if this plane should be aggressively cached in NVMe memory maps.
    #[inline(always)]
    pub fn is_hot_plane(&self) -> bool {
        matches!(self, PlaneType::AddressPlane | PlaneType::Tombstone)
    }
}

/// Identifies an immutable table partition segment.
///
/// Serves as the canonical Straw2 affinity hashing token:
/// All columns and planes belonging to this `SegmentKey` hash to the
/// exact same physical storage node in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SegmentKey {
    /// Unique identifier of the table.
    pub table_id: u32,
    /// Partition sequence identifier (0 .. N-1).
    pub part_id: u32,
}

impl SegmentKey {
    /// Creates a new `SegmentKey`.
    #[inline(always)]
    pub const fn new(table_id: u32, part_id: u32) -> Self {
        Self { table_id, part_id }
    }

    /// Generates the deterministic affinity token used by CRUSH Straw2.
    #[inline(always)]
    pub fn affinity_token(&self) -> String {
        format!("seg:{}:{:06}", self.table_id, self.part_id)
    }
}

/// Fully typed key identifying a specific column plane within a partition.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlockKey {
    /// Parent table and partition segment.
    pub segment: SegmentKey,
    /// Name of the column (or "tombstones" for tombstone mask).
    pub column_name: String,
    /// Plane classification (Address, Vault, Tombstone).
    pub plane: PlaneType,
}

impl BlockKey {
    /// Creates an Address Plane key for a column.
    #[inline(always)]
    pub fn address(table_id: u32, part_id: u32, column_name: impl Into<String>) -> Self {
        Self {
            segment: SegmentKey::new(table_id, part_id),
            column_name: column_name.into(),
            plane: PlaneType::AddressPlane,
        }
    }

    /// Creates a Payload Vault key for a column.
    #[inline(always)]
    pub fn vault(table_id: u32, part_id: u32, column_name: impl Into<String>) -> Self {
        Self {
            segment: SegmentKey::new(table_id, part_id),
            column_name: column_name.into(),
            plane: PlaneType::PayloadVault,
        }
    }

    /// Creates a Tombstone Mask key for a partition.
    #[inline(always)]
    pub fn tombstone(table_id: u32, part_id: u32) -> Self {
        Self {
            segment: SegmentKey::new(table_id, part_id),
            column_name: "tombstones".to_string(),
            plane: PlaneType::Tombstone,
        }
    }

    /// Canonical path string: `"tbl_{table_id}/part_{part_id:06}/{column_name}.{ext}"`
    pub fn to_canonical_path(&self) -> String {
        format!(
            "tbl_{}/part_{:06}/{}.{}",
            self.segment.table_id,
            self.segment.part_id,
            self.column_name,
            self.plane.extension()
        )
    }

    /// Parses a canonical path string back into a `BlockKey`.
    pub fn parse_canonical_path(path: &str) -> Result<Self, VfsError> {
        let parts: Vec<&str> = path.split('/').collect();
        if parts.len() != 3 {
            return Err(VfsError::Internal(format!(
                "Invalid canonical block path format: expected tbl_X/part_Y/file, got '{}'",
                path
            )));
        }

        let table_str = parts[0].strip_prefix("tbl_").ok_or_else(|| {
            VfsError::Internal(format!("Invalid table prefix in path '{}'", path))
        })?;
        let table_id: u32 = table_str.parse().map_err(|e| {
            VfsError::Internal(format!("Invalid table_id integer in path '{}': {}", path, e))
        })?;

        let part_str = parts[1].strip_prefix("part_").ok_or_else(|| {
            VfsError::Internal(format!("Invalid part prefix in path '{}'", path))
        })?;
        let part_id: u32 = part_str.parse().map_err(|e| {
            VfsError::Internal(format!("Invalid part_id integer in path '{}': {}", path, e))
        })?;

        let filename = parts[2];
        let (col_name, plane) = if let Some(base) = filename.strip_suffix(".exo.addr") {
            (base.to_string(), PlaneType::AddressPlane)
        } else if let Some(base) = filename.strip_suffix(".exo.vault") {
            (base.to_string(), PlaneType::PayloadVault)
        } else if let Some(base) = filename.strip_suffix(".exo.tomb") {
            (base.to_string(), PlaneType::Tombstone)
        } else {
            return Err(VfsError::Internal(format!(
                "Unrecognized file extension in block path '{}'",
                path
            )));
        };

        Ok(Self {
            segment: SegmentKey::new(table_id, part_id),
            column_name: col_name,
            plane,
        })
    }
}

impl fmt::Display for BlockKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_canonical_path())
    }
}

impl FromStr for BlockKey {
    type Err = VfsError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse_canonical_path(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_key_affinity_token() {
        let seg = SegmentKey::new(42, 7);
        assert_eq!(seg.affinity_token(), "seg:42:000007");

        let seg2 = SegmentKey::new(42, 7);
        assert_eq!(seg.affinity_token(), seg2.affinity_token());
    }

    #[test]
    fn test_block_key_canonical_paths_and_roundtrip() -> Result<(), VfsError> {
        let addr_key = BlockKey::address(100, 5, "customer_code");
        let path = addr_key.to_canonical_path();
        assert_eq!(path, "tbl_100/part_000005/customer_code.exo.addr");

        let parsed: BlockKey = path.parse()?;
        assert_eq!(parsed, addr_key);
        assert_eq!(parsed.plane, PlaneType::AddressPlane);
        assert!(parsed.plane.is_hot_plane());

        let vault_key = BlockKey::vault(100, 5, "customer_notes");
        let vpath = vault_key.to_canonical_path();
        assert_eq!(vpath, "tbl_100/part_000005/customer_notes.exo.vault");
        let parsed_vault: BlockKey = vpath.parse()?;
        assert_eq!(parsed_vault, vault_key);
        assert_eq!(parsed_vault.plane, PlaneType::PayloadVault);
        assert!(!parsed_vault.plane.is_hot_plane());

        let tomb_key = BlockKey::tombstone(100, 5);
        let tpath = tomb_key.to_canonical_path();
        assert_eq!(tpath, "tbl_100/part_000005/tombstones.exo.tomb");
        let parsed_tomb: BlockKey = tpath.parse()?;
        assert_eq!(parsed_tomb, tomb_key);
        assert_eq!(parsed_tomb.plane, PlaneType::Tombstone);
        assert!(parsed_tomb.plane.is_hot_plane());

        Ok(())
    }

    #[test]
    fn test_block_key_parse_invalid_formats() {
        assert!(BlockKey::parse_canonical_path("invalid").is_err());
        assert!(BlockKey::parse_canonical_path("invalid/part_0/col.exo.addr").is_err());
        assert!(BlockKey::parse_canonical_path("tbl_1/invalid/col.exo.addr").is_err());
        assert!(BlockKey::parse_canonical_path("tbl_1/part_0/col.unknown").is_err());
    }
}
