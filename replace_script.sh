#!/bin/bash
sed -i '' 's/TableDefinition<&\[u8; 16\], &\[u8\]>/TableDefinition<\&\[u8\], \&\[u8\]>/g' crates/vfs/src/metadata/redb_engine.rs
sed -i '' 's/"vfs_extents"/"vfs_extents_v2"/g' crates/vfs/src/metadata/redb_engine.rs
