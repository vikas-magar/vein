//! A bounded, self-invalidating store of parsed file metadata.
//!
//! Catalog implementations parse file metadata — Parquet footers, Iceberg
//! manifests, CSV headers — to answer planning questions. Execution needs the
//! same metadata to read the data. The store is the handoff between the two:
//! the catalog parses once, publishes the result under a [`MetadataKey`], and
//! execution looks it up instead of parsing the file a second time.
//!
//! Nothing here knows about a format. Each format declares its own
//! [`MetadataKind`] tag and stores its own payload type; the store only erases
//! payloads to [`Any`] and hands them back through a typed
//! [`MetadataStore::get`].

use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::UNIX_EPOCH;

/// Number of entries a store keeps before evicting the oldest ones.
pub const DEFAULT_CAPACITY: usize = 512;

/// Names the kind of parsed metadata an entry holds.
///
/// Each format owns its kinds, so unrelated payloads can never collide even
/// when they describe the same file. The name is part of the key and is
/// reported in logs, so it must stay stable once published.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MetadataKind(&'static str);

impl MetadataKind {
    /// Declares a metadata kind owned by one format.
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }
}

impl fmt::Display for MetadataKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

/// Identity of a parsed file: kind, canonical path, size, and modification time.
///
/// Size and modification time are part of the identity, so a rewritten file
/// never resolves to the metadata of the version it replaced.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MetadataKey {
    kind: MetadataKind,
    path: PathBuf,
    size_bytes: u64,
    modified_nanos: u128,
}

impl MetadataKey {
    /// Builds the key for `path`, or `None` when the file cannot be stat'ed.
    ///
    /// The path is canonicalized so that the catalog and the executor agree on
    /// the identity of a file they reached through different spellings.
    pub fn for_file(kind: MetadataKind, path: &Path) -> Option<Self> {
        let metadata = std::fs::metadata(path).ok()?;
        let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        let modified_nanos = metadata
            .modified()
            .ok()
            .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
            .map_or(0, |elapsed| elapsed.as_nanos());
        Some(Self {
            kind,
            path: canonical,
            size_bytes: metadata.len(),
            modified_nanos,
        })
    }
}

type Payload = Arc<dyn Any + Send + Sync>;

#[derive(Default)]
struct Entries {
    parsed: HashMap<MetadataKey, Payload>,
    order: VecDeque<MetadataKey>,
    capacity: usize,
}

impl Entries {
    fn capacity(&self) -> usize {
        if self.capacity == 0 {
            DEFAULT_CAPACITY
        } else {
            self.capacity
        }
    }
}

/// A cheaply cloneable handle to parsed file metadata shared across the engine.
///
/// Clones share one map. Entries are keyed by [`MetadataKey`], so lookups are
/// invalidated automatically when a file is rewritten. The store is bounded:
/// inserting past its capacity evicts the oldest entries first.
#[derive(Clone, Default)]
pub struct MetadataStore {
    entries: Arc<RwLock<Entries>>,
}

impl MetadataStore {
    /// Creates an empty store with the default capacity.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty store that retains at most `capacity` entries.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Arc::new(RwLock::new(Entries {
                capacity,
                ..Entries::default()
            })),
        }
    }

    /// Returns the parsed metadata registered under `key` when it is present
    /// and its payload has type `T`.
    ///
    /// A payload stored under a different type is reported as absent rather
    /// than as an error: the caller then parses the file and republishes it.
    pub fn get<T: Any + Send + Sync>(&self, key: &MetadataKey) -> Option<Arc<T>> {
        let entries = self.entries.read().ok()?;
        let payload = entries.parsed.get(key)?.clone();
        payload.downcast::<T>().ok()
    }

    /// Publishes `metadata` under `key`, replacing any previous entry for the
    /// same file and kind.
    pub fn insert<T: Any + Send + Sync>(&self, key: MetadataKey, metadata: Arc<T>) {
        let Ok(mut entries) = self.entries.write() else {
            return;
        };
        // Keep one entry per file and kind so a rewritten file cannot shadow its
        // own replacement while the superseded entry waits to be evicted.
        entries
            .parsed
            .retain(|existing, _| existing.kind != key.kind || existing.path != key.path);
        entries.parsed.insert(key.clone(), metadata);
        entries.order.push_back(key);
        let capacity = entries.capacity();
        while entries.parsed.len() > capacity {
            let Some(oldest) = entries.order.pop_front() else {
                break;
            };
            if entries.parsed.remove(&oldest).is_some() {
                tracing::debug!(
                    target: "vein.metadata_store",
                    event = "metadata_evicted",
                    kind = %oldest.kind,
                    path = %oldest.path.display(),
                    capacity,
                    "metadata_evicted"
                );
            }
        }
    }

    /// Returns the metadata for `key`, parsing it with `parse` on a miss.
    ///
    /// The flag is `true` when the value came from the store and `false` when
    /// `parse` ran, which lets callers report cache effectiveness.
    pub fn get_or_parse<T, E>(
        &self,
        key: MetadataKey,
        parse: impl FnOnce() -> Result<Arc<T>, E>,
    ) -> Result<(Arc<T>, bool), E>
    where
        T: Any + Send + Sync,
    {
        if let Some(cached) = self.get::<T>(&key) {
            return Ok((cached, true));
        }
        let parsed = parse()?;
        self.insert(key, parsed.clone());
        Ok((parsed, false))
    }

    /// Returns the number of entries currently retained.
    pub fn len(&self) -> usize {
        self.entries.read().map(|entries| entries.parsed.len()).unwrap_or(0)
    }

    /// Reports whether the store holds no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drops every entry.
    pub fn clear(&self) {
        if let Ok(mut entries) = self.entries.write() {
            entries.parsed.clear();
            entries.order.clear();
        }
    }
}

impl fmt::Debug for MetadataStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetadataStore")
            .field("len", &self.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    /// A format-owned payload: the store never names it, which is the point.
    #[derive(Debug, PartialEq)]
    struct RowCount(u64);

    const TEST_KIND: MetadataKind = MetadataKind::new("test.footer");
    const OTHER_KIND: MetadataKind = MetadataKind::new("test.manifest");

    fn write_file(path: &Path, contents: &[u8]) {
        let mut file = std::fs::File::create(path).expect("create file");
        file.write_all(contents).expect("write file");
    }

    fn key(path: &Path) -> MetadataKey {
        MetadataKey::for_file(TEST_KIND, path).expect("file exists")
    }

    #[test]
    fn misses_files_it_has_not_seen() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let path = dir.path().join("part.parquet");
        write_file(&path, b"metadata");

        let store = MetadataStore::new();
        assert!(store.get::<RowCount>(&key(&path)).is_none());
        assert!(store.is_empty());
    }

    #[test]
    fn serves_what_it_was_given() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let path = dir.path().join("part.parquet");
        write_file(&path, b"metadata");

        let store = MetadataStore::new();
        store.insert(key(&path), Arc::new(RowCount(7)));

        let cached = store.get::<RowCount>(&key(&path)).expect("cached payload");
        assert_eq!(*cached, RowCount(7));
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn distinguishes_kinds_and_payload_types() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let path = dir.path().join("part.parquet");
        write_file(&path, b"metadata");

        let store = MetadataStore::new();
        store.insert(key(&path), Arc::new(RowCount(7)));
        store.insert(
            MetadataKey::for_file(OTHER_KIND, &path).expect("file exists"),
            Arc::new("manifest".to_string()),
        );

        assert_eq!(
            *store.get::<RowCount>(&key(&path)).expect("footer"),
            RowCount(7)
        );
        assert!(
            store
                .get::<RowCount>(&MetadataKey::for_file(OTHER_KIND, &path).expect("file exists"))
                .is_none(),
            "a different kind must not answer for the footer payload type"
        );
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn a_rewritten_file_invalidates_its_entry() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let path = dir.path().join("part.parquet");
        write_file(&path, b"metadata");

        let store = MetadataStore::new();
        store.insert(key(&path), Arc::new(RowCount(7)));

        // A different size means the entry cannot be mistaken for the
        // replacement even at coarse modification-time granularity.
        write_file(&path, b"a longer metadata block");
        assert!(store.get::<RowCount>(&key(&path)).is_none());
    }

    #[test]
    fn keeps_one_entry_per_file_and_kind() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let path = dir.path().join("part.parquet");
        write_file(&path, b"metadata");

        let store = MetadataStore::new();
        store.insert(key(&path), Arc::new(RowCount(1)));
        store.insert(key(&path), Arc::new(RowCount(2)));

        assert_eq!(store.len(), 1);
        assert_eq!(*store.get::<RowCount>(&key(&path)).expect("footer"), RowCount(2));
    }

    #[test]
    fn evicts_the_oldest_entry_past_capacity() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let store = MetadataStore::with_capacity(2);
        let paths: Vec<PathBuf> = (0..3)
            .map(|index| {
                let path = dir.path().join(format!("part-{index}.parquet"));
                write_file(&path, format!("metadata-{index}").as_bytes());
                path
            })
            .collect();

        for (index, path) in paths.iter().enumerate() {
            store.insert(key(path), Arc::new(RowCount(index as u64)));
        }

        assert_eq!(store.len(), 2);
        assert!(store.get::<RowCount>(&key(&paths[0])).is_none(), "oldest evicted");
        assert!(store.get::<RowCount>(&key(&paths[1])).is_some());
        assert!(store.get::<RowCount>(&key(&paths[2])).is_some());
    }

    #[test]
    fn get_or_parse_parses_once() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let path = dir.path().join("part.parquet");
        write_file(&path, b"metadata");

        let store = MetadataStore::new();
        let mut parses = 0;
        for expected_hit in [false, true, true] {
            let (payload, hit) = store
                .get_or_parse(key(&path), || {
                    parses += 1;
                    Ok::<_, ()>(Arc::new(RowCount(7)))
                })
                .expect("parse succeeds");
            assert_eq!(hit, expected_hit);
            assert_eq!(*payload, RowCount(7));
        }
        assert_eq!(parses, 1, "the second and third lookups reuse the parsed value");
    }

    #[test]
    fn clear_drops_every_entry() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let path = dir.path().join("part.parquet");
        write_file(&path, b"metadata");

        let store = MetadataStore::new();
        store.insert(key(&path), Arc::new(RowCount(7)));
        store.clear();

        assert!(store.is_empty());
        assert!(store.get::<RowCount>(&key(&path)).is_none());
    }
}
