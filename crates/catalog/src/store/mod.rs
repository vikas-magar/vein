//! Bounded, format-neutral cache for parsed source metadata.

mod entries;
mod key;
mod singleflight;

#[cfg(test)]
mod tests;

use std::any::Any;
use std::fmt;
use std::sync::{Arc, RwLock};

use entries::Entries;
use singleflight::SingleFlight;
pub use key::MetadataKey;

/// Default number of parsed metadata entries retained by a store.
pub const DEFAULT_CAPACITY: usize = 512;

/// Names the format-owned kind of metadata stored under a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MetadataKind(&'static str);

impl MetadataKind {
    /// Declares a stable metadata kind owned by a format connector.
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }
}

impl fmt::Display for MetadataKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

/// Shared, bounded cache of parsed metadata with per-key parse coordination.
#[derive(Clone, Default)]
pub struct MetadataStore {
    entries: Arc<RwLock<Entries>>,
    singleflight: Arc<SingleFlight>,
}

impl MetadataStore {
    /// Creates an empty store with the default capacity.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty store retaining at most `capacity` entries.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Arc::new(RwLock::new(Entries::with_capacity(capacity))),
            singleflight: Arc::default(),
        }
    }

    /// Returns a typed payload when the exact object snapshot is cached.
    pub fn get<T: Any + Send + Sync>(&self, key: &MetadataKey) -> Option<Arc<T>> {
        self.entries.read().ok()?.get(key)
    }

    /// Publishes a payload and replaces prior versions of the same object/kind.
    pub fn insert<T: Any + Send + Sync>(&self, key: MetadataKey, metadata: Arc<T>) {
        if let Ok(mut entries) = self.entries.write() {
            for evicted in entries.insert(key, metadata) {
                tracing::debug!(
                    target: "vein.metadata_store",
                    event = "metadata_evicted",
                    kind = %evicted.kind(),
                    capacity = entries.capacity(),
                    "metadata_evicted"
                );
            }
        }
    }

    /// Returns cached metadata or runs one parser for concurrent misses of `key`.
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
        let insert_key = key.clone();
        self.singleflight.with_key(&key, || {
            if let Some(cached) = self.get::<T>(&key) {
                return Ok((cached, true));
            }
            let parsed = parse()?;
            self.insert(insert_key, Arc::clone(&parsed));
            Ok((parsed, false))
        })
    }

    /// Number of currently retained parsed metadata values.
    pub fn len(&self) -> usize {
        self.entries.read().map_or(0, |entries| entries.len())
    }

    /// Whether the store contains no parsed metadata.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Removes every cached value.
    pub fn clear(&self) {
        if let Ok(mut entries) = self.entries.write() {
            entries.clear();
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
