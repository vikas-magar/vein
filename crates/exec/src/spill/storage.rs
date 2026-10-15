//! Pluggable storage backends for query-scoped spill runs.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use common::Result;
use error::VeinError;

/// A seekable stream used by spill codecs. Backends can use local files,
/// in-memory test stores, or another query-local temporary storage mechanism.
pub trait SpillStream: Read + Write + Seek + Send + Sync {}

impl<T: Read + Write + Seek + Send + Sync> SpillStream for T {}

/// Opaque backend-owned identity for one run.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SpillRunId(String);

impl SpillRunId {
    /// Create an opaque run identifier for a backend-owned resource.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

/// Owns run creation, reopening, existence checks, and deletion.
///
/// The manager accounts bytes independently of the backend, so implementations
/// can vary storage without changing operator codecs or partition algorithms.
pub trait SpillStorage: Send + Sync {
    /// Allocate a unique writable run and return its identifier and stream.
    fn create(&self) -> Result<(SpillRunId, Box<dyn SpillStream>)>;
    /// Open an existing run for reading and seeking.
    fn open(&self, id: &SpillRunId) -> Result<Box<dyn SpillStream>>;
    /// Remove a run and release backend resources associated with it.
    fn remove(&self, id: &SpillRunId) -> Result<()>;
    #[cfg(test)]
    /// Report whether a run currently exists in the backend.
    fn exists(&self, id: &SpillRunId) -> bool;

    /// Returns a diagnostic directory when the backend is filesystem-backed.
    /// Operators must not depend on this path for reading or cleanup.
    #[cfg(test)]
    fn root_path(&self) -> Option<PathBuf> {
        None
    }
}

/// Creates an isolated spill backend for each query execution.
///
/// A factory is stored by an executor and invoked once per task. This keeps
/// query-local disk accounting and RAII cleanup independent when one executor
/// runs multiple tasks concurrently.
pub trait SpillStorageFactory: Send + Sync {
    /// Create a new storage owner whose lifetime is scoped to one query.
    fn create_for_query(&self) -> Result<Arc<dyn SpillStorage>>;
}

/// Factory for the default temporary-directory spill backend.
#[derive(Clone, Copy, Debug, Default)]
pub struct LocalSpillStorageFactory;

impl SpillStorageFactory for LocalSpillStorageFactory {
    fn create_for_query(&self) -> Result<Arc<dyn SpillStorage>> {
        Ok(Arc::new(LocalSpillStorage::new()))
    }
}

/// Default single-node backend. A query gets a unique lazily created directory;
/// dropping the backend removes any files left by success, errors, or cancel.
pub struct LocalSpillStorage {
    directory: OnceLock<std::result::Result<PathBuf, String>>,
    next_run: AtomicUsize,
}

impl LocalSpillStorage {
    /// Create a lazy temporary-directory storage owner for one query.
    pub fn new() -> Self {
        Self {
            directory: OnceLock::new(),
            next_run: AtomicUsize::new(0),
        }
    }

    fn directory(&self) -> Result<&PathBuf> {
        self.directory
            .get_or_init(|| create_query_directory().map_err(|error| error.to_string()))
            .as_ref()
            .map_err(|error| VeinError::internal(format!("create query spill directory: {error}")))
    }

    #[cfg(test)]
    fn directory_path(&self) -> Option<PathBuf> {
        self.directory
            .get()
            .and_then(|directory| directory.as_ref().ok())
            .cloned()
    }
}

impl SpillStorage for LocalSpillStorage {
    fn create(&self) -> Result<(SpillRunId, Box<dyn SpillStream>)> {
        let directory = self.directory()?;
        loop {
            let index = self.next_run.fetch_add(1, Ordering::Relaxed);
            let path = directory.join(format!("run-{index}.arrow"));
            match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(file) => {
                    return Ok((SpillRunId::new(path.to_string_lossy().into_owned()), Box::new(file)));
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(VeinError::internal(format!("create query spill file: {error}")));
                }
            }
        }
    }

    fn open(&self, id: &SpillRunId) -> Result<Box<dyn SpillStream>> {
        File::open(&id.0)
            .map(|file| Box::new(file) as Box<dyn SpillStream>)
            .map_err(|error| VeinError::internal(format!("open query spill run: {error}")))
    }

    fn remove(&self, id: &SpillRunId) -> Result<()> {
        match fs::remove_file(&id.0) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(VeinError::internal(format!("remove query spill run: {error}"))),
        }
    }

    #[cfg(test)]
    fn exists(&self, id: &SpillRunId) -> bool {
        PathBuf::from(&id.0).exists()
    }

    #[cfg(test)]
    fn root_path(&self) -> Option<PathBuf> {
        self.directory_path()
    }
}

impl Drop for LocalSpillStorage {
    fn drop(&mut self) {
        let Some(Ok(directory)) = self.directory.get() else {
            return;
        };
        if let Err(error) = fs::remove_dir_all(directory) {
            if error.kind() != io::ErrorKind::NotFound {
                tracing::warn!(
                    target: "vein.audit",
                    event = "query_spill_cleanup_failed",
                    path = %directory.display(),
                    error = %error,
                    "failed to remove query spill directory"
                );
            }
        }
    }
}

fn create_query_directory() -> io::Result<PathBuf> {
    let root = std::env::temp_dir();
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    for attempt in 0..128_u32 {
        let path = root.join(format!(
            "vein-query-{}-{timestamp}-{attempt}",
            std::process::id()
        ));
        match fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not allocate a unique query spill directory",
    ))
}
