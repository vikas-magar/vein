use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::catalog::manifest::{TableManifest, artifact_path};
use common::{Result, VeinError};
use serde::Serialize;

use super::options::safe_table_name;

static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);

pub(super) fn next_generation() -> Result<String> {
    let epoch_nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| VeinError::internal(format!("system clock is before Unix epoch: {error}")))?
        .as_nanos();
    Ok(format!("{}-{epoch_nanos}-{}", std::process::id(), NEXT_GENERATION.fetch_add(1, Ordering::Relaxed)))
}

pub(super) fn publish_manifest(path: &Path, manifest: &TableManifest, generation: &str) -> Result<()> {
    publish_json_atomic(path, manifest, &format!(".manifest-{generation}.tmp"))
}

pub(super) fn publish_json_atomic<T: Serialize>(path: &Path, value: &T, temporary_name: &str) -> Result<()> {
    let parent = path.parent().ok_or_else(|| VeinError::internal("manifest path has no parent"))?;
    fs::create_dir_all(parent).map_err(|error| VeinError::io("create catalog directory", error))?;
    let temporary = parent.join(temporary_name);
    let result = (|| {
        let file = OpenOptions::new().write(true).create_new(true).open(&temporary)
            .map_err(|error| VeinError::io("create staged catalog metadata", error))?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, value)
            .map_err(|error| VeinError::internal(format!("serialize catalog statistics: {error}")))?;
        writer.flush().map_err(|error| VeinError::io("flush staged catalog metadata", error))?;
        writer.get_ref().sync_all().map_err(|error| VeinError::io("sync staged catalog metadata", error))?;
        drop(writer);
        fs::rename(&temporary, path).map_err(|error| VeinError::io("atomically publish catalog metadata", error))?;
        File::open(parent).and_then(|directory| directory.sync_all()).map_err(|error| {
            VeinError::commit_outcome_unknown(format!("catalog metadata is visible but directory durability is uncertain: {error}"))
        })?;
        Ok(())
    })();
    if result.is_err() { let _ = fs::remove_file(&temporary); }
    result
}

pub(super) fn sync_directory_chain(directory: &Path, root: &Path) -> Result<()> {
    let mut current = directory;
    loop {
        File::open(current).and_then(|directory| directory.sync_all())
            .map_err(|error| VeinError::io(format!("sync directory '{}'", current.display()), error))?;
        if current == root { return Ok(()); }
        current = current.parent().ok_or_else(|| VeinError::internal("storage directory is not beneath its configured root"))?;
        if !current.starts_with(root) { return Err(VeinError::internal("storage directory is not beneath its configured root")); }
    }
}

pub(super) fn table_commit_lock(root: &Path, table: &str) -> Result<File> {
    let lock_dir = root.join(".vein").join("locks");
    fs::create_dir_all(&lock_dir).map_err(|error| VeinError::io("create table lock directory", error))?;
    let lock_path = lock_dir.join(format!("{}.lock", safe_table_name(table)?));
    let lock = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(&lock_path)
        .map_err(|error| VeinError::io(format!("open table lock '{}'", lock_path.display()), error))?;
    lock.lock().map_err(|error| VeinError::io(format!("lock table '{table}' for commit"), error))?;
    Ok(lock)
}

pub(super) fn resolve_manifest_file(root: &Path, relative: &str) -> Result<PathBuf> {
    let artifact = artifact_path(relative)?;
    let root = root.canonicalize().map_err(|error| VeinError::io("resolve storage root", error))?;
    let path = root.join(artifact.as_str()).canonicalize()
        .map_err(|error| VeinError::io(format!("resolve committed artifact '{relative}'"), error))?;
    if !path.starts_with(&root) || !path.is_file() {
        return Err(VeinError::internal("catalog artifact resolves outside the storage root or is not a file"));
    }
    Ok(path)
}
