//! Local Parquet implementation of the executor's transactional write contract.

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use common::{Result, VeinError};
use common::protocol::StorageTarget;
use exec::operators::sink::{WriteReceipt, WriteStorage, WriteTransaction};
use parquet::arrow::ArrowWriter;
use serde::{Deserialize, Serialize};

static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableManifest {
    pub revision: u64,
    pub files: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct ParquetStorage {
    root: Arc<PathBuf>,
}

impl ParquetStorage {
    pub(crate) fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: Arc::new(root.into()) }
    }

    pub(crate) fn manifest_path(&self, table: &str) -> Result<PathBuf> {
        Ok(self.root.join(".vein").join("catalog").join(format!("{}.json", safe_table_name(table)?)))
    }

    pub(crate) fn read_manifest(&self, table: &str) -> Result<Option<TableManifest>> {
        let path = self.manifest_path(table)?;
        match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|error| VeinError::internal(format!("invalid table manifest '{}': {error}", path.display()))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(VeinError::io(format!("read table manifest '{}'", path.display()), error)),
        }
    }
}

impl WriteStorage for ParquetStorage {
    fn validate_target(&self, destination_uri: &str, options: &[(String, String)]) -> Result<()> {
        validate_target_parts(destination_uri, options.iter().map(|(key, value)| (key.as_str(), value.as_str())), false)
            .map(|_| ())
    }

    fn begin_write(&self, target: &StorageTarget) -> Result<Box<dyn WriteTransaction>> {
        // `partition_id` is added by task dispatch after query preparation; it is
        // runtime metadata and is not a user-configurable Parquet writer option.
        let (table, mode) = validate_target_parts(
            &target.destination_uri,
            target.options.iter().map(|option| (option.key.as_str(), option.value.as_str())),
            true,
        )?;

        let base_manifest = self.read_manifest(&table)?;
        match (mode.as_str(), base_manifest.as_ref()) {
            ("create", Some(_)) => return Err(VeinError::planning(format!("table '{table}' already exists; use mode='overwrite' or mode='append'"), None)),
            ("append" | "overwrite", None) => return Err(VeinError::not_found(format!("managed table '{table}' does not exist for {mode}"))),
            _ => {}
        }
        if mode == "create" && self.root.join(&table).exists() {
            return Err(VeinError::planning(format!("target '{}' already exists outside Vein's managed catalog", self.root.join(&table).display()), None));
        }
        let schema = decode_schema(&target.schema_ipc)?;

        let epoch_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| VeinError::internal(format!("system clock is before Unix epoch: {error}")))?
            .as_nanos();
        let generation = format!("{}-{epoch_nanos}-{}", std::process::id(), NEXT_GENERATION.fetch_add(1, Ordering::Relaxed));
        let staging_dir = self.root.join(".vein").join("staging").join(&table).join(&generation);
        fs::create_dir_all(&staging_dir)
            .map_err(|error| VeinError::io(format!("create staging directory '{}'", staging_dir.display()), error))?;
        Ok(Box::new(ParquetWriteTransaction {
            storage: self.clone(),
            table,
            mode,
            base_manifest,
            staging_dir,
            generation,
            writer: None,
            rows: 0,
            schema,
        }))
    }
}

struct ParquetWriteTransaction {
    storage: ParquetStorage,
    table: String,
    mode: String,
    base_manifest: Option<TableManifest>,
    staging_dir: PathBuf,
    generation: String,
    writer: Option<ArrowWriter<BufWriter<File>>>,
    schema: SchemaRef,
    rows: u64,
}

impl WriteTransaction for ParquetWriteTransaction {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.schema.as_ref() != batch.schema().as_ref() {
            return Err(VeinError::planning("WRITE batch schema does not match the planned output schema", None));
        }
        let rows = self.rows.checked_add(batch.num_rows() as u64)
            .ok_or_else(|| VeinError::internal("WRITE row count overflow"))?;
        if self.writer.is_none() {
            let path = self.staging_dir.join("part.parquet");
            let file = OpenOptions::new().write(true).create_new(true).open(&path)
                .map_err(|error| VeinError::io(format!("create staged Parquet file '{}'", path.display()), error))?;
            self.writer = Some(ArrowWriter::try_new(BufWriter::new(file), self.schema.clone(), None)
                .map_err(|error| VeinError::internal(format!("initialize Parquet writer: {error}")))?);
        }
        self.writer.as_mut()
            .ok_or_else(|| VeinError::internal("Parquet writer was not initialized"))?
            .write(batch)
            .map_err(|error| VeinError::internal(format!("write Parquet batch: {error}")))?;
        self.rows = rows;
        Ok(())
    }

    fn commit(mut self: Box<Self>) -> Result<WriteReceipt> {
        let _guard = table_commit_lock(&self.storage.root, &self.table)?;
        let current = self.storage.read_manifest(&self.table)?;
        if current.as_ref() != self.base_manifest.as_ref() {
            return Err(VeinError::planning(format!("table '{}' changed while WRITE was running; retry against the new revision", self.table), None));
        }
        match (self.mode.as_str(), current.as_ref()) {
            ("create", Some(_)) => return Err(VeinError::planning(format!("table '{}' already exists", self.table), None)),
            ("append" | "overwrite", None) => return Err(VeinError::not_found(format!("managed table '{}' no longer exists", self.table))),
            _ => {}
        }
        if self.mode == "create" && self.storage.root.join(&self.table).exists() {
            return Err(VeinError::planning(format!("target '{}' appeared while WRITE was running", self.storage.root.join(&self.table).display()), None));
        }
        if self.mode == "append" {
            self.validate_append_schema(current.as_ref())?;
        }

        if self.writer.is_none() {
            let path = self.staging_dir.join("part.parquet");
            let file = OpenOptions::new().write(true).create_new(true).open(&path)
                .map_err(|error| VeinError::io(format!("create staged Parquet file '{}'", path.display()), error))?;
            self.writer = Some(ArrowWriter::try_new(BufWriter::new(file), self.schema.clone(), None)
                .map_err(|error| VeinError::internal(format!("initialize empty Parquet writer: {error}")))?);
        }
        let writer = self.writer.take().ok_or_else(|| VeinError::internal("Parquet writer initialization failed"))?;
        let output = writer.into_inner()
            .map_err(|error| VeinError::internal(format!("finish staged Parquet writer: {error}")))?;
        let mut output = output;
        output.flush().map_err(|error| VeinError::io("flush staged Parquet file", error))?;
        output.get_ref().sync_all()
            .map_err(|error| VeinError::io("sync staged Parquet file", error))?;
        drop(output);

        let final_dir = self.storage.root.join(".vein").join("tables").join(&self.table).join(&self.generation);
        fs::create_dir_all(&final_dir)
            .map_err(|error| VeinError::io(format!("create generation directory '{}'", final_dir.display()), error))?;
        let final_file = final_dir.join("part.parquet");
        fs::rename(self.staging_dir.join("part.parquet"), &final_file)
            .map_err(|error| VeinError::io("publish immutable Parquet generation", error))?;
        sync_directory_chain(&final_dir, &self.storage.root)?;

        let mut files = match (self.mode.as_str(), current) {
            ("append", Some(manifest)) => manifest.files,
            _ => Vec::new(),
        };
        let relative_file = final_file.strip_prefix(self.storage.root.as_path())
            .map_err(|_| VeinError::internal("published artifact is outside the storage root"))?;
        files.push(relative_file.to_string_lossy().into_owned());
        let revision = match &self.base_manifest {
            Some(base) => base.revision.checked_add(1)
                .ok_or_else(|| VeinError::internal("table catalog revision overflow"))?,
            None => 1,
        };
        let manifest = TableManifest { revision, files };
        if let Err(error) = publish_manifest(&self.storage.manifest_path(&self.table)?, &manifest, &self.generation) {
            if !matches!(&error, VeinError::CommitOutcomeUnknown { .. }) {
                let _ = fs::remove_dir_all(&final_dir);
            }
            return Err(error);
        }
        let _ = fs::remove_dir_all(&self.staging_dir);
        Ok(WriteReceipt { destination: self.table.clone(), row_count: self.rows })
    }
}

impl ParquetWriteTransaction {
    fn validate_append_schema(&self, manifest: Option<&TableManifest>) -> Result<()> {
        let existing = manifest
            .and_then(|manifest| manifest.files.first())
            .ok_or_else(|| VeinError::internal("append target has no committed data files"))?;
        let existing_path = resolve_manifest_file(&self.storage.root, existing)?;
        let file = File::open(&existing_path)
            .map_err(|error| VeinError::io(format!("open existing table artifact '{existing}'"), error))?;
        let existing_schema = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|error| VeinError::internal(format!("read existing table schema: {error}")))?
            .schema()
            .clone();
        if existing_schema.as_ref() != self.schema.as_ref() {
            return Err(VeinError::planning("append schema must exactly match the existing table schema", None));
        }
        Ok(())
    }
}

impl Drop for ParquetWriteTransaction {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.staging_dir);
    }
}

fn publish_manifest(path: &Path, manifest: &TableManifest, generation: &str) -> Result<()> {
    let parent = path.parent().ok_or_else(|| VeinError::internal("manifest path has no parent"))?;
    fs::create_dir_all(parent).map_err(|error| VeinError::io("create catalog directory", error))?;
    let temporary = parent.join(format!(".manifest-{generation}.tmp"));
    let result = (|| {
        let file = OpenOptions::new().write(true).create_new(true).open(&temporary)
            .map_err(|error| VeinError::io("create staged manifest", error))?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, manifest)
            .map_err(|error| VeinError::internal(format!("serialize table manifest: {error}")))?;
        writer.flush().map_err(|error| VeinError::io("flush staged manifest", error))?;
        writer.get_ref().sync_all().map_err(|error| VeinError::io("sync staged manifest", error))?;
        drop(writer);
        fs::rename(&temporary, path).map_err(|error| VeinError::io("atomically publish table manifest", error))?;
        File::open(parent).and_then(|directory| directory.sync_all())
            .map_err(|error| VeinError::commit_outcome_unknown(format!("manifest is visible but directory durability is uncertain: {error}")))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path).and_then(|directory| directory.sync_all())
        .map_err(|error| VeinError::io(format!("sync directory '{}'", path.display()), error))
}

fn sync_directory_chain(directory: &Path, root: &Path) -> Result<()> {
    let mut current = directory;
    loop {
        sync_directory(current)?;
        if current == root {
            return Ok(());
        }
        current = current.parent().ok_or_else(|| VeinError::internal("storage directory is not beneath its configured root"))?;
        if !current.starts_with(root) {
            return Err(VeinError::internal("storage directory is not beneath its configured root"));
        }
    }
}

fn decode_schema(bytes: &[u8]) -> Result<SchemaRef> {
    if bytes.is_empty() {
        return Err(VeinError::planning("WRITE target has no planned Arrow schema", None));
    }
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|error| VeinError::planning(format!("invalid WRITE Arrow schema: {error}"), None))?;
    Ok(reader.schema())
}

fn table_commit_lock(root: &Path, table: &str) -> Result<File> {
    let lock_dir = root.join(".vein").join("locks");
    fs::create_dir_all(&lock_dir).map_err(|error| VeinError::io("create table lock directory", error))?;
    let lock_path = lock_dir.join(format!("{}.lock", safe_table_name(table)?));
    let lock = OpenOptions::new().read(true).write(true).create(true).open(&lock_path)
        .map_err(|error| VeinError::io(format!("open table lock '{}'", lock_path.display()), error))?;
    lock.lock().map_err(|error| VeinError::io(format!("lock table '{}' for commit", table), error))?;
    Ok(lock)
}

pub(crate) fn resolve_manifest_file(root: &Path, relative: &str) -> Result<PathBuf> {
    let relative_path = Path::new(relative);
    if relative_path.is_absolute() || relative_path.components().any(|component| !matches!(component, Component::Normal(_))) {
        return Err(VeinError::internal("catalog manifest contains an unsafe artifact path"));
    }
    let root = root.canonicalize().map_err(|error| VeinError::io("resolve storage root", error))?;
    let path = root.join(relative_path).canonicalize()
        .map_err(|error| VeinError::io(format!("resolve committed artifact '{relative}'"), error))?;
    if !path.starts_with(&root) || !path.is_file() {
        return Err(VeinError::internal("catalog artifact resolves outside the storage root or is not a file"));
    }
    Ok(path)
}

fn safe_table_name(name: &str) -> Result<&str> {
    let mut components = Path::new(name).components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(_)), None) if !name.is_empty() && name != "." => Ok(name),
        _ => Err(VeinError::planning("WRITE target must be a single logical table name", None)),
    }
}

fn validate_target_parts<'a>(
    destination_uri: &str,
    options: impl IntoIterator<Item=(&'a str, &'a str)>,
    allow_partition_id: bool,
) -> Result<(String, String)> {
    let table = safe_table_name(destination_uri)?.to_owned();
    let mut seen = Vec::new();
    let mut mode = "create";

    for (key, value) in options {
        if seen.iter().any(|seen_key: &&str| seen_key.eq_ignore_ascii_case(key)) {
            return Err(VeinError::planning(format!("duplicate WRITE option '{key}'"), None));
        }
        seen.push(key);

        if key.eq_ignore_ascii_case("mode") {
            if !["create", "append", "overwrite"].iter().any(|supported| value.eq_ignore_ascii_case(supported)) {
                return Err(VeinError::planning("WRITE mode must be create, append, or overwrite", None));
            }
            mode = if value.eq_ignore_ascii_case("create") {
                "create"
            } else if value.eq_ignore_ascii_case("append") {
                "append"
            } else {
                "overwrite"
            };
        } else if key.eq_ignore_ascii_case("format") {
            if !value.eq_ignore_ascii_case("parquet") {
                return Err(VeinError::planning("local WRITE supports only format='parquet'", None));
            }
        } else if key.eq_ignore_ascii_case("partition_id") && allow_partition_id {
            value.parse::<u16>().map_err(|_| {
                VeinError::planning("invalid internal WRITE partition identifier", None)
            })?;
        } else if key.eq_ignore_ascii_case("partition_id") {
            return Err(VeinError::planning("WRITE option 'partition_id' is reserved for the execution engine", None));
        } else {
            return Err(VeinError::planning(format!("local Parquet WRITE does not support option '{key}'"), None));
        }
    }

    Ok((table, mode.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use common::protocol::KeyValue;
    use plan::CatalogProvider;

    fn target(mode: Option<&str>) -> StorageTarget {
        let mut options = vec![KeyValue::new("format", "parquet")];
        if let Some(mode) = mode {
            options.push(KeyValue::new("mode", mode));
        }
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let mut schema_ipc = Vec::new();
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut schema_ipc, &schema)
            .expect("create schema IPC stream");
        writer.finish().expect("finish schema IPC stream");
        StorageTarget::new(
            "events",
            options,
            Arc::new(plan::VeinSchema::new(vec![Arc::new(
                plan::ResolvedColumn::new(
                    plan::ColumnId(0),
                    "id",
                    None,
                    plan::VeinDataType::Int64,
                ),
            )])),
        )
            .with_schema_ipc(schema_ipc)
    }

    #[test]
    fn validates_parquet_targets_and_rejects_unsupported_or_ambiguous_options() {
        let storage = ParquetStorage::new(std::env::temp_dir());
        assert!(storage.validate_target("events", &[]).is_ok());
        assert!(storage
            .validate_target("events", &[("MODE".into(), "OVERWRITE".into()), ("format".into(), "PARQUET".into())])
            .is_ok());

        for (destination, options, expected) in [
            ("../events", vec![], "single logical table name"),
            ("events", vec![("mode".into(), "merge".into())], "mode must be create, append, or overwrite"),
            ("events", vec![("format".into(), "csv".into())], "supports only format='parquet'"),
            ("events", vec![("compression".into(), "zstd".into())], "does not support option 'compression'"),
            ("events", vec![("mode".into(), "append".into()), ("MODE".into(), "create".into())], "duplicate WRITE option"),
            ("events", vec![("partition_id".into(), "0".into())], "reserved for the execution engine"),
        ] {
            let error = storage.validate_target(destination, &options).err().expect("invalid target should fail preflight");
            assert!(error.to_string().contains(expected), "unexpected error: {error}");
        }
    }

    #[test]
    fn begin_write_revalidates_options_and_accepts_dispatch_partition_metadata() {
        let root = tempfile::tempdir().expect("temporary root");
        let storage = ParquetStorage::new(root.path());
        let mut supported = target(None);
        supported.options.push(KeyValue::new("partition_id", "0"));
        assert!(storage.begin_write(&supported).is_ok());

        let mut unsupported = target(None);
        unsupported.options.push(KeyValue::new("compression", "zstd"));
        let error = storage.begin_write(&unsupported).err().expect("unsupported option should fail");
        assert!(error.to_string().contains("does not support option 'compression'"));
    }

    fn batch(values: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))])
            .expect("valid test batch")
    }

    #[tokio::test]
    async fn staged_create_append_and_overwrite_publish_only_complete_manifests() {
        let root = tempfile::tempdir().expect("temporary root");
        let storage = ParquetStorage::new(root.path());

        let mut create = storage.begin_write(&target(None)).expect("begin create");
        create.write_batch(&batch(vec![1, 2])).expect("write create batch");
        assert!(storage.read_manifest("events").expect("read catalog").is_none());
        let receipt = create.commit().expect("commit create");
        assert_eq!(receipt.row_count, 2);
        let created = storage.read_manifest("events").expect("read committed catalog").expect("created manifest");
        assert_eq!(created.revision, 1);
        assert_eq!(created.files.len(), 1);
        let catalog = crate::parquet_catalog::ParquetCatalog::new(root.path());
        assert_eq!(catalog.get_table("events").await.expect("read created table").files.len(), 1);

        assert!(storage.begin_write(&target(None)).is_err());
        let mut append = storage.begin_write(&target(Some("append"))).expect("begin append");
        append.write_batch(&batch(vec![3])).expect("write append batch");
        append.commit().expect("commit append");
        let appended = storage.read_manifest("events").expect("read appended catalog").expect("appended manifest");
        assert_eq!(appended.revision, 2);
        assert_eq!(appended.files.len(), 2);
        assert_eq!(catalog.get_table("events").await.expect("read appended table").files.len(), 2);

        let mut overwrite = storage.begin_write(&target(Some("overwrite"))).expect("begin overwrite");
        overwrite.write_batch(&batch(vec![9])).expect("write overwrite batch");
        overwrite.commit().expect("commit overwrite");
        let replaced = storage.read_manifest("events").expect("read overwritten catalog").expect("overwritten manifest");
        assert_eq!(replaced.revision, 3);
        assert_eq!(replaced.files.len(), 1);
        assert_eq!(catalog.get_table("events").await.expect("read overwritten table").files.len(), 1);
    }

    #[test]
    fn failed_or_dropped_write_does_not_publish_a_table() {
        let root = tempfile::tempdir().expect("temporary root");
        let storage = ParquetStorage::new(root.path());
        let mut transaction = storage.begin_write(&target(None)).expect("begin create");
        transaction.write_batch(&batch(vec![1])).expect("write first batch");
        let incompatible = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("other", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![2]))],
        ).expect("valid incompatible batch");
        assert!(transaction.write_batch(&incompatible).is_err());
        drop(transaction);
        assert!(storage.read_manifest("events").expect("read catalog").is_none());
    }

    #[tokio::test]
    async fn empty_write_commits_a_valid_schema_only_parquet_file() {
        let root = tempfile::tempdir().expect("temporary root");
        let storage = ParquetStorage::new(root.path());
        let receipt = storage.begin_write(&target(None)).expect("begin write")
            .commit().expect("commit empty table");
        assert_eq!(receipt.row_count, 0);
        let catalog = crate::parquet_catalog::ParquetCatalog::new(root.path());
        let table = catalog.get_table("events").await.expect("read empty table");
        assert_eq!(table.files.len(), 1);
        assert_eq!(table.schema.fields[0].name, "id");
    }

    #[test]
    fn concurrent_stale_writer_cannot_overwrite_new_manifest() {
        let root = tempfile::tempdir().expect("temporary root");
        let first_storage = ParquetStorage::new(root.path());
        let second_storage = ParquetStorage::new(root.path());
        let mut first = first_storage.begin_write(&target(None)).expect("begin first");
        let mut second = second_storage.begin_write(&target(None)).expect("begin second");
        first.write_batch(&batch(vec![1])).expect("write first");
        second.write_batch(&batch(vec![2])).expect("write second");
        first.commit().expect("first commit wins");
        assert!(second.commit().is_err());
        let manifest = first_storage.read_manifest("events").expect("read manifest").expect("committed table");
        assert_eq!(manifest.revision, 1);
        assert_eq!(manifest.files.len(), 1);
    }

    #[test]
    fn manifest_paths_must_be_relative_and_remain_under_storage_root() {
        let root = tempfile::tempdir().expect("temporary root");
        assert!(resolve_manifest_file(root.path(), "../outside.parquet").is_err());
        assert!(resolve_manifest_file(root.path(), "/outside.parquet").is_err());
    }
}
