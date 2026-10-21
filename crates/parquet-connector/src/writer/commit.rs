use std::fs;
use std::io::Write;

use crate::catalog::manifest::TableManifest;
use catalog::TableStatistics;
use common::{Result, VeinError};
use exec::operators::sink::WriteReceipt;

use super::filesystem;
use super::transaction::ParquetWriteTransaction;

pub(super) fn commit(mut transaction: Box<ParquetWriteTransaction>) -> Result<WriteReceipt> {
    let _guard = filesystem::table_commit_lock(&transaction.storage.root, &transaction.table)?;
    let current = transaction.storage.read_manifest(&transaction.table)?;
    if current.as_ref() != transaction.base_manifest.as_ref() {
        return Err(VeinError::planning(
            format!("table '{}' changed while WRITE was running; retry against the new revision", transaction.table), None,
        ));
    }
    validate_current_target(&transaction, current.as_ref())?;
    if transaction.mode == "append" { transaction.validate_append_schema(current.as_ref())?; }

    transaction.ensure_writer()?;
    let writer = transaction.writer.take().ok_or_else(|| VeinError::internal("Parquet writer initialization failed"))?;
    let mut output = writer.into_inner()
        .map_err(|error| VeinError::internal(format!("finish staged Parquet writer: {error}")))?;
    output.flush().map_err(|error| VeinError::io("flush staged Parquet file", error))?;
    output.get_ref().sync_all().map_err(|error| VeinError::io("sync staged Parquet file", error))?;
    drop(output);

    let final_dir = transaction.storage.root.join(".vein").join("tables")
        .join(&transaction.table).join(&transaction.generation);
    fs::create_dir_all(&final_dir)
        .map_err(|error| VeinError::io(format!("create generation directory '{}'", final_dir.display()), error))?;
    let final_file = final_dir.join("part.parquet");
    fs::rename(transaction.staging_dir.join("part.parquet"), &final_file)
        .map_err(|error| VeinError::io("publish immutable Parquet generation", error))?;
    filesystem::sync_directory_chain(&final_dir, &transaction.storage.root)?;

    let manifest = build_manifest(&mut transaction, current, &final_file)?;
    let manifest_path = transaction.storage.manifest_path(&transaction.table)?;
    if let Err(error) = filesystem::publish_manifest(&manifest_path, &manifest, &transaction.generation) {
        if !matches!(&error, VeinError::CommitOutcomeUnknown { .. }) { let _ = fs::remove_dir_all(&final_dir); }
        return Err(error);
    }
    let _ = fs::remove_dir_all(&transaction.staging_dir);
    Ok(WriteReceipt { destination: transaction.table.clone(), row_count: transaction.rows })
}

fn validate_current_target(transaction: &ParquetWriteTransaction, current: Option<&TableManifest>) -> Result<()> {
    match (transaction.mode.as_str(), current) {
        ("create", Some(_)) => Err(VeinError::planning(format!("table '{}' already exists", transaction.table), None)),
        ("append" | "overwrite", None) => Err(VeinError::not_found(format!("managed table '{}' no longer exists", transaction.table))),
        ("create", None) if transaction.storage.root.join(&transaction.table).exists() => Err(VeinError::planning(
            format!("target '{}' appeared while WRITE was running", transaction.storage.root.join(&transaction.table).display()), None,
        )),
        _ => Ok(()),
    }
}

fn build_manifest(
    transaction: &mut ParquetWriteTransaction,
    current: Option<TableManifest>,
    final_file: &std::path::Path,
) -> Result<TableManifest> {
    let mut files = match (transaction.mode.as_str(), current) {
        ("append", Some(manifest)) => manifest.files,
        _ => Vec::new(),
    };
    let relative = final_file.strip_prefix(transaction.storage.root.as_path())
        .map_err(|_| VeinError::internal("published artifact is outside the storage root"))?;
    files.push(relative.to_string_lossy().into_owned());
    let revision = transaction.base_manifest.as_ref().map_or(Ok(1), |base| {
        base.revision.checked_add(1).ok_or_else(|| VeinError::internal("table catalog revision overflow"))
    })?;
    let statistics = completed_statistics(transaction, &relative.to_string_lossy(), revision)?;
    Ok(TableManifest { revision, files, statistics })
}

fn completed_statistics(
    transaction: &mut ParquetWriteTransaction,
    new_file: &str,
    revision: u64,
) -> Result<Option<catalog::AnalyzedTableStatistics>> {
    let Some(collector) = transaction.statistics.take() else { return Ok(None); };
    let mut source_identities = if transaction.mode == "append" {
        transaction.base_manifest.as_ref().and_then(|manifest| manifest.statistics.as_ref())
            .map(|statistics| statistics.source_identities.clone()).unwrap_or_default()
    } else { Vec::new() };
    source_identities.push(transaction.storage.source_identity(new_file)?);
    source_identities.sort_by(|left, right| left.identifier.cmp(&right.identifier));
    let statistics = collector.finish();
    log_statistics(&transaction.table, revision, &statistics);
    Ok(Some(catalog::AnalyzedTableStatistics {
        source_revision: revision,
        schema_fingerprint: transaction.schema_fingerprint.clone(),
        source_identities,
        statistics,
    }))
}

fn log_statistics(table: &str, revision: u64, statistics: &TableStatistics) {
    tracing::debug!(
        target: "vein.audit", event = "statistics_analysis_completed", table = %table, revision,
        row_count = statistics.num_rows.unwrap_or_default(),
        column_count = statistics.column_statistics.len(),
        distinct_columns = statistics.column_statistics.values().filter(|column| column.distinct_count.is_some()).count(),
        bounded_columns = statistics.column_statistics.values().filter(|column| column.min_max_complete == Some(true)).count(),
        numeric_columns = statistics.column_statistics.values().filter(|column| column.numeric.is_some()).count(),
        variable_length_columns = statistics.column_statistics.values().filter(|column| column.byte_length.is_some()).count(),
        "statistics_analysis_completed"
    );
}
