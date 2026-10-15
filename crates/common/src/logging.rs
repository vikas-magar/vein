//! Shared structured logging and audit export initialization for Vein binaries.

use std::env;
use std::fs::{File, OpenOptions};
use std::path::Path;
use tracing_subscriber::{
    EnvFilter, Layer, fmt,
    filter::filter_fn,
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

/// Initializes the process-wide subscriber.
///
/// Set VEIN_LOG_FORMAT=json for ingestion by a log collector. The default
/// format is compact human-readable output. RUST_LOG controls filtering.
/// Set VEIN_AUDIT_LOG_PATH to append structured vein.audit events to a
/// durable JSONL file. The file is append-only and can be shipped by the
/// node's log collector or placed on durable shared storage.
pub fn init(service: &'static str) -> std::io::Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,vein=info"));
    let format = env::var("VEIN_LOG_FORMAT").unwrap_or_else(|_| "pretty".to_string());
    let node_id = env::var("VEIN_NODE_ID").unwrap_or_else(|_| "unknown".to_string());
    let audit_file = open_audit_file()?;

    if format.eq_ignore_ascii_case("json") {
        let stdout = fmt::layer()
            .json()
            .with_target(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .with_current_span(true)
            .with_span_list(true);
        let subscriber = tracing_subscriber::registry().with(filter).with(stdout);
        if let Some(file) = audit_file {
            subscriber
                .with(
                    fmt::layer()
                        .json()
                        .with_target(true)
                        .with_thread_ids(true)
                        .with_thread_names(true)
                        .with_current_span(true)
                        .with_span_list(true)
                        .with_writer(file)
                        .with_filter(filter_fn(|metadata| metadata.target() == "vein.audit")),
                )
                .try_init()
                .map_err(|error| std::io::Error::other(error.to_string()))?;
        } else {
            subscriber
                .try_init()
                .map_err(|error| std::io::Error::other(error.to_string()))?;
        }
    } else {
        let stdout = fmt::layer()
            .compact()
            .with_target(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .with_ansi(env::var("VEIN_LOG_ANSI").map(|v| v != "0").unwrap_or(true));
        let subscriber = tracing_subscriber::registry().with(filter).with(stdout);
        if let Some(file) = audit_file {
            subscriber
                .with(
                    fmt::layer()
                        .json()
                        .with_target(true)
                        .with_thread_ids(true)
                        .with_thread_names(true)
                        .with_current_span(true)
                        .with_span_list(true)
                        .with_writer(file)
                        .with_filter(filter_fn(|metadata| metadata.target() == "vein.audit")),
                )
                .try_init()
                .map_err(|error| std::io::Error::other(error.to_string()))?;
        } else {
            subscriber
                .try_init()
                .map_err(|error| std::io::Error::other(error.to_string()))?;
        }
    }

    tracing::info!(
        target: "vein.audit",
        service,
        node_id = %node_id,
        log_format = %format,
        audit_export = env::var("VEIN_AUDIT_LOG_PATH").is_ok(),
        "logging_initialized"
    );
    Ok(())
}

fn open_audit_file() -> std::io::Result<Option<File>> {
    let Some(path) = env::var_os("VEIN_AUDIT_LOG_PATH") else {
        return Ok(None);
    };
    let path = Path::new(&path);
    if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map(Some)
}

