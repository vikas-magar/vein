//! Process-wide structured logging and privacy-safe diagnostic identities.

mod location;
/// Process-local path identity used in diagnostic events without exposing paths.
pub use location::LogLocation;

#[cfg(test)]
mod tests;

use std::env::{self, VarError};
use std::fs::{File, OpenOptions};
use std::io::{self, ErrorKind};
use std::path::Path;

use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::layer::{Layered, SubscriberExt};
use tracing_subscriber::registry::Registry;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, fmt};

const DEFAULT_FILTER: &str = "info,vein=info";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LogFormat {
    Pretty,
    Json,
}

impl LogFormat {
    fn parse(value: Option<&str>) -> io::Result<Self> {
        match value.unwrap_or("pretty").to_ascii_lowercase().as_str() {
            "pretty" => Ok(Self::Pretty),
            "json" => Ok(Self::Json),
            value => Err(invalid_setting("VEIN_LOG_FORMAT", value)),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Pretty => "pretty",
            Self::Json => "json",
        }
    }
}

/// Initializes the process-wide structured subscriber.
///
/// Diagnostics go to stderr so stdout remains available for query results.
/// `RUST_LOG` controls event levels. `VEIN_LOG_FORMAT` accepts `pretty` or
/// `json`. `VEIN_AUDIT_LOG_PATH` appends JSONL events whose targets begin with
/// `vein.`; this is an event export, not a rotating log-management service.
pub fn init(service: &'static str) -> io::Result<()> {
    let filter = parse_filter(read_env("RUST_LOG")?.as_deref())?;
    let format = LogFormat::parse(read_env("VEIN_LOG_FORMAT")?.as_deref())?;
    let node_id = read_env("VEIN_NODE_ID")?.unwrap_or_else(|| "unknown".to_owned());
    let ansi = parse_ansi(read_env("VEIN_LOG_ANSI")?.as_deref())?;
    let audit_file = open_audit_file()?;

    match format {
        LogFormat::Json => {
            let diagnostics = fmt::layer()
                .with_writer(io::stderr)
                .json()
                .with_target(true)
                .with_thread_ids(true)
                .with_thread_names(true)
                .with_current_span(true)
                .with_span_list(true);
            install(filter, diagnostics, audit_file)?;
        }
        LogFormat::Pretty => {
            let diagnostics = fmt::layer()
                .with_writer(io::stderr)
                .compact()
                .with_target(true)
                .with_thread_ids(true)
                .with_thread_names(true)
                .with_ansi(ansi);
            install(filter, diagnostics, audit_file)?;
        }
    }

    tracing::info!(
        target: "vein.audit",
        service,
        node_id = %node_id,
        log_format = format.name(),
        audit_export = env::var_os("VEIN_AUDIT_LOG_PATH").is_some(),
        "logging_initialized"
    );
    Ok(())
}

fn install<L>(filter: EnvFilter, diagnostics: L, audit_file: Option<File>) -> io::Result<()>
where
    L: Layer<Layered<EnvFilter, Registry>> + Send + Sync + 'static,
{
    let subscriber = tracing_subscriber::registry()
        .with(filter)
        .with(diagnostics);
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
                    .with_filter(filter_fn(|metadata| is_vein_target(metadata.target()))),
            )
            .try_init()
            .map_err(|error| io::Error::other(error.to_string()))
    } else {
        subscriber
            .try_init()
            .map_err(|error| io::Error::other(error.to_string()))
    }
}

fn parse_filter(value: Option<&str>) -> io::Result<EnvFilter> {
    EnvFilter::try_new(value.unwrap_or(DEFAULT_FILTER))
        .map_err(|error| io::Error::new(ErrorKind::InvalidInput, error.to_string()))
}

fn parse_ansi(value: Option<&str>) -> io::Result<bool> {
    match value.map(str::to_ascii_lowercase).as_deref() {
        None | Some("true") | Some("1") | Some("yes") | Some("on") => Ok(true),
        Some("false") | Some("0") | Some("no") | Some("off") => Ok(false),
        Some(value) => Err(invalid_setting("VEIN_LOG_ANSI", value)),
    }
}

fn read_env(name: &str) -> io::Result<Option<String>> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(VarError::NotPresent) => Ok(None),
        Err(VarError::NotUnicode(_)) => Err(io::Error::new(
            ErrorKind::InvalidInput,
            format!("{name} must contain valid Unicode"),
        )),
    }
}

fn invalid_setting(name: &str, value: &str) -> io::Error {
    io::Error::new(
        ErrorKind::InvalidInput,
        format!("invalid {name} value {value:?}"),
    )
}

fn is_vein_target(target: &str) -> bool {
    target.starts_with("vein.")
}

fn open_audit_file() -> io::Result<Option<File>> {
    let Some(path) = env::var_os("VEIN_AUDIT_LOG_PATH") else {
        return Ok(None);
    };
    let path = Path::new(&path);
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map(Some)
}
