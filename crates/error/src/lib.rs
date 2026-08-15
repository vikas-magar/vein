#![deny(missing_docs)]

//! Unified error type for the entire Vein engine.

use std::time::Duration;

use thiserror::Error;

/// Result alias using [`VeinError`].
pub type Result<T> = std::result::Result<T, VeinError>;

/// Unified error type for all Vein engine components.
///
/// Every subsystem (pipeline, storage, transport, catalog, execution)
/// reports errors through this single enum. External error types are
/// converted via `From` impls defined here.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum VeinError {
    // ── System ──────────────────────────────────────────────
    /// An I/O operation failed.
    #[error("I/O error: {context}")]
    Io {
        /// Description of the operation that failed.
        context: String,
        /// The underlying I/O error, if available.
        #[source]
        source: Option<std::io::Error>,
    },

    /// An Arrow error (deserialization, schema, etc.).
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// An Iceberg catalog error.
    #[error("Iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),

    /// Serialization or deserialization failure.
    #[error("Serialization error: {0}")]
    Serialization(String),

    // ── Catalog ──────────────────────────────────────────────
    /// A table or namespace was not found in the catalog.
    #[error("Not found: {0}")]
    NotFound(String),

    // ── Planning ─────────────────────────────────────────────
    /// A semantic violation detected during query binding (unknown column,
    /// ambiguous reference, type mismatch, invalid aggregate, ...).
    #[error("Semantic error: {message}")]
    Semantic {
        /// Description of the semantic violation.
        message: String,
        /// The source span of the offending syntax, if known.
        span: Option<parser::token::Span>,
    },

    /// A planning error: the query is syntactically valid but a requested
    /// feature is not yet supported by the planner.
    #[error("Planning error: {message}")]
    Planning {
        /// Description of the unsupported feature.
        message: String,
        /// The source span of the offending syntax, if known.
        span: Option<parser::token::Span>,
    },

    // ── Execution ────────────────────────────────────────────
    /// An error during pipeline stage execution.
    #[error("Stage error in {stage}: {message}")]
    StageError {
        /// The name of the stage that failed.
        stage: String,
        /// The error message.
        message: String,
    },

    /// The task or query was cancelled.
    #[error("Cancelled")]
    Cancelled,

    /// A timeout occurred.
    #[error("Timeout after {duration:?}")]
    Timeout {
        /// The duration that was exceeded.
        duration: Duration,
    },

    /// No executors available to schedule work.
    #[error("No executors available")]
    NoExecutorsAvailable,

    /// A task failed with a message.
    #[error("Task {task_id} failed: {message}")]
    TaskFailed {
        /// The ID of the failed task.
        task_id: String,
        /// The failure message.
        message: String,
    },

    /// Query execution failed.
    #[error("Execution failed: {0}")]
    ExecutionFailed(String),

    // ── Shuffle ──────────────────────────────────────────────
    /// A partition was drained more than once.
    #[error("Partition {0} already drained")]
    PartitionAlreadyDrained(u16),

    /// A shuffle key was not found.
    #[error("Shuffle key not found")]
    ShuffleKeyNotFound,

    /// A shuffle operation failed.
    #[error("Shuffle error: {0}")]
    Shuffle(String),

    // ── Transport ────────────────────────────────────────────
    /// A connection failed.
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    /// A QUIC send operation failed.
    #[error("QUIC send error: {0}")]
    QuicSend(String),

    // ── Internal ─────────────────────────────────────────────
    /// An internal or unexpected error.
    #[error("Internal error: {message}")]
    Internal {
        /// The error message.
        message: String,
        /// The underlying source error, if any.
        #[source]
        source: Option<Box<dyn std::error::Error + Send>>,
    },
}

// ── Constructors ──────────────────────────────────────────────

impl VeinError {
    /// Create an I/O error with context and underlying source.
    pub fn io(context: impl Into<String>, source: std::io::Error) -> Self {
        Self::Io {
            context: context.into(),
            source: Some(source),
        }
    }

    /// Create a stage execution error.
    pub fn stage(stage: impl Into<String>, message: impl Into<String>) -> Self {
        Self::StageError {
            stage: stage.into(),
            message: message.into(),
        }
    }

    /// Create a not-found error.
    pub fn not_found(entity: impl Into<String>) -> Self {
        Self::NotFound(entity.into())
    }

    /// Create a semantic error with an optional source span.
    pub fn semantic(message: impl Into<String>, span: Option<parser::token::Span>) -> Self {
        Self::Semantic {
            message: message.into(),
            span,
        }
    }

    /// Create a planning error (valid syntax, unsupported feature) with an
    /// optional source span.
    pub fn planning(message: impl Into<String>, span: Option<parser::token::Span>) -> Self {
        Self::Planning {
            message: message.into(),
            span,
        }
    }

    /// Create an internal error with a message.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
            source: None,
        }
    }

    /// Create a cancellation error.
    pub fn cancelled() -> Self {
        Self::Cancelled
    }

    /// Create a connection failure error.
    pub fn connection_failed(msg: impl Into<String>) -> Self {
        Self::ConnectionFailed(msg.into())
    }

    /// Create a serialization error.
    pub fn serialization(msg: impl Into<String>) -> Self {
        Self::Serialization(msg.into())
    }

    /// Create a shuffle error.
    pub fn shuffle(msg: impl Into<String>) -> Self {
        Self::Shuffle(msg.into())
    }

    /// Create a QUIC send error.
    pub fn quic_send(msg: impl Into<String>) -> Self {
        Self::QuicSend(msg.into())
    }
}

// ── From impls ────────────────────────────────────────────────

impl From<std::io::Error> for VeinError {
    fn from(e: std::io::Error) -> Self {
        Self::io("I/O operation failed", e)
    }
}

impl From<String> for VeinError {
    fn from(s: String) -> Self {
        Self::Internal {
            message: s,
            source: None,
        }
    }
}

impl From<&str> for VeinError {
    fn from(s: &str) -> Self {
        Self::Internal {
            message: s.to_string(),
            source: None,
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────

#[cfg(test)]
#[allow(missing_docs)]
mod tests {
    use super::*;

    #[test]
    fn test_display_not_found() {
        let err = VeinError::NotFound("orders".into());
        assert_eq!(err.to_string(), "Not found: orders");
    }

    #[test]
    fn test_semantic_with_and_without_span() {
        let err = VeinError::semantic("unknown column", None);
        assert_eq!(err.to_string(), "Semantic error: unknown column");
        assert!(matches!(err, VeinError::Semantic { span: None, .. }));

        let span = parser::token::Span::new(4, 9, 1, 5);
        let err = VeinError::semantic("unknown column", Some(span));
        let VeinError::Semantic { span: Some(s), .. } = err else {
            panic!("expected span to be preserved");
        };
        assert_eq!(s, span);
    }

    #[test]
    fn test_planning_renders_and_carries_span() {
        let span = parser::token::Span::new(0, 5, 1, 1);
        let err = VeinError::planning("WITH is not supported yet", Some(span));
        assert_eq!(err.to_string(), "Planning error: WITH is not supported yet");
        assert!(matches!(
            err,
            VeinError::Planning {
                span: Some(ref s),
                ..
            } if *s == span
        ));
    }

    #[test]
    fn test_from_arrow() {
        let arrow_err = arrow::error::ArrowError::ParseError("bad data".into());
        let vein: VeinError = arrow_err.into();
        assert!(matches!(vein, VeinError::Arrow(_)));
    }

    #[test]
    fn test_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let vein: VeinError = io_err.into();
        assert!(matches!(vein, VeinError::Io { .. }));
    }

    #[test]
    fn test_io_with_context() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "no access");
        let vein = VeinError::io("reading config", io_err);
        assert!(vein.to_string().contains("reading config"));
    }

    #[test]
    fn test_cancelled() {
        let err = VeinError::cancelled();
        assert_eq!(err.to_string(), "Cancelled");
    }

    #[test]
    fn test_internal_from_str() {
        let err: VeinError = "something broke".into();
        assert!(matches!(err, VeinError::Internal { .. }));
    }

    #[test]
    fn test_from_iceberg() {
        // Iceberg's own error type — verify the From path compiles
        let _vein: VeinError = VeinError::Iceberg(iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "catalog error",
        ));
    }

    #[test]
    fn test_quic_send() {
        let err = VeinError::quic_send("timeout");
        assert!(err.to_string().contains("QUIC send"));
    }
}
