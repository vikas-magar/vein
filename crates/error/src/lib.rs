#![deny(missing_docs)]

//! Unified error type for the entire Vein engine.

use std::time::Duration;

use thiserror::Error;

/// Stable error domains shared by all Vein components.
pub mod category;
mod external;
mod parse;
mod resource;
mod span;

pub use category::ErrorCategory;
pub use external::{BoxError, ExternalError, VeinResultExt};
pub use parse::{LexicalError, ParseError};
pub use resource::ResourceUnit;
pub use span::SourceSpan;

/// Result alias using [`VeinError`].
pub type Result<T> = std::result::Result<T, VeinError>;

/// Unified error type for all Vein engine components.
///
/// Every subsystem reports Vein-owned failures through this enum. External
/// libraries are represented by [`ExternalError`], retaining their original
/// source while keeping vendor types out of public subsystem APIs.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum VeinError {
    // ── Input ──────────────────────────────────────────────────
    /// A command or API argument is invalid before query parsing begins.
    #[error("Invalid input: {0}")]
    InvalidInput(String),

    // ── Parsing and binding ───────────────────────────────────
    /// Tokenization or query syntax failed.
    #[error(transparent)]
    Parse(#[from] ParseError),

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

    /// An error from an external library, tagged with Vein context.
    #[error(transparent)]
    External(#[from] ExternalError),

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
        span: Option<SourceSpan>,
    },

    /// A valid query requests a feature not implemented by the planner or engine.
    #[error("Unsupported feature: {message}")]
    Unsupported {
        /// Description of the unsupported feature and available next action.
        message: String,
        /// The source span of the offending syntax, if known.
        span: Option<SourceSpan>,
    },

    /// A query cannot be lowered because a physical planning contract is invalid.
    #[error("Planning error: {message}")]
    Planning {
        /// Description of the invalid planning contract.
        message: String,
        /// The source span of the offending syntax, if known.
        span: Option<SourceSpan>,
    },

    /// Context added while preserving the category and source of another error.
    #[error("{context}: {source}")]
    Context {
        /// Operation that was in progress when the source error occurred.
        context: String,
        /// Original typed error.
        #[source]
        source: Box<VeinError>,
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

    /// A bounded execution resource could not satisfy a reservation.
    #[error("Resource exhausted: {resource}, requested {requested} {unit}, limit {limit} {unit}")]
    ResourceExhausted {
        /// Resource whose budget was exceeded.
        resource: String,
        /// Amount requested by the operation.
        requested: u128,
        /// Configured resource limit.
        limit: u128,
        /// Unit used to measure the resource.
        unit: ResourceUnit,
    },

    /// A metadata publication became visible, but durable directory sync failed.
    #[error("Commit outcome unknown for {entity}: {message}")]
    CommitOutcomeUnknown {
        /// Resource whose commit may already be visible.
        entity: String,
        /// Reason durability could not be confirmed.
        message: String,
    },

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
        source: Option<BoxError>,
    },
}

// ── Constructors ──────────────────────────────────────────────

impl VeinError {
    /// Create an invalid input or invocation error.
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::InvalidInput(message.into())
    }

    /// Returns a stable classification suitable for telemetry and caller policy.
    #[must_use]
    pub const fn category(&self) -> ErrorCategory {
        match self {
            Self::InvalidInput(_) => ErrorCategory::Input,
            Self::Parse(error) => error.category(),
            Self::Io { .. } => ErrorCategory::Storage,
            Self::Serialization(_) => ErrorCategory::Data,
            Self::NotFound(_) => ErrorCategory::Catalog,
            Self::Semantic { .. } => ErrorCategory::Semantic,
            Self::Unsupported { .. } => ErrorCategory::Unsupported,
            Self::Planning { .. } => ErrorCategory::Planning,
            Self::Context { source, .. } => source.category(),
            Self::StageError { .. }
            | Self::Cancelled
            | Self::Timeout { .. }
            | Self::NoExecutorsAvailable
            | Self::TaskFailed { .. }
            | Self::ExecutionFailed(_)
            | Self::PartitionAlreadyDrained(_)
            | Self::ShuffleKeyNotFound
            | Self::Shuffle(_) => ErrorCategory::Execution,
            Self::ResourceExhausted { .. } => ErrorCategory::Resource,
            Self::CommitOutcomeUnknown { .. } => ErrorCategory::Catalog,
            Self::ConnectionFailed(_) | Self::QuicSend(_) => ErrorCategory::Transport,
            Self::Internal { .. } => ErrorCategory::Internal,
            Self::External(error) => error.category(),
        }
    }

    /// Returns the source errors below this error, from nearest to deepest.
    ///
    /// The iterator borrows the typed source chain and allocates nothing. It is
    /// useful for structured logging while callers retain the original errors.
    #[must_use]
    pub fn causes(&self) -> impl Iterator<Item=&(dyn std::error::Error + 'static)> {
        std::iter::successors(
            std::error::Error::source(self),
            |cause| std::error::Error::source(*cause),
        )
    }

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

    /// Create an execution failure with a concise message.
    pub fn execution_failed(message: impl Into<String>) -> Self {
        Self::ExecutionFailed(message.into())
    }

    /// Create a not-found error.
    pub fn not_found(entity: impl Into<String>) -> Self {
        Self::NotFound(entity.into())
    }

    /// Create a semantic error with an optional source span.
    pub fn semantic(message: impl Into<String>, span: Option<SourceSpan>) -> Self {
        Self::Semantic {
            message: message.into(),
            span,
        }
    }

    /// Create an error for an unsupported feature with an optional source span.
    pub fn unsupported(message: impl Into<String>, span: Option<SourceSpan>) -> Self {
        Self::Unsupported {
            message: message.into(),
            span,
        }
    }

    /// Create an error for an invalid planning contract with an optional span.
    pub fn planning(message: impl Into<String>, span: Option<SourceSpan>) -> Self {
        Self::Planning {
            message: message.into(),
            span,
        }
    }

    /// Add operation context without discarding the source error or its category.
    pub fn context(context: impl Into<String>, source: Self) -> Self {
        Self::Context {
            context: context.into(),
            source: Box::new(source),
        }
    }

    /// Create an internal error with a message.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
            source: None,
        }
    }

    /// Creates an internal error while preserving a thread-safe source cause.
    pub fn internal_with_source<E>(message: impl Into<String>, source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::Internal {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    /// Wraps an external error with its Vein category and operation context.
    pub fn external<E>(category: ErrorCategory, context: impl Into<String>, source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::External(ExternalError::new(category, context, source))
    }

    /// Converts an external error while preserving a canonical resource error
    /// found anywhere in the exposed source chain. Some foreign APIs discard
    /// their original cause; callers with richer operation context should
    /// resolve those failures before using this fallback.
    pub fn from_error_chain<E>(
        category: ErrorCategory,
        context: impl Into<String>,
        source: E,
    ) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        if let Some(error) = Self::resource_error_from_chain(&source) {
            return error;
        }
        Self::external(category, context, source)
    }

    /// Create a bounded-resource exhaustion error.
    pub fn resource_exhausted(
        resource: impl Into<String>,
        requested: u128,
        limit: u128,
    ) -> Self {
        Self::resource_limit_exceeded(resource, requested, limit, ResourceUnit::Bytes)
    }

    /// Create a resource-limit error measured in an explicit unit.
    pub fn resource_limit_exceeded(
        resource: impl Into<String>,
        requested: u128,
        limit: u128,
        unit: impl Into<ResourceUnit>,
    ) -> Self {
        Self::ResourceExhausted {
            resource: resource.into(),
            requested,
            limit,
            unit: unit.into(),
        }
    }

    fn resource_error_from_chain(error: &(dyn std::error::Error + 'static)) -> Option<Self> {
        let mut cause = Some(error);
        while let Some(error) = cause {
            if let Some(vein_error) = error.downcast_ref::<Self>()
                && let Some(resource_error) = Self::copy_resource_error(vein_error)
            {
                return Some(resource_error);
            }
            // `io::Error::other` stores its original error in `get_ref`, but
            // its `source()` may skip that value and expose only its cause.
            if let Some(io_error) = error.downcast_ref::<std::io::Error>()
                && let Some(inner) = io_error.get_ref()
                && let Some(vein_error) = inner.downcast_ref::<Self>()
                && let Some(resource_error) = Self::copy_resource_error(vein_error)
            {
                return Some(resource_error);
            }
            cause = error.source();
        }
        None
    }

    fn copy_resource_error(error: &Self) -> Option<Self> {
        let Self::ResourceExhausted {
            resource,
            requested,
            limit,
            unit,
        } = error
        else {
            return None;
        };

        Some(Self::resource_limit_exceeded(
            resource.clone(),
            *requested,
            *limit,
            unit.clone(),
        ))
    }

    /// Create an error for a commit whose visibility is known but durability is uncertain.
    pub fn commit_outcome_unknown(message: impl Into<String>) -> Self {
        Self::CommitOutcomeUnknown {
            entity: "metadata manifest".to_owned(),
            message: message.into(),
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
        if let Some(error) = Self::resource_error_from_chain(&e) {
            return error;
        }
        Self::io("I/O operation failed", e)
    }
}

impl From<arrow::error::ArrowError> for VeinError {
    fn from(source: arrow::error::ArrowError) -> Self {
        Self::from_error_chain(ErrorCategory::Data, "processing Arrow data", source)
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
    fn invalid_input_has_stable_display_and_category() {
        let error = VeinError::invalid_input("query argument is required");
        assert_eq!(error.to_string(), "Invalid input: query argument is required");
        assert_eq!(error.category(), ErrorCategory::Input);
    }

    #[test]
    fn test_semantic_with_and_without_span() {
        let err = VeinError::semantic("unknown column", None);
        assert_eq!(err.to_string(), "Semantic error: unknown column");
        assert!(matches!(err, VeinError::Semantic { span: None, .. }));

        let span = SourceSpan::new(4, 9, 1, 5);
        let err = VeinError::semantic("unknown column", Some(span));
        let VeinError::Semantic { span: Some(s), .. } = err else {
            panic!("expected span to be preserved");
        };
        assert_eq!(s, span);
    }

    #[test]
    fn test_planning_renders_and_carries_span() {
        let span = SourceSpan::new(0, 5, 1, 1);
        let err = VeinError::planning("join key is absent from its input", Some(span));
        assert_eq!(
            err.to_string(),
            "Planning error: join key is absent from its input"
        );
        assert!(matches!(
            err,
            VeinError::Planning {
                span: Some(ref s),
                ..
            } if *s == span
        ));
    }

    #[test]
    fn unsupported_and_context_errors_preserve_category_and_source_chain() {
        let error = VeinError::context(
            "lowering a recursive filter",
            VeinError::external(
                ErrorCategory::Unsupported,
                "VM expression lowering",
                std::io::Error::other("expression opcode unavailable"),
            ),
        );

        assert_eq!(
            error.category(),
            ErrorCategory::Unsupported,
            "adding context must not change the underlying classification"
        );
        assert!(error.to_string().contains("lowering a recursive filter"));
        assert!(error.to_string().contains("expression opcode unavailable"));
        let causes = error.causes().map(ToString::to_string).collect::<Vec<_>>();
        assert!(
            causes
                .iter()
                .any(|cause| cause.contains("expression opcode unavailable")),
            "typed causes should remain reachable: {causes:?}"
        );
    }

    #[test]
    fn arrow_errors_use_the_generic_external_wrapper() {
        let arrow_err = arrow::error::ArrowError::ParseError("bad data".into());
        let vein: VeinError = arrow_err.into();
        assert_eq!(vein.category(), ErrorCategory::Data);
        let VeinError::External(external) = vein else {
            panic!("Arrow errors should use the generic external wrapper");
        };
        assert!(external
            .source_error()
            .downcast_ref::<arrow::error::ArrowError>()
            .is_some());
    }

    #[test]
    fn parser_errors_are_central_and_preserve_spans() {
        let span = SourceSpan::new(4, 9, 1, 5);
        let parse_error = ParseError::Expected {
            expected: "FROM".into(),
            found: "SELECT".into(),
            span,
        };
        let vein = VeinError::from(parse_error);

        assert_eq!(vein.category(), ErrorCategory::Syntax);
        assert!(matches!(
            vein,
            VeinError::Parse(ParseError::Expected { span: actual, .. }) if actual == span
        ));
    }

    #[test]
    fn every_engine_error_has_a_stable_category() {
        let cases = [
            (
                VeinError::invalid_input("missing query"),
                ErrorCategory::Input,
            ),
            (
                VeinError::Parse(ParseError::Unsupported("feature".into())),
                ErrorCategory::Unsupported,
            ),
            (
                VeinError::io("reading metadata", std::io::Error::other("failed")),
                ErrorCategory::Storage,
            ),
            (VeinError::Serialization("invalid payload".into()), ErrorCategory::Data),
            (VeinError::NotFound("table".into()), ErrorCategory::Catalog),
            (
                VeinError::semantic("unknown column", None),
                ErrorCategory::Semantic,
            ),
            (
                VeinError::planning("join key is absent from its input", None),
                ErrorCategory::Planning,
            ),
            (
                VeinError::unsupported("window frames are not implemented", None),
                ErrorCategory::Unsupported,
            ),
            (
                VeinError::context(
                    "compiling a source",
                    VeinError::unsupported("nested loop join is not implemented", None),
                ),
                ErrorCategory::Unsupported,
            ),
            (VeinError::stage("scan", "failed"), ErrorCategory::Execution),
            (VeinError::cancelled(), ErrorCategory::Execution),
            (
                VeinError::Timeout {
                    duration: Duration::from_millis(1),
                },
                ErrorCategory::Execution,
            ),
            (VeinError::NoExecutorsAvailable, ErrorCategory::Execution),
            (
                VeinError::TaskFailed {
                    task_id: "task-1".into(),
                    message: "failed".into(),
                },
                ErrorCategory::Execution,
            ),
            (
                VeinError::execution_failed("failed"),
                ErrorCategory::Execution,
            ),
            (
                VeinError::resource_exhausted("memory", 2, 1),
                ErrorCategory::Resource,
            ),
            (
                VeinError::CommitOutcomeUnknown {
                    entity: "manifest".into(),
                    message: "sync failed".into(),
                },
                ErrorCategory::Catalog,
            ),
            (
                VeinError::PartitionAlreadyDrained(0),
                ErrorCategory::Execution,
            ),
            (VeinError::ShuffleKeyNotFound, ErrorCategory::Execution),
            (VeinError::shuffle("failed"), ErrorCategory::Execution),
            (
                VeinError::connection_failed("refused"),
                ErrorCategory::Transport,
            ),
            (VeinError::quic_send("failed"), ErrorCategory::Transport),
            (VeinError::internal("invariant failed"), ErrorCategory::Internal),
        ];

        for (error, expected) in cases {
            assert_eq!(error.category(), expected, "unexpected category for {error}");
        }
    }

    #[test]
    fn external_result_adapter_keeps_context_category_and_source() {
        let result: std::result::Result<(), std::io::Error> = Err(std::io::Error::other("disk full"));
        let vein = result
            .map_external_err(ErrorCategory::Storage, "writing spill run")
            .expect_err("the source result should fail");

        assert_eq!(vein.category(), ErrorCategory::Storage);
        assert!(vein.to_string().contains("writing spill run"));
        let VeinError::External(external) = vein else {
            panic!("foreign errors should use the generic external wrapper");
        };
        assert_eq!(external.source_error().to_string(), "disk full");
        assert!(external.source_error().downcast_ref::<std::io::Error>().is_some());
    }

    #[test]
    fn resource_error_survives_nested_io_wrapper() {
        let source = std::io::Error::other(VeinError::resource_exhausted(
            "query spill disk",
            17,
            8,
        ));
        let error: VeinError = source.into();

        assert_eq!(error.category(), ErrorCategory::Resource);
        assert!(matches!(
            error,
            VeinError::ResourceExhausted {
                resource,
                requested: 17,
                limit: 8,
                unit: ResourceUnit::Bytes,
            } if resource == "query spill disk"
        ));
    }

    #[test]
    fn resource_limit_errors_support_custom_units_and_wide_values() {
        let requested = u64::MAX.into();
        let limit = u32::MAX.into();
        let error = VeinError::resource_limit_exceeded(
            "recursive partitions",
            requested,
            limit,
            "partitions",
        );

        assert_eq!(error.category(), ErrorCategory::Resource);
        assert!(error.to_string().contains("requested 18446744073709551615 partitions"));
        assert!(error.to_string().contains("limit 4294967295 partitions"));
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
    fn test_quic_send() {
        let err = VeinError::quic_send("timeout");
        assert!(err.to_string().contains("QUIC send"));
    }
}
