//! Generic adapters for errors originating outside Vein.

use std::error::Error;
use std::sync::Arc;

use crate::ErrorCategory;
use crate::{Result, VeinError};

/// A boxed, thread-safe external error source.
pub type BoxError = Box<dyn Error + Send + Sync + 'static>;

impl VeinError {
    /// Retains a terminal error once, recognizing the owned shared-error adapter.
    pub fn into_shared(self) -> Arc<Self> {
        if let Self::External(error) = &self
            && error.context() == "query execution"
            && let Some(source) = error.source_error().downcast_ref::<Arc<Self>>() {
            return Arc::clone(source);
        }
        Arc::new(self)
    }
    /// Adapts a shared terminal error to an owned boundary error without losing
    /// its typed source chain. Cancellation retains its direct variant.
    pub fn from_shared(source: Arc<Self>) -> Self {
        if matches!(source.as_ref(), Self::Cancelled) { return Self::cancelled(); }
        Self::external(source.category(), "query execution", source)
    }
}

/// An external error with Vein-owned category and operation context.
#[derive(Debug, thiserror::Error)]
#[error("{category} error while {context}: {source}")]
pub struct ExternalError {
    category: ErrorCategory,
    context: String,
    #[source]
    source: BoxError,
}

impl ExternalError {
    /// Wraps an external error without discarding its concrete source type.
    pub fn new<E>(category: ErrorCategory, context: impl Into<String>, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            category,
            context: context.into(),
            source: Box::new(source),
        }
    }

    /// Returns the error category assigned by the Vein boundary.
    #[must_use]
    pub const fn category(&self) -> ErrorCategory {
        self.category
    }

    /// Returns the operation that was being performed.
    #[must_use]
    pub fn context(&self) -> &str {
        &self.context
    }

    /// Returns the original error as a trait object for inspection.
    #[must_use]
    pub fn source_error(&self) -> &(dyn Error + Send + Sync + 'static) {
        self.source.as_ref()
    }
}

/// Extension methods for translating foreign `Result` errors at a subsystem boundary.
pub trait VeinResultExt<T> {
    /// Converts the error into a categorized [`VeinError`] while retaining its source.
    fn map_external_err(
        self,
        category: ErrorCategory,
        context: impl Into<String>,
    ) -> Result<T>;
}

impl<T, E> VeinResultExt<T> for std::result::Result<T, E>
where
    E: Error + Send + Sync + 'static,
{
    fn map_external_err(
        self,
        category: ErrorCategory,
        context: impl Into<String>,
    ) -> Result<T> {
        self.map_err(|source| VeinError::from_error_chain(category, context, source))
    }
}

#[cfg(test)]
mod tests;
