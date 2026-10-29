use super::ResultBatch;
use common::{Result, VeinError};
use exec::operators::sink::WriteReceipt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// The central Vein error retained once across repeated terminal fetches.
pub type QueryResult<T> = std::result::Result<T, Arc<VeinError>>;

/// Result semantics, independent of the client or transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum QueryKind {
    /// A row-producing query; an empty batch is not completion.
    Rows,
    /// A transactional write; completion requires a confirmed receipt.
    Write,
}

/// One owned output or an explicit terminal event from an execution backend.
pub enum QueryEvent {
    /// Transfer a leased canonical view, without materializing its columns.
    Batch(ResultBatch),
    /// Output was drained, or a write was committed.
    Complete {
        /// Present only when the storage backend confirmed a commit.
        write_receipt: Option<WriteReceipt>,
    },
}

/// Extensible execution boundary for any query producer, with no client branches.
///
/// Pending must register the supplied waker with the awaited resource. Backend
/// state survives dropped fetch futures; accepted output cannot be replayed.
/// Computation completion alone must not mark the query finished while unread
/// output remains. Implementations share the supplied query resource scope.
pub trait QueryExecution {
    /// Polls one output event, yielding to the host when work cannot progress.
    fn poll_event(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<QueryEvent>>;
}
