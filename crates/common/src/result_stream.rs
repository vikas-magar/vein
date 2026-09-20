//! Query result stream primitives shared by local and distributed execution.

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures::stream::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;

use crate::Result;
use crate::identifiers::TaskId;

/// A batch produced by one executor task.
///
/// The [`RecordBatch`] is moved through the stream. Arrow arrays inside the
/// batch are reference-counted, so forwarding this value does not deep-copy
/// column buffers.
#[derive(Debug)]
pub struct ResultBatch {
    /// The task that produced this batch.
    pub task_id: TaskId,
    /// The task attempt that produced this batch.
    pub attempt: u32,
    /// The monotonically increasing batch sequence within the task attempt.
    pub sequence: u64,
    /// The batch payload.
    pub batch: RecordBatch,
}

impl ResultBatch {
    /// Create a result batch and derive its row count from Arrow.
    pub fn new(task_id: TaskId, attempt: u32, sequence: u64, batch: RecordBatch) -> Self {
        Self {
            task_id,
            attempt,
            sequence,
            batch,
        }
    }

    /// Return the number of rows in the batch without copying data.
    #[inline]
    pub fn row_count(&self) -> usize {
        self.batch.num_rows()
    }
}

/// Lifecycle events emitted by a query result stream.
///
/// Transport errors are represented by the outer [`Result`]. The `Error`
/// variant is reserved for an explicit query-level error event that must be
/// delivered in stream order.
#[derive(Debug)]
pub enum ResultEvent {
    /// Announces the schema before the first batch.
    Start {
        /// The schema shared by all batches in this result stream.
        schema: SchemaRef,
    },
    /// Delivers one task-produced batch.
    Batch(ResultBatch),
    /// Marks a finite result stream as complete.
    End {
        /// Total rows emitted before completion.
        row_count: u64,
    },
    /// Reports an ordered query-level failure.
    Error {
        /// Human-readable failure description.
        message: String,
    },
}

/// A sendable stream of query result events.
///
/// The stream owns each [`ResultEvent`] as it is yielded. Consumers should
/// apply backpressure by polling only when they can process another event.
pub trait ResultStream: Stream<Item = Result<ResultEvent>> + Send {}

impl<T> ResultStream for T where T: Stream<Item = Result<ResultEvent>> + Send {}

/// A boxed query result stream used at execution boundaries.
pub type SendableResultStream = Pin<Box<dyn ResultStream>>;

/// Create an empty, already-completed result stream.
pub fn empty_result_stream() -> SendableResultStream {
    Box::pin(futures::stream::empty())
}

/// A sender for a bounded query result stream.
#[derive(Clone)]
pub struct ResultChannelSender {
    inner: mpsc::Sender<Result<ResultEvent>>,
}

impl ResultChannelSender {
    /// Send one result event, waiting for capacity when the consumer is slow.
    pub async fn send(
        &self,
        event: Result<ResultEvent>,
    ) -> std::result::Result<(), mpsc::error::SendError<Result<ResultEvent>>> {
        self.inner.send(event).await
    }
}

/// A receiver-backed result stream with bounded buffering.
pub struct ResultChannelReceiver {
    inner: mpsc::Receiver<Result<ResultEvent>>,
}

impl Stream for ResultChannelReceiver {
    type Item = Result<ResultEvent>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_recv(cx)
    }
}

/// Create a bounded result channel.
pub fn bounded_result_channel(capacity: usize) -> (ResultChannelSender, SendableResultStream) {
    assert!(capacity > 0, "result channel capacity must be positive");
    let (sender, receiver) = mpsc::channel(capacity);
    (
        ResultChannelSender { inner: sender },
        Box::pin(ResultChannelReceiver { inner: receiver }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::StreamExt;
    use std::sync::Arc;

    #[test]
    fn result_batch_moves_record_batch_without_recomputing_metadata() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))])
            .expect("test batch should be valid");
        let task_id = TaskId::from_raw(7);
        let result = ResultBatch::new(task_id, 2, 4, batch);

        assert_eq!(result.task_id, task_id);
        assert_eq!(result.attempt, 2);
        assert_eq!(result.sequence, 4);
        assert_eq!(result.row_count(), 3);
    }

    #[test]
    fn empty_result_stream_is_constructible() {
        let _stream = empty_result_stream();
    }

    #[tokio::test]
    async fn bounded_result_channel_delivers_owned_events() {
        let (sender, mut stream) = bounded_result_channel(1);
        sender
            .send(Ok(ResultEvent::End { row_count: 3 }))
            .await
            .expect("receiver should still be connected");

        let event = stream.next().await.expect("event should be available");
        assert!(matches!(event, Ok(ResultEvent::End { row_count: 3 })));
    }
}
