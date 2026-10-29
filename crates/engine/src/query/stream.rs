use super::{QueryExecution, QueryHandle, QueryResult, ResultBatch};
use futures::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

/// An owning stream over the same cursor, with one-shot terminal error delivery.
pub struct QueryStream<E: QueryExecution + ?Sized = dyn QueryExecution + Send> {
    handle: QueryHandle<E>,
    ended: bool,
}
impl<E: QueryExecution + ?Sized> QueryStream<E> {
    pub(super) fn new(handle: QueryHandle<E>) -> Self { Self { handle, ended: false } }
}
impl<E: QueryExecution + ?Sized> Stream for QueryStream<E> {
    type Item = QueryResult<ResultBatch>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.ended { return Poll::Ready(None); }
        match this.handle.poll_fetch(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(Some(batch))) => Poll::Ready(Some(Ok(batch))),
            Poll::Ready(Ok(None)) => {
                this.ended = true;
                Poll::Ready(None)
            }
            Poll::Ready(Err(error)) => {
                this.ended = true;
                Poll::Ready(Some(Err(error)))
            }
        }
    }
}
