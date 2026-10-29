use super::super::{QueryEvent, QueryStatus};
use super::*;
use common::VeinError;
use std::task::{Context, Poll};

impl<E: QueryExecution + ?Sized> QueryHandle<E> {
    /// Polls without allocating a fetch task; at most one cursor task may register.
    pub fn poll_fetch(&mut self, cx: &mut Context<'_>) -> Poll<QueryResult<Option<ResultBatch>>> {
        self.control.resources.register_cursor_waker(cx.waker());
        if let Some(terminal) = self.terminal() { return Poll::Ready(terminal); }
        self.control.resources.begin_query();
        let Some(execution) = &mut self.execution else {
            return Poll::Ready(self.failed(VeinError::internal("query execution is unavailable")));
        };
        match execution.as_mut().poll_event(cx) {
            Poll::Pending => match self.terminal() {
                Some(terminal) => Poll::Ready(terminal),
                None => Poll::Pending,
            },
            Poll::Ready(Err(error)) => Poll::Ready(self.failed(error)),
            Poll::Ready(Ok(QueryEvent::Batch(batch))) => {
                if let Some(terminal) = self.terminal() { return Poll::Ready(terminal); }
                let validation = if self.kind == QueryKind::Write {
                    Err(VeinError::internal("WRITE execution emitted result rows"))
                } else { batch.view().validate_schema(&self.arrow_schema) };
                if let Err(error) = validation.and_then(|()|
                    self.control.resources.record_delivery(batch.view().num_rows())) {
                    return Poll::Ready(self.failed(error));
                }
                Poll::Ready(Ok(Some(batch)))
            }
            Poll::Ready(Ok(QueryEvent::Complete { write_receipt })) => {
                if self.kind == QueryKind::Write && write_receipt.is_none() {
                    return Poll::Ready(self.failed(VeinError::internal("WRITE completed without a commit receipt")));
                }
                if self.kind == QueryKind::Rows && write_receipt.is_some() {
                    return Poll::Ready(self.failed(VeinError::internal("row query emitted a write receipt")));
                }
                self.control.resources.finish_query(write_receipt);
                self.release();
                Poll::Ready(self.terminal().unwrap_or(Ok(None)))
            }
        }
    }

    fn terminal(&mut self) -> Option<QueryResult<Option<ResultBatch>>> {
        match self.control.status() {
            QueryStatus::Prepared | QueryStatus::Running => None,
            QueryStatus::Finished | QueryStatus::Closed => {
                self.release();
                Some(Ok(None))
            }
            QueryStatus::Failed | QueryStatus::Cancelled => {
                let error = self.control.failure().unwrap_or_else(|| Arc::new(VeinError::cancelled()));
                self.release();
                Some(Err(error))
            }
        }
    }
    fn failed(&mut self, error: VeinError) -> QueryResult<Option<ResultBatch>> {
        let original = self.control.resources.fail_query(error);
        self.release();
        Err(original)
    }
}
