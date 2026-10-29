use super::*;
use crate::query::{QueryEvent, QueryExecution, QueryHandle, QueryKind, ResultBatch};
use std::task::{Context, Poll};
use futures::StreamExt;

struct LocalExecution {
    events: Pin<Box<dyn Stream<Item=Result<QueryEvent>> + Send>>,
}
impl QueryExecution for LocalExecution {
    fn poll_event(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<QueryEvent>> {
        match self.events.as_mut().poll_next(cx) {
            Poll::Ready(None) => Poll::Ready(Err(VeinError::internal("execution ended without a completion event"))),
            Poll::Ready(Some(event)) => Poll::Ready(event),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl LocalQuery {
    /// Starts one client-neutral cursor. The prepared execution is consumed once.
    ///
    /// The handle retains execution owners, so it can outlive its engine/session.
    /// Client adapters choose their own connection concurrency policy.
    pub fn start(self) -> Result<QueryHandle> {
        let resources = Arc::clone(self.scope.resources());
        let kind = if self.is_write { QueryKind::Write } else { QueryKind::Rows };
        let schema = if self.is_write {
            Arc::new(plan::VeinSchema::empty())
        } else {
            Arc::clone(&self.physical_plan(0)
                .ok_or_else(|| VeinError::internal("query has no output task"))?.root.output_schema)
        };
        let accounting = Arc::clone(&resources);
        // This adapter delegates to the existing execution algorithm. T11-T19
        // replace its producer behind QueryExecution, not the public cursor.
        let events = async_stream::stream! {
            if kind == QueryKind::Write {
                match self.execute_write().await {
                    Ok(receipt) => yield Ok(QueryEvent::Complete { write_receipt: Some(receipt) }),
                    Err(error) => yield Err(error),
                }
            } else {
                let mut stream = self.into_stream();
                while let Some(view) = stream.next().await {
                    let batch = view.and_then(|view| ResultBatch::account(view, accounting.memory_tracker()));
                    match batch {
                        Ok(batch) => yield Ok(QueryEvent::Batch(batch)),
                        Err(error) => { yield Err(error); return; }
                    }
                }
                yield Ok(QueryEvent::Complete { write_receipt: None });
            }
        };
        let execution: Pin<Box<dyn QueryExecution + Send>> = Box::pin(LocalExecution { events: Box::pin(events) });
        QueryHandle::from_pinned(resources, schema, kind, execution)
    }
}
