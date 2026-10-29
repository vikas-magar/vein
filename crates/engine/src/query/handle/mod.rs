use super::{QueryControl, QueryExecution, QueryKind, QueryResult, QueryStream, ResultBatch};
use common::Result;
use exec::runtime::QueryExecutionResources;
use plan::VeinSchemaRef;
use std::pin::Pin;
use std::sync::Arc;

mod fetch;

/// One owning result cursor over a typed or type-erased execution backend.
///
/// Fetch requires mutable ownership. Controls are shareable; fetched batches
/// retain leases independently of handle close/drop. Dropping a fetch future
/// leaves this cursor resumable. Dropping the handle closes remaining work.
pub struct QueryHandle<E: QueryExecution + ?Sized = dyn QueryExecution + Send> {
    pub(super) execution: Option<Pin<Box<E>>>,
    pub(super) control: QueryControl,
    pub(super) schema: VeinSchemaRef,
    pub(super) arrow_schema: arrow::datatypes::SchemaRef,
    pub(super) kind: QueryKind,
}

impl<E: QueryExecution + 'static> QueryHandle<E> {
    /// Attaches a typed execution to a fresh scope, claiming its sole cursor.
    pub fn from_execution(resources: Arc<QueryExecutionResources>, schema: VeinSchemaRef,
                          kind: QueryKind, execution: E) -> Result<Self> {
        Self::from_pinned(resources, schema, kind, Box::pin(execution))
    }
}

impl<E: QueryExecution + ?Sized> QueryHandle<E> {
    /// Attaches a pinned backend, including registered/type-erased implementations.
    pub fn from_pinned(resources: Arc<QueryExecutionResources>, schema: VeinSchemaRef,
                       kind: QueryKind, execution: Pin<Box<E>>) -> Result<Self> {
        resources.claim_result_cursor()?;
        let arrow_schema = Arc::new(schema.to_arrow());
        Ok(Self {
            execution: Some(execution),
            control: QueryControl { resources },
            schema,
            arrow_schema,
            kind,
        })
    }
    /// Stable declared column metadata, available before the first fetch and after close.
    pub fn schema(&self) -> &VeinSchemaRef { &self.schema }
    /// The query's row/write result semantics.
    pub fn kind(&self) -> QueryKind { self.kind }
    /// Observes or cancels execution without sharing cursor ownership.
    pub fn control(&self) -> QueryControl { self.control.clone() }
    /// Waits for one leased batch; terminal error references remain stable.
    pub async fn fetch(&mut self) -> QueryResult<Option<ResultBatch>> {
        let _wait = CursorWait(Arc::clone(&self.control.resources));
        futures::future::poll_fn(|cx| self.poll_fetch(cx)).await
    }
    /// Closes active execution. Existing terminal outcomes and fetched leases survive.
    pub fn close(&mut self) {
        self.control.resources.close_query();
        self.release();
    }
    pub(super) fn release(&mut self) {
        self.control.resources.clear_cursor_waker();
        drop(self.execution.take());
    }
    /// Standard stream adapter: reports a terminal error once, then ends.
    pub fn into_stream(self) -> QueryStream<E> { QueryStream::new(self) }
}

impl<E: QueryExecution + ?Sized> Drop for QueryHandle<E> {
    fn drop(&mut self) { self.close(); }
}

struct CursorWait(Arc<QueryExecutionResources>);
impl Drop for CursorWait {
    fn drop(&mut self) { self.0.clear_cursor_waker(); }
}
