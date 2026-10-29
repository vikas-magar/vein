//! Client-neutral query execution and single-owner result cursors.
//!
//! A handle owns one cursor; cloneable controls may observe or cancel it from
//! other threads. Independent queries have isolated scopes. Connection/session
//! adapters impose their own concurrency limits; the engine has no global
//! single-query slot. Prepared executions are single-use, not reusable parameter
//! statements. Encoding, ODBC/JDBC conversion and transport belong to adapters.
mod contract;
mod control;
mod handle;
mod stream;

pub use contract::{QueryEvent, QueryExecution, QueryKind, QueryResult};
pub use control::QueryControl;
pub use handle::QueryHandle;
pub use stream::QueryStream;
pub use exec::runtime::{QueryStatus, QueryProgress};
pub use exec::pipeline::resumable::result::ResultBatch;

#[cfg(test)]
mod tests;
