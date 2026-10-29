#![deny(missing_docs)]

//! Format-neutral query orchestration over injected catalogs and scan providers.

/// In-process query preparation and streaming execution.
pub mod local;
/// Client-neutral execution contracts and result ownership.
pub mod query;
