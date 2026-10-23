//! Parquet policy retained by a format-neutral execution worker.

use std::sync::Arc;

use crate::scan::parquet::predicate::AdaptiveFilterOrder;

#[derive(Default)]
pub(super) struct AdaptiveWorkerState {
    pub(super) order: Option<Arc<AdaptiveFilterOrder>>,
}

#[cfg(test)]
mod tests;
