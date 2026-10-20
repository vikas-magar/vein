//! Parquet's staged, late-materialized row-filter adapter for Vein predicates.

#[path = "predicate/adaptive_order.rs"]
mod adaptive_order;
#[path = "predicate/row_filter.rs"]
mod row_filter;

pub(super) use adaptive_order::AdaptiveFilterOrder;
pub(super) use row_filter::{ScanPredicateContext, build_scan_predicates};
