//! Parquet footer, page, Bloom-filter, and dictionary statistics.
//!
//! Public helpers stay at this facade while the implementation is grouped by
//! metadata extraction, pruning decisions, and format-specific indexes.

mod metadata;
pub use metadata::{row_group_column_statistics, table_statistics};

mod units;
pub use units::file_units;

mod pruning_diagnostics;
pub use pruning_diagnostics::{
    PredicatePruningDiagnostic, PredicatePruningDiagnostics, RowGroupPruning,
};

mod page;
pub use page::{
    PageHeaderRowStatistics, PageHeaderStatistics, PagePruning,
    column_statistics_from_page_header, prune_data_pages, prune_data_pages_with_headers,
    prune_row_groups, prune_row_groups_in_range,
};

mod page_header_plan;
pub use page_header_plan::{PageHeaderReadPlan, page_header_requests_with_audit};

#[cfg(test)]
use page::page_statistic_literal;
mod bloom;
mod dictionary;
mod dictionary_page;

pub use dictionary_page::{
    dictionary_page_header_requests, has_dictionary_encoded_page, prune_dictionary_encoded_pages,
    DictionaryPagePruning,
};

pub use bloom::{
    bloom_filter_columns, equality_predicate_columns, prune_row_groups_with_bloom,
    BloomFilterColumn, BloomPruning, ParquetBloomFilter,
};
pub use dictionary::{
    dictionary_page_predicate_matches, dictionary_page_range, dictionary_predicates,
    is_fully_dictionary_encoded, prune_row_groups_with_dictionary, DictionaryPredicate,
    DictionaryPruning,
};

#[cfg(test)]
use arrow::datatypes::Schema;
#[cfg(test)]
use catalog::{DataUnitStatistics, StatisticsUnitKind};
#[cfg(test)]
use metadata::exact_scalar_at;
#[cfg(test)]
use metadata::complete_sum;

#[cfg(test)]
mod tests;
