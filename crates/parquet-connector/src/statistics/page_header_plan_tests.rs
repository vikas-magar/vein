use std::collections::{BTreeSet, HashMap, HashSet};

use catalog::{StatisticsComparison, StatisticsPredicate};
use types::LogicalLiteral;

use super::{PageHeaderReadPlan, page_header_requests_with_audit};
use crate::parquet_statistics::PagePruning;

fn equality(column: &str) -> StatisticsPredicate {
    StatisticsPredicate::Comparison {
        column: column.to_string(),
        comparison: StatisticsComparison::Equal,
        literal: LogicalLiteral::Int64(1),
    }
}

fn complete_index(groups: &[(usize, &[&str])]) -> PagePruning {
    PagePruning {
        page_index_complete_columns: groups
            .iter()
            .map(|(row_group, columns)| {
                (
                    *row_group,
                    columns.iter().map(|name| (*name).to_string()).collect(),
                )
            })
            .collect::<HashMap<usize, HashSet<String>>>(),
        ..PagePruning::default()
    }
}

#[test]
fn complete_index_pairs_do_not_request_page_headers() {
    let predicates = [equality("shipmode"), equality("receiptdate")];
    let index = complete_index(&[(0, &["shipmode", "receiptdate"]), (1, &["shipmode", "receiptdate"])]);

    let plan = page_header_requests_with_audit(&predicates, &[0, 1], true, &index, false);

    assert!(plan.is_empty());
}

#[test]
fn fallback_requests_only_incomplete_row_group_column_pairs() {
    let predicates = [equality("shipmode"), equality("receiptdate")];
    let index = complete_index(&[(0, &["shipmode", "receiptdate"]), (1, &["receiptdate"])]);

    let plan = page_header_requests_with_audit(&predicates, &[0, 1], true, &index, false);

    assert_eq!(plan.pair_count(), 1);
    assert_eq!(
        plan.requests().get(&1),
        Some(&BTreeSet::from(["shipmode".to_string()]))
    );
    assert!(!plan.requests().contains_key(&0));
}

#[test]
fn absent_page_index_requests_each_predicate_for_each_surviving_group() {
    let predicates = [equality("shipmode"), equality("receiptdate")];

    let plan = page_header_requests_with_audit(&predicates, &[2, 4], false, &PagePruning::default(), false);

    assert_eq!(plan.pair_count(), 4);
    assert_eq!(plan.column_count(), 2);
    assert_eq!(plan.requests()[&2], BTreeSet::from(["receiptdate".into(), "shipmode".into()]));
    assert_eq!(plan.requests()[&4], BTreeSet::from(["receiptdate".into(), "shipmode".into()]));
}

#[test]
fn audit_requests_complete_pairs_and_plan_merge_deduplicates_dictionary_requests() {
    let predicates = [equality("shipmode"), equality("receiptdate")];
    let index = complete_index(&[(3, &["shipmode", "receiptdate"])]);
    let mut plan = page_header_requests_with_audit(&predicates, &[3], true, &index, true);
    let mut dictionary = PageHeaderReadPlan::default();
    dictionary.request(3, "shipmode");
    dictionary.request(5, "encoding_only");
    plan.merge(dictionary);

    assert_eq!(plan.pair_count(), 3);
    assert_eq!(plan.column_count(), 3);
    assert_eq!(plan.requests()[&3], BTreeSet::from(["receiptdate".into(), "shipmode".into()]));
    assert_eq!(plan.requests()[&5], BTreeSet::from(["encoding_only".into()]));
}

#[test]
fn empty_predicates_or_surviving_groups_produce_no_requests() {
    assert!(page_header_requests_with_audit(&[], &[0], false, &PagePruning::default(), false).is_empty());
    assert!(page_header_requests_with_audit(&[equality("shipmode")], &[], false, &PagePruning::default(), false).is_empty());
}
